use ciborium::value::Value;

use crate::model::{
    AttemptCount, ClaimCount, DeadReason, Instant, JobPayload, JobRecord, JobStatus, LeaseToken,
    StoredJob,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    Decode(String),
    Encode(String),
    InvalidShape(&'static str),
    IntegerOutOfRange(&'static str),
    NonCanonical,
}

pub fn encode_job_record(record: &JobRecord) -> Result<Vec<u8>, CodecError> {
    encode(&Value::Array(vec![
        integer(1),
        Value::Bytes(record.payload().as_bytes().to_vec()),
        job_status_value(record.status()),
        integer(record.token().get()),
        integer(record.attempts().get()),
        integer(record.claims().get()),
    ]))
}

pub fn decode_job_record(bytes: &[u8]) -> Result<JobRecord, CodecError> {
    let value = decode(bytes)?;
    let record = parse_job_record(&value)?;
    require_canonical(bytes, &encode_job_record(&record)?)?;
    Ok(record)
}

pub fn encode_stored_job(job: &StoredJob) -> Result<Vec<u8>, CodecError> {
    encode(&Value::Array(vec![
        integer(1),
        Value::Text(job.job_type().to_owned()),
        Value::Bytes(job.payload().as_bytes().to_vec()),
    ]))
}

pub fn decode_stored_job(bytes: &[u8]) -> Result<StoredJob, CodecError> {
    let value = decode(bytes)?;
    let job = parse_stored_job(&value)?;
    require_canonical(bytes, &encode_stored_job(&job)?)?;
    Ok(job)
}

fn encode(value: &Value) -> Result<Vec<u8>, CodecError> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .map_err(|error| CodecError::Encode(error.to_string()))?;
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<Value, CodecError> {
    ciborium::de::from_reader(bytes).map_err(|error| CodecError::Decode(error.to_string()))
}

fn parse_job_record(value: &Value) -> Result<JobRecord, CodecError> {
    let Value::Array(fields) = value else {
        return Err(CodecError::InvalidShape("job-record"));
    };
    let [version, payload, status, token, attempts, claims] = fields.as_slice() else {
        return Err(CodecError::InvalidShape("job-record"));
    };
    require_version(version, "job-record format version")?;
    let Value::Bytes(payload) = payload else {
        return Err(CodecError::InvalidShape("job-record payload"));
    };

    Ok(JobRecord::new(
        JobPayload::new(payload.clone()),
        parse_job_status(status)?,
        LeaseToken::new(unsigned(token, "job-record token")?),
        AttemptCount::new(unsigned(attempts, "job-record attempts")?),
        ClaimCount::new(unsigned(claims, "job-record claims")?),
    ))
}

fn parse_stored_job(value: &Value) -> Result<StoredJob, CodecError> {
    let Value::Array(fields) = value else {
        return Err(CodecError::InvalidShape("stored-job"));
    };
    let [version, job_type, payload] = fields.as_slice() else {
        return Err(CodecError::InvalidShape("stored-job"));
    };
    require_version(version, "stored-job format version")?;
    let Value::Text(job_type) = job_type else {
        return Err(CodecError::InvalidShape("stored-job job type"));
    };
    let Value::Bytes(payload) = payload else {
        return Err(CodecError::InvalidShape("stored-job payload"));
    };
    Ok(StoredJob::new(
        job_type.clone(),
        JobPayload::new(payload.clone()),
    ))
}

fn parse_job_status(value: &Value) -> Result<JobStatus, CodecError> {
    let Value::Array(fields) = value else {
        return Err(CodecError::InvalidShape("job-status"));
    };
    let Some(tag) = fields.first() else {
        return Err(CodecError::InvalidShape("job-status"));
    };
    match (unsigned(tag, "job-status tag")?, fields.as_slice()) {
        (0, [_]) => Ok(JobStatus::Ready),
        (1, [_, run_at]) => Ok(JobStatus::Scheduled(Instant::from_millis(unsigned(
            run_at,
            "job-status run_at",
        )?))),
        (2, [_, expires]) => Ok(JobStatus::Leased(Instant::from_millis(unsigned(
            expires,
            "job-status expires",
        )?))),
        (3, [_]) => Ok(JobStatus::Completed),
        (4, [_, reason]) => Ok(JobStatus::DeadLettered(parse_dead_reason(reason)?)),
        _ => Err(CodecError::InvalidShape("job-status")),
    }
}

fn parse_dead_reason(value: &Value) -> Result<DeadReason, CodecError> {
    match unsigned(value, "dead-reason")? {
        0 => Ok(DeadReason::RetriesExhausted),
        1 => Ok(DeadReason::Rejected),
        2 => Ok(DeadReason::Undecodable),
        3 => Ok(DeadReason::Abandoned),
        _ => Err(CodecError::InvalidShape("dead-reason")),
    }
}

fn job_status_value(status: JobStatus) -> Value {
    let fields = match status {
        JobStatus::Ready => vec![integer(0)],
        JobStatus::Scheduled(run_at) => vec![integer(1), integer(run_at.as_millis())],
        JobStatus::Leased(expires) => vec![integer(2), integer(expires.as_millis())],
        JobStatus::Completed => vec![integer(3)],
        JobStatus::DeadLettered(reason) => {
            vec![integer(4), integer(dead_reason_tag(reason))]
        }
    };
    Value::Array(fields)
}

const fn dead_reason_tag(reason: DeadReason) -> u64 {
    match reason {
        DeadReason::RetriesExhausted => 0,
        DeadReason::Rejected => 1,
        DeadReason::Undecodable => 2,
        DeadReason::Abandoned => 3,
    }
}

fn require_version(value: &Value, name: &'static str) -> Result<(), CodecError> {
    if unsigned(value, name)? == 1 {
        Ok(())
    } else {
        Err(CodecError::InvalidShape(name))
    }
}

fn unsigned(value: &Value, name: &'static str) -> Result<u64, CodecError> {
    let Value::Integer(integer) = value else {
        return Err(CodecError::InvalidShape(name));
    };
    u64::try_from(*integer).map_err(|_| CodecError::IntegerOutOfRange(name))
}

fn integer(value: u64) -> Value {
    Value::Integer(value.into())
}

fn require_canonical(input: &[u8], canonical: &[u8]) -> Result<(), CodecError> {
    if input == canonical {
        Ok(())
    } else {
        Err(CodecError::NonCanonical)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CodecError, decode_job_record, decode_stored_job, encode_job_record, encode_stored_job,
    };
    use crate::model::{
        AttemptCount, ClaimCount, DeadReason, Instant, JobPayload, JobRecord, JobStatus,
        LeaseToken, StoredJob,
    };

    #[test]
    fn stored_job_matches_the_versioned_cddl_bytes() {
        let job = StoredJob::new("email", JobPayload::new([1, 2]));
        let expected = vec![
            0x83, 0x01, 0x65, b'e', b'm', b'a', b'i', b'l', 0x42, 0x01, 0x02,
        ];

        assert_eq!(encode_stored_job(&job), Ok(expected.clone()));
        assert_eq!(decode_stored_job(&expected), Ok(job));
    }

    #[test]
    fn job_record_matches_the_versioned_cddl_bytes() {
        let record = JobRecord::new(
            JobPayload::new([1, 2]),
            JobStatus::Scheduled(Instant::from_millis(1_000)),
            LeaseToken::new(3),
            AttemptCount::new(4),
            ClaimCount::new(5),
        );
        let expected = vec![
            0x86, 0x01, 0x42, 0x01, 0x02, 0x82, 0x01, 0x19, 0x03, 0xe8, 0x03, 0x04, 0x05,
        ];

        assert_eq!(encode_job_record(&record), Ok(expected.clone()));
        assert_eq!(decode_job_record(&expected), Ok(record));
    }

    #[test]
    fn every_job_status_round_trips() {
        let statuses = [
            JobStatus::Ready,
            JobStatus::Scheduled(Instant::from_millis(10)),
            JobStatus::Leased(Instant::from_millis(20)),
            JobStatus::Completed,
            JobStatus::DeadLettered(DeadReason::RetriesExhausted),
            JobStatus::DeadLettered(DeadReason::Rejected),
            JobStatus::DeadLettered(DeadReason::Undecodable),
            JobStatus::DeadLettered(DeadReason::Abandoned),
        ];

        for status in statuses {
            let record = JobRecord::new(
                JobPayload::new([9]),
                status,
                LeaseToken::new(1),
                AttemptCount::new(2),
                ClaimCount::new(3),
            );
            let encoded = encode_job_record(&record);
            let Ok(bytes) = encoded else {
                panic!("valid job record must encode")
            };
            assert_eq!(decode_job_record(&bytes), Ok(record));
        }
    }

    #[test]
    fn decoder_rejects_noncanonical_and_indefinite_encodings() {
        let non_shortest_version = [0x86, 0x18, 0x01, 0x41, 0x09, 0x81, 0x00, 0x00, 0x00, 0x00];
        let indefinite_record = [0x9f, 0x01, 0x41, 0x09, 0x81, 0x00, 0x00, 0x00, 0x00, 0xff];

        assert_eq!(
            decode_job_record(&non_shortest_version),
            Err(CodecError::NonCanonical)
        );
        assert_eq!(
            decode_job_record(&indefinite_record),
            Err(CodecError::NonCanonical)
        );
    }

    #[test]
    fn decoder_rejects_wrong_versions_tags_and_floats() {
        let wrong_version = [0x83, 0x02, 0x61, b'x', 0x40];
        let tagged_payload = [0x83, 0x01, 0x61, b'x', 0xc0, 0x40];
        let float_token = [0x86, 0x01, 0x40, 0x81, 0x00, 0xf9, 0x00, 0x00, 0x00, 0x00];

        assert_eq!(
            decode_stored_job(&wrong_version),
            Err(CodecError::InvalidShape("stored-job format version"))
        );
        assert_eq!(
            decode_stored_job(&tagged_payload),
            Err(CodecError::InvalidShape("stored-job payload"))
        );
        assert_eq!(
            decode_job_record(&float_token),
            Err(CodecError::InvalidShape("job-record token"))
        );
    }
}
