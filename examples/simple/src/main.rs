//! Single-entity `event-sorcery` example: a support-ticket aggregate with a
//! materialized view (filtered queries via a SQLite generated column) and an
//! entity-dispatched durable job (ADR-0009) -- closing a ticket kicks off a
//! `NotifyClosed` job; the ticket settles to `Closed` only when the worker's
//! verdict is delivered back.
//!
//! Run with: `cargo run --manifest-path examples/simple/Cargo.toml`
//!
//! See `README.md` next to this file for design notes; see
//! `support_ticket.rs` for the entity definition, the jobs, view SQL, and tests.

use std::error::Error;
use std::time::{Duration, Instant};

use sqlx::SqlitePool;

use event_sorcery::{
    Clock, JobInput, JobRuntime, JobWorkerConfig, StoreBuilder, build_supervised_worker,
};

mod support_ticket;

use support_ticket::{
    Notifier, NotifyClosed, STATUS, Status, SupportTicket, SupportTicketCommand, SweepStaleTickets,
    TicketId,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
        .build()
        .await?;

    // Callers stamp the timestamp onto the command (handlers take no services).
    let now = chrono::Utc::now();
    let login = TicketId(1);
    let feature = TicketId(2);
    let billing = TicketId(3);

    for (id, subject) in [
        (login, "login broken"),
        (feature, "feature request"),
        (billing, "billing question"),
    ] {
        store
            .send(
                &id,
                SupportTicketCommand::Open {
                    ticket: id,
                    subject: subject.to_string(),
                    at: now,
                },
            )
            .await?;
    }

    store
        .send(&feature, SupportTicketCommand::AwaitCustomer { at: now })
        .await?;
    // Closing KICKS OFF the NotifyClosed job: the framework commits the
    // `Dispatched` intent and the enqueue atomically, and the ticket shows
    // `Closing` until the worker's verdict lands.
    store
        .send(&billing, SupportTicketCommand::Close { at: now })
        .await?;

    let one = projection.load(&login).await?.ok_or("login missing")?;
    println!(
        "projection.load({login})       = {{ subject: {:?}, status: {:?} }}",
        one.subject, one.status
    );

    let closing = projection.filter(STATUS, &Status::Closing).await?;
    println!(
        "filter(STATUS, Closing)      = {} (ids: {:?}) -- notify job in flight",
        closing.len(),
        closing.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );

    // The worker input bundles the notifier with the origin store the
    // framework delivers verdicts through. A standalone job (ADR-0007) rides
    // the same runtime with a plain input.
    let runtime = JobRuntime::build(pool.clone()).await?;
    runtime.enqueue(SweepStaleTickets).await?;

    let config = JobWorkerConfig {
        poll_interval: Duration::from_millis(25),
        ..JobWorkerConfig::default()
    };
    let monitor = build_supervised_worker!(runtime, config, Clock::system(), {
        NotifyClosed => JobInput::<NotifyClosed>::new(Notifier, store.clone()),
        SweepStaleTickets => (),
    });
    let worker = tokio::spawn(monitor.run());

    println!("running the job worker until the close settles...");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let ticket = store.load(&billing).await?.ok_or("billing missing")?;
        if ticket.status == Status::Closed {
            break;
        }
        if Instant::now() > deadline {
            return Err("the close did not settle in time".into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    worker.abort();

    let closed = projection.filter(STATUS, &Status::Closed).await?;
    println!(
        "filter(STATUS, Closed)       = {} (ids: {:?}) -- verdict delivered",
        closed.len(),
        closed.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );

    Ok(())
}
