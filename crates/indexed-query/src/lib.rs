//! Backend-neutral indexed-view query contracts and SQL lowering.
//!
//! Storage adapters own execution and connections; these contracts describe
//! the shared predicate, ordering, validation and SQL-lowering vocabulary.

use cqrs_es::persist::{PersistenceError, ViewRepository};
use cqrs_es::{Aggregate, View};
use std::error::Error;
use std::fmt;

/// A predicate scan the load-by-id [`ViewRepository`] cannot express.
///
/// Returns matching view identities without requiring callers to write SQL.
pub trait IndexedView<ViewState, AggregateType>: ViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    /// Returns view identities matching an OR of AND-groups, ordered and capped
    /// at the maximum row count `limit`.
    fn find(
        &self,
        predicate: &Predicate,
        order: Option<&Order>,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<String>, PersistenceError>> + Send;
}

/// A predicate in disjunctive normal form; an empty disjunction matches nothing.
pub struct Predicate {
    /// OR of conjunctions; each inner vector is an AND of [`Term`]s.
    pub any_of: Vec<Vec<Term>>,
}

/// One comparison against a materialized-view column.
pub struct Term {
    /// A trusted, application-controlled column identifier, never user input.
    pub column: String,
    /// The comparison operator applied to the column.
    pub cmp: Cmp,
    /// The bound scalar; absent for [`Cmp::IsNull`] and [`Cmp::IsNotNull`].
    pub value: Option<Value>,
}

/// Comparison operators for a [`Term`].
pub enum Cmp {
    /// `=`
    Eq,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `IS NULL`, without a bound value.
    IsNull,
    /// `IS NOT NULL`, without a bound value.
    IsNotNull,
}

/// A scalar encoded by the selected storage driver.
pub enum Value {
    /// An integer column value, preserving the full signed 64-bit range.
    Int(i64),
    /// A text column value, bound as data rather than interpolated into SQL.
    Text(String),
}

/// Result ordering for [`IndexedView::find`].
pub struct Order {
    /// A trusted, application-controlled column identifier.
    pub column: String,
    /// Ascending when true, descending otherwise.
    pub ascending: bool,
}

/// Invalid indexed-query inputs rejected before executing SQL.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionQueryError {
    /// The selected storage driver could not encode a query argument.
    #[error("indexed projection argument encoding failed")]
    Encoding(#[source] Box<dyn Error + Send + Sync>),
    /// The driver could not format its own placeholder.
    #[error("indexed projection placeholder formatting failed")]
    Placeholder(#[from] fmt::Error),
    /// The caller supplied a negative maximum row count.
    #[error("indexed projection query limit cannot be negative: {0}")]
    NegativeLimit(i64),
    /// Every disjunction arm must contain at least one comparison.
    #[error("indexed projection query contains an empty conjunction")]
    EmptyConjunction,
    /// Binary comparisons require a bound value.
    #[error("indexed projection comparison requires a bound value")]
    MissingValue,
    /// Null comparisons do not accept a bound value.
    #[error("indexed projection null comparison cannot contain a bound value")]
    UnexpectedValue,
    /// No further placeholder fits the selected driver's parameter budget.
    #[error("indexed projection query exceeds the driver parameter limit")]
    ParameterLimit,
}

/// Identifier provenance accepted by an existing repository's SQL contract.
#[derive(Clone, Copy)]
pub enum IdentifierMode {
    /// A complete literal name, quoted and escaped as one identifier.
    Literal,
    /// Existing application-controlled SQL identifier syntax, including qualified
    /// names and explicit identifier quotes. Never accepts untrusted input.
    TrustedSql,
}

/// Lowers an indexed query while the repository encodes each bound scalar.
///
/// `bind` must encode the scalar using its storage driver and return only that
/// driver's placeholder, never interpolated application data. The final bound
/// scalar is the maximum row count. An empty disjunction produces no query.
///
/// # Errors
///
/// Returns typed predicate/limit validation or native argument-encoding errors.
pub fn compile_query(
    table: &str,
    identifiers: IdentifierMode,
    predicate: &Predicate,
    order: Option<&Order>,
    limit: i64,
    mut bind: impl FnMut(&Value) -> Result<String, ProjectionQueryError>,
) -> Result<Option<String>, ProjectionQueryError> {
    if limit < 0 {
        return Err(ProjectionQueryError::NegativeLimit(limit));
    }
    if predicate.any_of.is_empty() {
        return Ok(None);
    }

    let groups = predicate
        .any_of
        .iter()
        .map(|conjunction| {
            if conjunction.is_empty() {
                return Err(ProjectionQueryError::EmptyConjunction);
            }
            let terms = conjunction
                .iter()
                .map(|term| {
                    use Cmp::{Eq, Ge, Gt, IsNotNull, IsNull, Le, Lt};

                    let column = identifiers.render(&term.column);
                    let operator = term.cmp.sql();
                    match (&term.cmp, &term.value) {
                        (IsNull | IsNotNull, None) => Ok(format!("{column} {operator}")),
                        (Eq | Lt | Le | Gt | Ge, Some(value)) => {
                            let placeholder = bind(value)?;
                            Ok(format!("{column} {operator} {placeholder}"))
                        }
                        (Eq | Lt | Le | Gt | Ge, None) => Err(ProjectionQueryError::MissingValue),
                        (IsNull | IsNotNull, Some(_)) => Err(ProjectionQueryError::UnexpectedValue),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("({})", terms.join(" AND ")))
        })
        .collect::<Result<Vec<_>, ProjectionQueryError>>()?;
    let order_clause = order.map_or_else(String::new, |order| {
        let column = identifiers.render(&order.column);
        let direction = if order.ascending {
            "ASC NULLS FIRST"
        } else {
            "DESC NULLS LAST"
        };
        format!(" ORDER BY {column} {direction}")
    });
    let limit_placeholder = bind(&Value::Int(limit))?;
    let table = identifiers.render(table);
    Ok(Some(format!(
        "SELECT view_id FROM {table} WHERE {}{order_clause} LIMIT {limit_placeholder}",
        groups.join(" OR "),
    )))
}

/// Quotes one complete SQL identifier; dots remain literal name characters.
#[must_use]
pub fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

impl IdentifierMode {
    fn render(self, name: &str) -> String {
        use IdentifierMode::{Literal, TrustedSql};

        match self {
            Literal => quote_identifier(name),
            TrustedSql => name.to_owned(),
        }
    }
}

impl Cmp {
    /// Returns this operator's SQL token without operands or a placeholder.
    pub const fn sql(&self) -> &'static str {
        use Cmp::{Eq, Ge, Gt, IsNotNull, IsNull, Le, Lt};

        match self {
            Eq => "=",
            Lt => "<",
            Le => "<=",
            Gt => ">",
            Ge => ">=",
            IsNull => "IS NULL",
            IsNotNull => "IS NOT NULL",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_disjunction_needs_no_driver_encoding() {
        let query = compile_query(
            "items_projection",
            IdentifierMode::Literal,
            &Predicate { any_of: vec![] },
            None,
            1,
            |_| panic!("an empty query must not encode an argument"),
        )
        .unwrap();
        assert_eq!(query, None);
    }

    #[test]
    fn empty_conjunction_fails_without_encoding() {
        let error = compile_query(
            "items_projection",
            IdentifierMode::Literal,
            &Predicate {
                any_of: vec![vec![]],
            },
            None,
            1,
            |_| panic!("invalid conjunction must not encode an argument"),
        )
        .unwrap_err();
        assert!(matches!(error, ProjectionQueryError::EmptyConjunction));
    }

    #[test]
    fn missing_binary_value_fails_without_encoding() {
        let predicate = Predicate {
            any_of: vec![vec![Term {
                column: "amount".to_string(),
                cmp: Cmp::Eq,
                value: None,
            }]],
        };
        let error = compile_query(
            "items_projection",
            IdentifierMode::Literal,
            &predicate,
            None,
            1,
            |_| panic!("a missing value must not reach the encoder"),
        )
        .unwrap_err();
        assert!(matches!(error, ProjectionQueryError::MissingValue));
    }

    #[test]
    fn null_comparison_rejects_a_bound_value_without_encoding() {
        let predicate = Predicate {
            any_of: vec![vec![Term {
                column: "amount".to_string(),
                cmp: Cmp::IsNull,
                value: Some(Value::Int(3)),
            }]],
        };
        let error = compile_query(
            "items_projection",
            IdentifierMode::Literal,
            &predicate,
            None,
            1,
            |_| panic!("a null comparison must not encode a value"),
        )
        .unwrap_err();
        assert!(matches!(error, ProjectionQueryError::UnexpectedValue));
    }

    #[test]
    fn driver_failure_stops_lowering_before_the_limit() {
        let predicate = Predicate {
            any_of: vec![vec![Term {
                column: "amount".to_string(),
                cmp: Cmp::Ge,
                value: Some(Value::Int(3)),
            }]],
        };
        let mut calls = 0;
        let error = compile_query(
            "items_projection",
            IdentifierMode::Literal,
            &predicate,
            None,
            1,
            |_| {
                calls += 1;
                Err(ProjectionQueryError::ParameterLimit)
            },
        )
        .unwrap_err();
        assert!(matches!(error, ProjectionQueryError::ParameterLimit));
        assert_eq!(calls, 1);
    }
}
