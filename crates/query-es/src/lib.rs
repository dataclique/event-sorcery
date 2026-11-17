//! Backend-neutral indexed-view query contracts.
//!
//! Storage adapters own execution and connections; these contracts describe
//! the shared predicate, ordering and validation vocabulary.

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
