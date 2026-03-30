pub mod dtype;
pub mod error;
pub mod schema;
pub mod series;
pub mod dataframe;
pub mod expr;
pub mod lazy;
pub mod compute;
pub mod io;
pub mod dataset;

/// Prelude: import the most commonly used types.
pub mod prelude {
    pub use crate::dataframe::DataFrame;
    pub use crate::dtype::DataType;
    pub use crate::error::{BlazeError, Result};
    pub use crate::expr::{col, lit, Expr, SortOptions, IntoLit};
    pub use crate::lazy::{LazyFrame, JoinType};
    pub use crate::schema::{Field, Schema};
    pub use crate::series::Series;
}
