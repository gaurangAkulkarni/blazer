use std::fmt;

/// A scalar literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum LitValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Null,
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

/// Aggregation functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    Mean,
    Min,
    Max,
    Count,
    NUnique,
    First,
    Last,
}

/// String operations.
#[derive(Debug, Clone, PartialEq)]
pub enum StringOp {
    ToUppercase,
    ToLowercase,
    Contains(String),
    StartsWith(String),
    EndsWith(String),
    Replace(String, String),
    Len,
}

/// Date/time component to extract from a temporal column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Weekday,
}

/// Sort options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SortOptions {
    pub descending: bool,
    pub nulls_first: bool,
}

impl SortOptions {
    pub fn ascending() -> Self {
        SortOptions {
            descending: false,
            nulls_first: false,
        }
    }

    pub fn descending() -> Self {
        SortOptions {
            descending: true,
            nulls_first: false,
        }
    }
}

impl Default for SortOptions {
    fn default() -> Self {
        Self::ascending()
    }
}

/// The Expression AST node.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Reference a column by name.
    Column(String),
    /// A literal value.
    Literal(LitValue),
    /// Binary operation: left op right.
    BinaryExpr {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// Aggregation (e.g., col("x").sum()).
    Agg {
        input: Box<Expr>,
        func: AggFunc,
    },
    /// Alias (e.g., col("x").sum().alias("total")).
    Alias {
        expr: Box<Expr>,
        name: String,
    },
    /// Sort expression.
    Sort {
        expr: Box<Expr>,
        options: SortOptions,
    },
    /// String operation.
    StringExpr {
        input: Box<Expr>,
        op: StringOp,
    },
    /// Rolling window operation.
    Rolling {
        input: Box<Expr>,
        func: AggFunc,
        window_size: usize,
    },
    /// Window function (over partition).
    Window {
        input: Box<Expr>,
        partition_by: Vec<Expr>,
    },
    /// Wildcard (select all columns).
    Wildcard,
    /// Not (logical negation).
    Not(Box<Expr>),
    /// IsNull check.
    IsNull(Box<Expr>),
    /// IsNotNull check.
    IsNotNull(Box<Expr>),
    /// Cast to a type.
    Cast {
        expr: Box<Expr>,
        dtype: crate::dtype::DataType,
    },
    /// Date/time part extraction (year, month, day, hour, …).
    DateExpr {
        input: Box<Expr>,
        part: DatePart,
    },
}

// ---- Builder methods (fluent API) ----

impl Expr {
    // Arithmetic
    pub fn add(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Add,
            right: Box::new(other),
        }
    }

    pub fn sub(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Sub,
            right: Box::new(other),
        }
    }

    pub fn mul(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Mul,
            right: Box::new(other),
        }
    }

    pub fn div(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Div,
            right: Box::new(other),
        }
    }

    // Comparison
    pub fn eq(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }

    pub fn neq(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::NotEq,
            right: Box::new(other),
        }
    }

    pub fn lt(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Lt,
            right: Box::new(other),
        }
    }

    pub fn lt_eq(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::LtEq,
            right: Box::new(other),
        }
    }

    pub fn gt(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Gt,
            right: Box::new(other),
        }
    }

    pub fn gt_eq(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::GtEq,
            right: Box::new(other),
        }
    }

    // Logical
    pub fn and(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }

    pub fn or(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }

    pub fn not(self) -> Expr {
        Expr::Not(Box::new(self))
    }

    // Aggregation
    pub fn sum(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::Sum,
        }
    }

    pub fn mean(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::Mean,
        }
    }

    pub fn min(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::Min,
        }
    }

    pub fn max(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::Max,
        }
    }

    pub fn count(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::Count,
        }
    }

    pub fn n_unique(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::NUnique,
        }
    }

    pub fn first(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::First,
        }
    }

    pub fn last(self) -> Expr {
        Expr::Agg {
            input: Box::new(self),
            func: AggFunc::Last,
        }
    }

    // Alias
    pub fn alias(self, name: &str) -> Expr {
        Expr::Alias {
            expr: Box::new(self),
            name: name.to_string(),
        }
    }

    // Sort
    pub fn sort(self, options: SortOptions) -> Expr {
        Expr::Sort {
            expr: Box::new(self),
            options,
        }
    }

    // String ops
    pub fn str(self) -> StringExprBuilder {
        StringExprBuilder { expr: self }
    }

    // Rolling
    pub fn rolling_mean(self, window_size: usize) -> Expr {
        Expr::Rolling {
            input: Box::new(self),
            func: AggFunc::Mean,
            window_size,
        }
    }

    pub fn rolling_sum(self, window_size: usize) -> Expr {
        Expr::Rolling {
            input: Box::new(self),
            func: AggFunc::Sum,
            window_size,
        }
    }

    // Window (over)
    pub fn over(self, partition_by: Vec<Expr>) -> Expr {
        Expr::Window {
            input: Box::new(self),
            partition_by,
        }
    }

    // Null checks
    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(Box::new(self))
    }

    pub fn cast(self, dtype: crate::dtype::DataType) -> Expr {
        Expr::Cast {
            expr: Box::new(self),
            dtype,
        }
    }

    // ---- Date/time extraction ----

    pub fn dt_year(self) -> Expr {
        Expr::DateExpr { input: Box::new(self), part: DatePart::Year }
    }
    pub fn dt_month(self) -> Expr {
        Expr::DateExpr { input: Box::new(self), part: DatePart::Month }
    }
    pub fn dt_day(self) -> Expr {
        Expr::DateExpr { input: Box::new(self), part: DatePart::Day }
    }
    pub fn dt_hour(self) -> Expr {
        Expr::DateExpr { input: Box::new(self), part: DatePart::Hour }
    }
    pub fn dt_minute(self) -> Expr {
        Expr::DateExpr { input: Box::new(self), part: DatePart::Minute }
    }
    pub fn dt_second(self) -> Expr {
        Expr::DateExpr { input: Box::new(self), part: DatePart::Second }
    }
    pub fn dt_weekday(self) -> Expr {
        Expr::DateExpr { input: Box::new(self), part: DatePart::Weekday }
    }

    /// Get the output name of this expression.
    pub fn output_name(&self) -> String {
        match self {
            Expr::Column(name) => name.clone(),
            Expr::Alias { name, .. } => name.clone(),
            Expr::Agg { input, func } => {
                format!("{}_{:?}", input.output_name(), func).to_lowercase()
            }
            Expr::BinaryExpr { left, .. } => left.output_name(),
            Expr::Sort { expr, .. } => expr.output_name(),
            Expr::StringExpr { input, .. } => input.output_name(),
            Expr::Rolling { input, func, window_size } => {
                format!("{}_{:?}_{}", input.output_name(), func, window_size).to_lowercase()
            }
            Expr::Window { input, .. } => input.output_name(),
            Expr::Not(e) => format!("not_{}", e.output_name()),
            Expr::IsNull(e) => format!("{}_is_null", e.output_name()),
            Expr::IsNotNull(e) => format!("{}_is_not_null", e.output_name()),
            Expr::Cast { expr, .. } => expr.output_name(),
            Expr::DateExpr { input, part } => {
                format!("{}_{:?}", input.output_name(), part).to_lowercase()
            }
            Expr::Literal(_) => "literal".to_string(),
            Expr::Wildcard => "*".to_string(),
        }
    }
}

/// Builder for string operations on expressions.
pub struct StringExprBuilder {
    expr: Expr,
}

impl StringExprBuilder {
    pub fn to_uppercase(self) -> Expr {
        Expr::StringExpr {
            input: Box::new(self.expr),
            op: StringOp::ToUppercase,
        }
    }

    pub fn to_lowercase(self) -> Expr {
        Expr::StringExpr {
            input: Box::new(self.expr),
            op: StringOp::ToLowercase,
        }
    }

    pub fn contains(self, pattern: &str) -> Expr {
        Expr::StringExpr {
            input: Box::new(self.expr),
            op: StringOp::Contains(pattern.to_string()),
        }
    }

    pub fn starts_with(self, prefix: &str) -> Expr {
        Expr::StringExpr {
            input: Box::new(self.expr),
            op: StringOp::StartsWith(prefix.to_string()),
        }
    }

    pub fn ends_with(self, suffix: &str) -> Expr {
        Expr::StringExpr {
            input: Box::new(self.expr),
            op: StringOp::EndsWith(suffix.to_string()),
        }
    }
}

// ---- Top-level expression constructors ----

/// Create a column reference expression.
pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_string())
}

/// Create a literal value expression.
pub fn lit<T: IntoLit>(value: T) -> Expr {
    value.into_lit()
}

/// Trait for converting values into literal expressions.
pub trait IntoLit {
    fn into_lit(self) -> Expr;
}

impl IntoLit for i64 {
    fn into_lit(self) -> Expr {
        Expr::Literal(LitValue::Int64(self))
    }
}

impl IntoLit for i32 {
    fn into_lit(self) -> Expr {
        Expr::Literal(LitValue::Int64(self as i64))
    }
}

impl IntoLit for f64 {
    fn into_lit(self) -> Expr {
        Expr::Literal(LitValue::Float64(self))
    }
}

impl IntoLit for f32 {
    fn into_lit(self) -> Expr {
        Expr::Literal(LitValue::Float64(self as f64))
    }
}

impl IntoLit for &str {
    fn into_lit(self) -> Expr {
        Expr::Literal(LitValue::Utf8(self.to_string()))
    }
}

impl IntoLit for bool {
    fn into_lit(self) -> Expr {
        Expr::Literal(LitValue::Boolean(self))
    }
}

impl IntoLit for String {
    fn into_lit(self) -> Expr {
        Expr::Literal(LitValue::Utf8(self))
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "col(\"{}\")", name),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::BinaryExpr { left, op, right } => {
                write!(f, "({} {:?} {})", left, op, right)
            }
            Expr::Agg { input, func } => write!(f, "{}.{:?}()", input, func),
            Expr::Alias { expr, name } => write!(f, "{}.alias(\"{}\")", expr, name),
            Expr::Sort { expr, options } => {
                write!(f, "{}.sort(desc={})", expr, options.descending)
            }
            Expr::StringExpr { input, op } => write!(f, "{}.str.{:?}", input, op),
            Expr::Rolling {
                input,
                func,
                window_size,
            } => write!(f, "{}.rolling_{:?}({})", input, func, window_size),
            Expr::Window {
                input,
                partition_by,
            } => write!(f, "{}.over({:?})", input, partition_by),
            Expr::Wildcard => write!(f, "*"),
            Expr::Not(e) => write!(f, "!{}", e),
            Expr::IsNull(e) => write!(f, "{}.is_null()", e),
            Expr::IsNotNull(e) => write!(f, "{}.is_not_null()", e),
            Expr::Cast { expr, dtype } => write!(f, "{}.cast({})", expr, dtype),
            Expr::DateExpr { input, part } => write!(f, "{}.dt.{:?}()", input, part),
        }
    }
}
