use arrow2::datatypes::DataType as ArrowDataType;

/// DataType enum representing all supported types in the Blazer engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp,
    Null,
}

impl DataType {
    /// Convert to Arrow2 DataType
    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Int8 => ArrowDataType::Int8,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::UInt8 => ArrowDataType::UInt8,
            DataType::UInt16 => ArrowDataType::UInt16,
            DataType::UInt32 => ArrowDataType::UInt32,
            DataType::UInt64 => ArrowDataType::UInt64,
            DataType::Float16 => ArrowDataType::Float16,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::LargeUtf8 => ArrowDataType::LargeUtf8,
            DataType::Binary => ArrowDataType::Binary,
            DataType::LargeBinary => ArrowDataType::LargeBinary,
            DataType::Date32 => ArrowDataType::Date32,
            DataType::Date64 => ArrowDataType::Date64,
            DataType::Timestamp => {
                ArrowDataType::Timestamp(arrow2::datatypes::TimeUnit::Microsecond, None)
            }
            DataType::Null => ArrowDataType::Null,
        }
    }

    /// Convert from Arrow2 DataType
    pub fn from_arrow(arrow: &ArrowDataType) -> Self {
        match arrow {
            ArrowDataType::Boolean => DataType::Boolean,
            ArrowDataType::Int8 => DataType::Int8,
            ArrowDataType::Int16 => DataType::Int16,
            ArrowDataType::Int32 => DataType::Int32,
            ArrowDataType::Int64 => DataType::Int64,
            ArrowDataType::UInt8 => DataType::UInt8,
            ArrowDataType::UInt16 => DataType::UInt16,
            ArrowDataType::UInt32 => DataType::UInt32,
            ArrowDataType::UInt64 => DataType::UInt64,
            ArrowDataType::Float16 => DataType::Float16,
            ArrowDataType::Float32 => DataType::Float32,
            ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Utf8 => DataType::Utf8,
            ArrowDataType::LargeUtf8 => DataType::LargeUtf8,
            ArrowDataType::Binary => DataType::Binary,
            ArrowDataType::LargeBinary => DataType::LargeBinary,
            ArrowDataType::Date32 => DataType::Date32,
            ArrowDataType::Date64 => DataType::Date64,
            ArrowDataType::Timestamp(_, _) => DataType::Timestamp,
            ArrowDataType::Null => DataType::Null,
            _ => DataType::Null,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
        )
    }

    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }

    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
