use crate::dtype::DataType;
use ahash::HashMap;

/// A field in a schema: name + data type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub dtype: DataType,
}

impl Field {
    pub fn new(name: &str, dtype: DataType) -> Self {
        Field {
            name: name.to_string(),
            dtype,
        }
    }
}

/// Ordered field map representing a DataFrame schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    fields: Vec<Field>,
    pub(crate) index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        let index = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        Schema { fields, index }
    }

    pub fn empty() -> Self {
        Schema {
            fields: Vec::new(),
            index: HashMap::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.index.get(name).map(|&i| &self.fields[i])
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    pub fn dtype(&self, name: &str) -> Option<&DataType> {
        self.field(name).map(|f| &f.dtype)
    }

    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    pub fn to_arrow(&self) -> arrow2::datatypes::Schema {
        let fields: Vec<arrow2::datatypes::Field> = self
            .fields
            .iter()
            .map(|f| arrow2::datatypes::Field::new(&f.name, f.dtype.to_arrow(), true))
            .collect();
        arrow2::datatypes::Schema::from(fields)
    }

    pub fn from_arrow(schema: &arrow2::datatypes::Schema) -> Self {
        let fields = schema
            .fields
            .iter()
            .map(|f| Field::new(&f.name, DataType::from_arrow(&f.data_type)))
            .collect();
        Schema::new(fields)
    }

    pub fn merge(&self, other: &Schema) -> Self {
        let mut fields = self.fields.clone();
        for f in &other.fields {
            if !self.index.contains_key(&f.name) {
                fields.push(f.clone());
            }
        }
        Schema::new(fields)
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for field in &self.fields {
            writeln!(f, "  {} ({})", field.name, field.dtype)?;
        }
        Ok(())
    }
}
