#[derive(Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Debug)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}
