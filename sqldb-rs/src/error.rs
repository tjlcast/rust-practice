use std::sync::PoisonError;

use bincode::ErrorKind;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Parse(String),
    Internal(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Parse(e) => write!(f, "{}", e),
            Error::Internal(e) => write!(f, "{}", e),
        }
    }
}

// 将 std::num::ParseIntError（整数解析错误）自动转换为自定义的 Error::Parse 类型
impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error::Parse(value.to_string())
    }
}

// 将 std::num::ParseFloatError（浮点数解析错误）自动转换为自定义的 Error::Parse 类型
impl From<std::num::ParseFloatError> for Error {
    fn from(value: std::num::ParseFloatError) -> Self {
        Error::Parse(value.to_string())
    }
}

// 将 std::str::Utf8Error（UTF-8 解析错误）自动转换为自定义的 Error::Parse 类型
impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::Internal(value.to_string())
    }
}

// 将 bincode::ErrorKind（bincode 解码错误）自动转换为自定义的 Error::Internal 类型
impl From<Box<ErrorKind>> for Error {
    fn from(value: Box<ErrorKind>) -> Self {
        Error::Internal(value.to_string())
    }
}
