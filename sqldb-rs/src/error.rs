use serde::{de, ser};
use std::fmt::Display;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Parse(String),
    Internal(String),
    WriteConflict,
}

// impl std::fmt::Display for Error {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Error::Parse(e) => write!(f, "{}", e),
//             Error::Internal(e) => write!(f, "{}", e),
//             Error::WriteConflict => write!(f, "Write Conflict"),
//         }
//     }
// }

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
impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(value: std::sync::PoisonError<T>) -> Self {
        Error::Internal(value.to_string())
    }
}

// 将 bincode::ErrorKind（bincode 解码错误）自动转换为自定义的 Error::Internal 类型
impl From<Box<bincode::ErrorKind>> for Error {
    fn from(value: Box<bincode::ErrorKind>) -> Self {
        Error::Internal(value.to_string())
    }
}

// 将 std::io::Error（IO 错误）自动转换为自定义的 Error::Internal 类型
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Internal(value.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(value: std::array::TryFromSliceError) -> Self {
        Error::Internal(value.to_string())
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::Internal(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::Internal(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Parse(err) => write!(f, "parse error {}", err),
            Error::Internal(err) => write!(f, "internal error {}", err),
            Error::WriteConflict => write!(f, "write conflict, retry transaction"),
        }
    }
}
