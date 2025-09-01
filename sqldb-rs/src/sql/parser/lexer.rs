use crate::error::{Error, Result};
use std::{fmt::Display, iter::Peekable, str::Chars};

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
    Update,
    Set,
    Where,
    Delete,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    As,
    Cross,
    Join,
    Left,
    Right,
    On,
    Group,
}

impl Keyword {
    pub fn from_str(index: &str) -> Option<Self> {
        Some(match index.to_uppercase().as_str() {
            "CREATE" => Self::Create,
            "TABLE" => Self::Table,
            "INT" => Self::Int,
            "INTEGER" => Self::Integer,
            "BOOLEAN" => Self::Boolean,
            "BOOL" => Self::Bool,
            "STRING" => Self::String,
            "TEXT" => Self::Text,
            "VARCHAR" => Self::Varchar,
            "FLOAT" => Self::Float,
            "DOUBLE" => Self::Double,
            "SELECT" => Self::Select,
            "FROM" => Self::From,
            "INSERT" => Self::Insert,
            "INTO" => Self::Into,
            "VALUES" => Self::Values,
            "TRUE" => Self::True,
            "FALSE" => Self::False,
            "DEFAULT" => Self::Default,
            "NOT" => Self::Not,
            "NULL" => Self::Null,
            "PRIMARY" => Self::Primary,
            "KEY" => Self::Key,
            "UPDATE" => Self::Update,
            "WHERE" => Self::Where,
            "SET" => Self::Set,
            "DELETE" => Self::Delete,
            "ORDER" => Self::Order,
            "BY" => Self::By,
            "ASC" => Self::Asc,
            "DESC" => Self::Desc,
            "LIMIT" => Self::Limit,
            "OFFSET" => Self::Offset,
            "AS" => Self::As,
            "CROSS" => Self::Cross,
            "JOIN" => Self::Join,
            "LEFT" => Self::Left,
            "RIGHT" => Self::Right,
            "ON" => Self::On,
            "GROUP" => Self::Group,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Self::Create => "CREATE",
            Self::Table => "TABLE",
            Self::Int => "INT",
            Self::Integer => "INTEGER",
            Self::Boolean => "BOOLEAN",
            Self::Bool => "BOOL",
            Self::String => "STRING",
            Self::Text => "TEXT",
            Self::Varchar => "VARCHAR",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::Select => "SELECT",
            Self::From => "FROM",
            Self::Insert => "INSERT",
            Self::Into => "INTO",
            Self::Values => "VALUES",
            Self::True => "TRUE",
            Self::False => "FALSE",
            Self::Default => "DEFAULT",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Primary => "PRIMARY",
            Self::Key => "KEY",
            Self::Update => "UPDATE",
            Self::Set => "SET",
            Self::Where => "WHERE",
            Self::Delete => "DELETE",
            Self::Order => "ORDER",
            Self::By => "BY",
            Self::Asc => "ASC",
            Self::Desc => "DESC",
            Self::Limit => "LIMIT",
            Self::Offset => "OFFSET",
            Self::As => "AS",
            Self::Cross => "CROSS",
            Self::Join => "JOIN",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::On => "ON",
            Self::Group => "GROUP",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 关键字
    Keyword(Keyword),
    // 其他类型的字符串Token，比如表名、列名
    Ident(String),
    // 字符串类型的数据
    String(String),
    // 数值类型，比如整数和浮点数
    Number(String),
    // 左括号 (
    OpenParen,
    // 右括号 )
    CloseParen,
    // 逗号 ,
    Comma,
    // 分号 ;
    Semicolon,
    // 星号 *
    Asterisk,
    // 加号 +
    Plus,
    // 减号 -
    Minus,
    // 斜杠 /
    Slash,
    // 等于 =
    Equal,
    // 大于
    GreaterThan,
    // 小于
    LessThan,
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(string) => string,
            Token::Number(number) => number,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
            Token::Equal => "=",
            Token::GreaterThan => ">",
            Token::LessThan => "<",
        })
    }
}

// See README.md for lexer grammar
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self
                .iter
                .peek()
                .map(|c| Err(Error::Parse(format!("[Lexer] Unexpected character {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}

impl<'a> Lexer<'a> {
    pub fn new(sql_text: &'a str) -> Self {
        Self {
            iter: sql_text.chars().peekable(),
        }
    }

    // 清楚空白字符
    fn erase_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        // 这里的 |&c| 是模式匹配：把 &&char 解引用一次变成 &char，命名为 c
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }
        Some(value).filter(|v| !v.is_empty())
    }

    // 只有是 Token 类型，才能跳转下一个，并返回 Token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let value = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(value)
    }

    // 扫描得到下一个 Token
    fn scan(&mut self) -> Result<Option<Token>> {
        // 清除字符串中空白的部分
        self.erase_whitespace();

        match self.iter.peek() {
            // 扫描字符串
            Some('\'') => self.scan_string(),
            // 扫描数字
            Some(c) if c.is_ascii_digit() => self.scan_number(), // 扫描数字
            Some(c) if c.is_alphabetic() => self.scan_ident_or_keyword(), // 扫描 Ident
            Some(_) => self.scan_symbol(),                       // 扫描符号
            None => Ok(None),
        }
    }

    // 扫描符号
    fn scan_symbol(&mut self) -> Result<Option<Token>> {
        Ok(self.next_if_token(|c| match c {
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            '=' => Some(Token::Equal),
            '>' => Some(Token::GreaterThan),
            '<' => Some(Token::LessThan),
            _ => None,
        }))
    }

    // 扫描 Ident 类型，例如：表名、列名等(也有可能是关键字： True or false)
    fn scan_ident_or_keyword(&mut self) -> Result<Option<Token>> {
        let mut value: String = match self.next_if(|c| c.is_alphabetic()) {
            Some(first) => first.to_string(),
            None => return Ok(None),
        };

        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c == '_') {
            value.push(c);
        }

        let res: Token = Keyword::from_str(&value) // 尝试将字符串解析为关键字
            .map_or(
                // 如果解析失败，使用默认值
                Token::Ident(value.to_lowercase()), // 失败时返回标识符（转为小写）
                Token::Keyword,                     // 成功时返回关键字枚举
            );

        Ok(Some(res))
    }

    // 扫描数字
    fn scan_number(&mut self) -> Result<Option<Token>> {
        let mut num = if let Some(n) = self.next_while(|c| c.is_ascii_digit()) {
            n
        } else {
            return Ok(None);
        };

        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);

            if let Some(dot_num) = self.next_while(|c| c.is_ascii_digit()) {
                num.push_str(&dot_num);
            } else {
                // 这里认为数字和小数点后面还应该街上数字。
                return Err(Error::Parse(format!(
                    "[Lexer] Unexpected end of number with dot: {}",
                    num
                )));
            }
        }

        Ok(Some(Token::Number(num)))
    }

    fn scan_string(&mut self) -> Result<Option<Token>> {
        // 判断是否为单引号开头
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }

        let mut val = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,
                Some(c) => val.push(c),
                None => return Err(Error::Parse(format!("[Lexer] Unexpected end of string"))),
            }
        }

        Ok(Some(Token::String(val)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let tokens = Lexer::new(
            "create table tbl
                        (
                            id1 int primary key,
                            id2 integer
                        );
                        ",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        println!("{:?}", tokens);
        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_create_case1() -> Result<()> {
        let tokens = Lexer::new(
            "create table tbl
                        (
                            id1 int primary key,
                            id2 integer
                        );",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_case2() -> Result<()> {
        let tokens = Lexer::new(
            "create table tbl
                        (
                            id1 int primary key,
                            id2 integer,
                            c1 bool null,
                            c2 boolean not null,
                            c3 float null,
                            c4 double,
                            c5 string,
                            c6 text,
                            c7 varchar default 'foo',
                            c8 int default 100,
                            c9 integer
                        );",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::Comma,
                Token::Ident("c1".to_string()),
                Token::Keyword(Keyword::Bool),
                Token::Keyword(Keyword::Null),
                Token::Comma,
                Token::Ident("c2".to_string()),
                Token::Keyword(Keyword::Boolean),
                Token::Keyword(Keyword::Not),
                Token::Keyword(Keyword::Null),
                Token::Comma,
                Token::Ident("c3".to_string()),
                Token::Keyword(Keyword::Float),
                Token::Keyword(Keyword::Null),
                Token::Comma,
                Token::Ident("c4".to_string()),
                Token::Keyword(Keyword::Double),
                Token::Comma,
                Token::Ident("c5".to_string()),
                Token::Keyword(Keyword::String),
                Token::Comma,
                Token::Ident("c6".to_string()),
                Token::Keyword(Keyword::Text),
                Token::Comma,
                Token::Ident("c7".to_string()),
                Token::Keyword(Keyword::Varchar),
                Token::Keyword(Keyword::Default),
                Token::String("foo".to_string()),
                Token::Comma,
                Token::Ident("c8".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Default),
                Token::Number("100".to_string()),
                Token::Comma,
                Token::Ident("c9".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_insert_case1() -> Result<()> {
        let tokens = Lexer::new("insert into tbl values (1, 2, '3', true, false, 4.55);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_insert_case2() -> Result<()> {
        let tokens = Lexer::new(
            "INSERT INTO      tbl (id, name, age) values (1, 2, '3', true, false, 4.55);",
        )
        .peekable()
        .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id".to_string()),
                Token::Comma,
                Token::Ident("name".to_string()),
                Token::Comma,
                Token::Ident("age".to_string()),
                Token::CloseParen,
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::Number("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_lexer_select_case1() -> Result<()> {
        let tokens = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("tbl".to_string()),
                Token::Semicolon,
            ]
        );

        Ok(())
    }
}
