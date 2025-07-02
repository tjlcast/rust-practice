use crate::error::{Error, Result};
use std::{iter::Peekable, str::Chars};
/*

CREATE TABLE table_name (
    [ column_name data_type [column_constraint [...] ]]
    [, ...]
);

where data_type is
    - BOOLEAN(BOOL): true | false
    - FLOAT(DOUBLE)
    - INTEGER(INT)
    - STRING(TEXT, VARCHAR)

where column_constraint is:
    [NOT NULL | NULL | DEFAULT expr ]

*/

/*

INSERT INTO table_name
[ ( column_name [,...] ) ]
values (expr [,...]);

*/

/*

SELECT * FROM table_name;

*/
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
            _ => return None,
        })
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
}

pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
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
        let mut value = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(value)
    }

    // 扫描得到下一个token
    fn scan(&mut self) -> Result<Token> {
        // 清除字符串中空白的部分
        self.erase_whitespace();

        match self.iter.peek() {
            // 扫描字符串
            Some('\'') => self.scan_string(),
            // 扫描数字
            Some(c) if c.is_ascii_digit() => self.scan_number(), // 扫描数字
            Some(c) if c.is_alphabetic() => self.scan_ident(),   // 扫描 Ident
            Some(_) => self.scan_symbol(),
            None => Ok(None),
        };

        todo!()
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
            _ => None,
        }))
    }

    // 扫描 Ident 类型，例如：表名、列名等(也有可能是关键字： True or false)
    fn scan_ident(&mut self) -> Result<Option<Token>> {
        let mut value: String = match self.next_if(|c| c.is_alphabetic()) {
            Some(first) => first.to_string(),
            None => return Ok(None),
        };

        while let Some(c) = self.next_if(|c| c.is_alphabetic() || c == '_') {
            value.push(c);
        }

        let res: Token =
            Keyword::from_str(&value).map_or(Token::Ident(value.to_lowercase()), Token::Keyword);

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
