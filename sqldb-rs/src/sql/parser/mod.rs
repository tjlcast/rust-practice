use crate::error::{Error, Result};
use crate::sql::parser::ast::Column;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::sql::types::DataType;
use std::iter::Peekable;

pub mod ast;
pub mod lexer;

// 解析器定义
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }

    // 解析，获取抽象语法树
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;

        // 期望 sql 语句的最后有一个分号
        self.next_expect(Token::Semicolon)?;
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // 查看第一个 Token 类型
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token: {:?}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }

    // 解析 insert 类型
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;

        // 表名
        let table_name = self.next_indent()?;

        // 查看是否有指定的列
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_indent()?.to_string());
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!(
                            "[Parser] Unexpected token: {}",
                            token
                        )));
                    }
                }
            }
            Some(cols)
        } else {
            None
        };

        // 解析 value 信息
        self.next_expect(Token::Keyword(Keyword::Values))?;
        // inser into tbl(a, b, c) values (1, 2, 3), (3, 4, 5);
        let mut values = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {}
                    token => {
                        return Err(Error::Parse(format!(
                            "[Parser] Unexpected token: {}",
                            token
                        )));
                    }
                }
            }
            values.push(exprs);

            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        Ok(ast::Statement::Insert {
            table_name,
            columns,
            values,
        })
    }

    // 解析 select 类型
    fn parse_select(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Select))?;
        self.next_expect(Token::Asterisk)?;
        self.next_expect(Token::Keyword(Keyword::From))?;

        // 解析表名
        let table_name = self.next_indent()?;

        Ok(ast::Statement::Select { table_name })
    }

    // 解析 DDL 类型
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            //  再读入一个 token
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!(
                    "[Parser] Unexpected token: {}",
                    token
                ))),
            },
            token => Err(Error::Parse(format!(
                "[Parser] Unexpected end of input {}",
                token
            ))),
        }
    }

    // 解析 DDL 类型
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        // 期望是 Table 名
        let table_name = self.next_indent()?;
        // 表名之后是括号
        self.next_expect(Token::OpenParen)?;

        // 括号之后是列的信息
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            // 如果后面没有逗号，列解析完成，退出
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }

        self.next_expect(Token::CloseParen)?;
        Ok(ast::Statement::CreateTable {
            name: table_name,
            columns,
        })
    }

    fn parse_ddl_column(&mut self) -> Result<ast::Column> {
        let mut column = Column {
            name: self.next_indent()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => {
                    DataType::Integer
                }
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => {
                    DataType::Boolean
                }
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String)
                | Token::Keyword(Keyword::Text)
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => {
                    return Err(Error::Parse(format!(
                        "[Parser] Unexpected token: {}",
                        token
                    )));
                }
            },
            nullable: None,
            default: None,
        };

        // 解析列的默认值和是否可以为空
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    // 必须为 not null
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword: {}", k))),
            };
        }

        Ok(column)
    }

    // 解析表达式
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    // 整数
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    // 浮点数
                    ast::Consts::Float(n.parse()?).into()
                }
            }
            Token::String(s) => ast::Consts::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            t => {
                return Err(Error::Parse(format!(
                    "[Parse] Unexpected expression token {}",
                    t
                )));
            }
        })
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer
            .next()
            .unwrap_or_else(|| Err(Error::Parse(format!("[Parser] unexpected end of input"))))
    }

    /// 获取下一个标记，并期望它是一个标识符（indent）。
    ///
    /// 这个方法会消耗迭代器中的一个标记。
    ///
    /// # 返回值
    /// 如果下一个标记是一个标识符，则返回该标识符的字符串表示。
    ///
    /// # 错误
    /// 如果下一个标记不是标识符，则返回一个包含错误信息的 `Err`。
    fn next_indent(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!(
                "[Parser] Expected indent, but got token {}",
                token
            ))),
        }
    }

    /// 只有当前token是指定的token的时候返回，否则报错(返回Err)
    /// 检查下一个标记是否与期望的标记相符，如果不相符则返回错误。
    ///
    /// 这个方法会消耗迭代器中的一个标记。
    ///
    /// # 参数
    /// * `expect` - 期望的标记。
    ///
    /// # 返回值
    /// 如果下一个标记与期望的标记相符，则返回 `Ok(())`，否则返回一个解析错误。
    ///
    /// # 错误
    /// 如果下一个标记与期望的标记不符，则返回一个包含错误信息的 `Err`。
    fn next_expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!(
                "[Parser] Expected {}, got {}",
                expect, token
            )));
        }
        Ok(())
    }

    /// 如果满足条件，则跳转到下一个 Token
    /// 如果下一个标记满足给定条件，则返回该标记，否则返回 None。
    ///
    /// 这个方法可能会消耗迭代器中的一个标记，如果满足条件的话;
    /// 如果不满足则不消耗迭代器中的标记.
    ///
    /// # 参数
    /// * `predicate` - 用于检查标记是否满足条件的闭包。
    ///
    /// # 返回值
    /// 如果下一个标记满足条件，则返回该标记，否则返回 None。
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    // 如果下一个 token 是关键字，则跳转
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::Result,
        sql::{
            parser::ast::{Expression, Statement},
            types::Value,
        },
    };

    #[test]
    fn test_parse_create_table() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true);
        ";

        let stmt1 = Parser::new(sql1).parse()?;
        println!("{:?}", stmt1);
        assert_eq!(
            stmt1,
            Statement::CreateTable {
                name: "tbl1".to_string(),
                columns: vec![
                    Column {
                        name: "a".to_string(),
                        datatype: DataType::Integer,
                        nullable: None,
                        default: Some(Expression::Consts(ast::Consts::Integer(100))),
                    },
                    Column {
                        name: "b".to_string(),
                        datatype: DataType::Float,
                        nullable: Some(false),
                        default: None,
                    },
                    Column {
                        name: "c".to_string(),
                        datatype: DataType::String,
                        nullable: Some(true),
                        default: None,
                    },
                    Column {
                        name: "d".to_string(),
                        datatype: DataType::Boolean,
                        nullable: None,
                        default: Some(Expression::Consts(ast::Consts::Boolean(true))),
                    },
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_create_table1() -> Result<()> {
        let sql1 = "
        create table tbl1 (
            a int default 100,
            b float not null     ,
            c varchar null,
            d bool default     true
        );
        ";

        let stmt1 = Parser::new(sql1).parse()?;
        println!("{:?}", stmt1);
        assert_eq!(
            stmt1,
            Statement::CreateTable {
                name: "tbl1".to_string(),
                columns: vec![
                    Column {
                        name: "a".to_string(),
                        datatype: DataType::Integer,
                        nullable: None,
                        default: Some(Expression::Consts(ast::Consts::Integer(100))),
                    },
                    Column {
                        name: "b".to_string(),
                        datatype: DataType::Float,
                        nullable: Some(false),
                        default: None,
                    },
                    Column {
                        name: "c".to_string(),
                        datatype: DataType::String,
                        nullable: Some(true),
                        default: None,
                    },
                    Column {
                        name: "d".to_string(),
                        datatype: DataType::Boolean,
                        nullable: None,
                        default: Some(Expression::Consts(ast::Consts::Boolean(true))),
                    },
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_create_table_with_err() -> Result<()> {
        let sql1 = "
            create tabl tb1 (
            a int default 100,
            b float not null     ,
            c varchar null,
            d bool default     true
        );
        ";
        let stmt1_or_err = Parser::new(sql1).parse();
        assert!(stmt1_or_err.is_err());
        match stmt1_or_err {
            Ok(_) => println!("ok"),
            Err(e) => {
                println!("err: {}", e);
                assert_eq!(e.to_string(), "[Parser] Unexpected token: tabl");
            }
        }

        Ok(())
    }

    #[test]
    fn test_parse_create_table_with_err1() -> Result<()> {
        let sql1 = "
            create table tb1 (
            a int default 100,
            b float not null     ,
            c varchar null,
            d bool default     true
        ); create
        ";
        let stmt1_or_err = Parser::new(sql1).parse();
        assert!(stmt1_or_err.is_err());
        match stmt1_or_err {
            Ok(stmt) => println!("{:?}", stmt),
            Err(e) => {
                println!("err: {}", e);
                assert_eq!(e.to_string(), "[Parser] Unexpected token CREATE");
            }
        }

        Ok(())
    }

    #[test]
    fn test_parse_insert0() -> Result<()> {
        let sql1 = "
            insert into tbl1 values (1, 2.0, 'hello', true);
        ";
        let stmt1_or_err = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1_or_err,
            Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    Expression::Consts(ast::Consts::Integer(1)),
                    Expression::Consts(ast::Consts::Float(2.0)),
                    Expression::Consts(ast::Consts::String("hello".to_string())),
                    Expression::Consts(ast::Consts::Boolean(true)),
                ]]
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_insert1() -> Result<()> {
        let sql1 = "
            insert into tbl1 (a, b, c, d) values (1, 2.0, 'hello', true);
        ";
        let stmt1_or_err = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1_or_err,
            Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: Some(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string(),
                ]),
                values: vec![vec![
                    Expression::Consts(ast::Consts::Integer(1)),
                    Expression::Consts(ast::Consts::Float(2.0)),
                    Expression::Consts(ast::Consts::String("hello".to_string())),
                    Expression::Consts(ast::Consts::Boolean(true)),
                ]]
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_insert2() -> Result<()> {
        let sql1 = "
            insert into tbl1 (a, b, c, d) values (1, 2.0, 'hello', true), (1, 2.0, 'hello', true);
        ";
        let stmt1_or_err = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1_or_err,
            Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: Some(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string(),
                ]),
                values: vec![
                    vec![
                        Expression::Consts(ast::Consts::Integer(1)),
                        Expression::Consts(ast::Consts::Float(2.0)),
                        Expression::Consts(ast::Consts::String("hello".to_string())),
                        Expression::Consts(ast::Consts::Boolean(true)),
                    ],
                    vec![
                        Expression::Consts(ast::Consts::Integer(1)),
                        Expression::Consts(ast::Consts::Float(2.0)),
                        Expression::Consts(ast::Consts::String("hello".to_string())),
                        Expression::Consts(ast::Consts::Boolean(true)),
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_parse_select() -> Result<()> {
        let sql1 = "
            select * from tbl1;
        ";
        let stmt1_or_err = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1_or_err,
            Statement::Select {
                table_name: "tbl1".to_string(),
            }
        );

        Ok(())
    }
}
