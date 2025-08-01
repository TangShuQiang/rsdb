use std::iter::Peekable;

use ast::Column;
use lexer::{Keyword, Lexer, Token};

use super::types::DataType;
use crate::error::{Error, Result};

pub mod ast;
mod lexer;

// 解析器
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser {
            lexer: Lexer::new(input).peekable(),
        }
    }

    // 解析，获取到AST
    pub fn parse(&mut self) -> Result<ast::Statement> {
        let stmt = self.parse_statement()?;
        // 期望sql语句的最后是分号
        self.next_expect(Token::Semicolon)?;
        // 分号后面不能有其他 Token
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!(
                "[Parse] Unexpected token after statement: {}",
                token
            )));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<ast::Statement> {
        // 查看第一个 Token 类型
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(t) => Err(Error::Parse(format!("[Parse] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parse] Unexpected end of input"))),
        }
    }

    // 解析 DDL 语句
    fn parse_ddl(&mut self) -> Result<ast::Statement> {
        match self.next()? {
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parse] Unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("[Parse] Unexpected token {}", token))),
        }
    }

    // 解析 Select 语句
    fn parse_select(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Select))?;
        self.next_expect(Token::Asterisk)?;
        self.next_expect(Token::Keyword(Keyword::From))?;

        // 表名
        let table_name = self.next_ident()?;
        Ok(ast::Statement::Select { table_name })
    }

    // 解析 Insert 语句
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;

        // 表名
        let table_name = self.next_ident()?;

        // 查看是否给指定的列进行 insert
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut cols = Vec::new();
            loop {
                cols.push(self.next_ident()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => continue,
                    token => {
                        return Err(Error::Parse(format!("[Parse] Unexpected token {}", token)));
                    }
                }
            }
            Some(cols)
        } else {
            None
        };

        // 解析 value 信息
        self.next_expect(Token::Keyword(Keyword::Values))?;
        let mut values = Vec::new();
        loop {
            self.next_expect(Token::OpenParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => continue,
                    token => {
                        return Err(Error::Parse(format!("[Parse] Unexpected token {}", token)));
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

    // 解析 Create Table 语句
    fn parse_ddl_create_table(&mut self) -> Result<ast::Statement> {
        // 期待是 Table 名
        let table_name = self.next_ident()?;
        // 表名后面是左括号
        self.next_expect(Token::OpenParen)?;
        // 解析列信息
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_ddl_column()?);
            // 如果没有逗号，列解析完成
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

    // 解析列信息
    fn parse_ddl_column(&mut self) -> Result<Column> {
        let mut column = Column {
            name: self.next_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => {
                    DataType::Boolean
                }
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => {
                    DataType::Integer
                }
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String)
                | Token::Keyword(Keyword::Text)
                | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parse] Unexpected token {}", token))),
            },
            nullable: None,
            default: None,
            primary_key: false,
        };
        // 解析列的默认值，以及是否可以为空
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Primary => {
                    self.next_expect(Token::Keyword(Keyword::Key))?;
                    column.primary_key = true;
                }
                k => return Err(Error::Parse(format!("[Parse] Unexpected keyword {}", k))),
            }
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
            t => return Err(Error::Parse(format!("[Parse] Unexpected token {}", t))),
        })
    }

    fn next_if_token(&mut self, token: Token) -> Option<Token> {
        self.next_if(|t| t == &token)
    }

    // 如果下一个 Token 是关键字，则跳过并返回该 Token
    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    // 如果满足条件，则跳转并返回该 Token
    fn next_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predicate(t))?;
        self.next().ok()
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer
            .next()
            .unwrap_or_else(|| Err(Error::Parse(format!("[Parse] Unexpected end of input"))))
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!(
                "[Parse] Expected ident, got token {}",
                token
            ))),
        }
    }

    fn next_expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!(
                "[Parse] Expected token {}, got token {}",
                expect, token
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::{error::Result, sql::parser::ast};

    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql1 = "
            create table tab1 (
                a int,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stm1 = Parser::new(sql1).parse()?;

        let sql2 = "
            create    table   tab1 (
                a    int,
                b float not   null ,
                c varchar null,
                d   bool default   true
            );
        ";
        let stm2 = Parser::new(sql2).parse()?;
        assert_eq!(stm1, stm2);

        let sql3 = "
            create table tab1 (
                a int,
                b float not null,
                c varchar null,
                d bool default true
            )
        ";
        let stm3 = Parser::new(sql3).parse();
        assert!(stm3.is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let sql1 = "
            insert into tab1 values (1, 2, 3, 'a', true);
        ";
        let stm1 = Parser::new(sql1).parse()?;
        assert_eq!(
            stm1,
            ast::Statement::Insert {
                table_name: "tab1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into()
                ]]
            }
        );

        let sql2 = "
            insert into tab1 (c1, c2, c3) values (3, 'a', true), (4, 'b', false);
        ";
        let stm2 = Parser::new(sql2).parse()?;
        assert_eq!(
            stm2,
            ast::Statement::Insert {
                table_name: "tab1".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into()
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into()
                    ]
                ]
            }
        );
        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let sql = "select * from tab1;";
        let stm = Parser::new(sql).parse()?;
        assert_eq!(
            stm,
            ast::Statement::Select {
                table_name: "tab1".to_string()
            }
        );
        Ok(())
    }
}
