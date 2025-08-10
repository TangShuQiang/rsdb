use std::{fmt::Display, iter::Peekable, str::Chars};

use crate::error::{RSDBError, RSDBResult};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword), // 关键字
    Ident(String),    // 其他类型的字符串Token，如表名、列名等
    String(String),   // 字符串类型
    Number(String),   // 数值类型
    OpenParen,        // 左括号 (
    CloseParen,       // 右括号 )
    Comma,            // 逗号 ，
    Semicolon,        // 分号 ；
    Asterisk,         // 星号 *
    Plus,             // 加号 +
    Minus,            // 减号 -
    Slash,            // 斜杠 /
    Equal,            // 等号 =
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(s) => s,
            Token::Number(n) => n,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
            Token::Equal => "=",
        })
    }
}

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
    // 将字符串转换为对应的 Keyword 枚举
    pub fn from_str(ident: &str) -> Option<Self> {
        Some(match ident.to_uppercase().as_ref() {
            "CREATE" => Keyword::Create,
            "TABLE" => Keyword::Table,
            "INT" => Keyword::Int,
            "INTEGER" => Keyword::Integer,
            "BOOLEAN" => Keyword::Boolean,
            "BOOL" => Keyword::Bool,
            "STRING" => Keyword::String,
            "TEXT" => Keyword::Text,
            "VARCHAR" => Keyword::Varchar,
            "FLOAT" => Keyword::Float,
            "DOUBLE" => Keyword::Double,
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "INSERT" => Keyword::Insert,
            "INTO" => Keyword::Into,
            "VALUES" => Keyword::Values,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "DEFAULT" => Keyword::Default,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            "UPDATE" => Keyword::Update,
            "SET" => Keyword::Set,
            "WHERE" => Keyword::Where,
            "DELETE" => Keyword::Delete,
            "ORDER" => Keyword::Order,
            "BY" => Keyword::By,
            "ASC" => Keyword::Asc,
            "DESC" => Keyword::Desc,
            "LIMIT" => Keyword::Limit,
            "OFFSET" => Keyword::Offset,
            "AS" => Keyword::As,
            "CROSS" => Keyword::Cross,
            "JOIN" => Keyword::Join,
            "LEFT" => Keyword::Left,
            "RIGHT" => Keyword::Right,
            "ON" => Keyword::On,
            "GROUP" => Keyword::Group,
            _ => return None,
        })
    }

    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
            Keyword::Update => "UPDATE",
            Keyword::Set => "SET",
            Keyword::Where => "WHERE",
            Keyword::Delete => "DELETE",
            Keyword::Order => "ORDER",
            Keyword::By => "BY",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
            Keyword::As => "AS",
            Keyword::Cross => "CROSS",
            Keyword::Join => "JOIN",
            Keyword::Left => "LEFT",
            Keyword::Right => "RIGHT",
            Keyword::On => "ON",
            Keyword::Group => "GROUP",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

// 词法分析 Lexer 定义
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>,
}

// 自定义迭代器，返回 Token
impl<'a> Iterator for Lexer<'a> {
    type Item = RSDBResult<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self.iter.peek().map(|c| {
                Err(RSDBError::Parse(format!(
                    "[Lexer] Unexpected character: {}",
                    c
                )))
            }),
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

    // 清除空白字符
    // select *   from   t;
    fn erase_whitespace(&mut self) {
        self.next_while(|c| c.is_whitespace());
    }

    // 判断当前字符是否满足条件，如果是的话就跳转到下一个字符
    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&predicate) {
            value.push(c);
        }
        Some(value).filter(|v| !v.is_empty())
    }

    // 如果满足条件，则跳转到下一个字符，并返回该字符
    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.iter.peek().filter(|&c| predicate(*c))?;
        self.iter.next()
    }

    // 只有是 Token 类型，才跳转到到下一个，并返回 Token
    fn next_if_token<F: Fn(char) -> Option<Token>>(&mut self, predicate: F) -> Option<Token> {
        let token = self.iter.peek().and_then(|c| predicate(*c))?;
        self.iter.next();
        Some(token)
    }

    // 扫描拿到下一个Token
    fn scan(&mut self) -> RSDBResult<Option<Token>> {
        // 清除空白字符
        self.erase_whitespace();
        // 根据第一个字符判断
        match self.iter.peek() {
            Some('\'') => self.scan_string(), // 扫描字符串
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()), // 扫描数字
            Some(c) if c.is_ascii_alphabetic() => Ok(self.scan_ident()), // 扫描 Ident 类型
            Some(_) => Ok(self.scan_symbol()), // 扫描符号
            None => Ok(None),
        }
    }

    // 扫描字符串
    fn scan_string(&mut self) -> RSDBResult<Option<Token>> {
        // 判断是否是单引号开头
        if self.next_if(|c| c == '\'').is_none() {
            return Ok(None);
        }
        let mut value = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,      // 遇到单引号结束
                Some(c) => value.push(c), // 其他字符加入到字符串中
                None => {
                    return Err(RSDBError::Parse(format!(
                        "[Lexer] Unterminated string literal: {}",
                        value
                    )));
                } // 如果没有遇到单引号，说明字符串没有结束
            }
        }
        Ok(Some(Token::String(value)))
    }

    // 扫描数字
    fn scan_number(&mut self) -> Option<Token> {
        // 先扫描一部分
        let mut num = self.next_while(|c| c.is_ascii_digit())?;
        if let Some(sep) = self.next_if(|c| c == '.') {
            num.push(sep);
            // 扫描小数点后面的部分
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }
        Some(Token::Number(num))
    }

    // 扫描 Ident 类型, 如表名、列名等，也有可能是关键字，true / false
    fn scan_ident(&mut self) -> Option<Token> {
        let mut value = self.next_if(|c| c.is_ascii_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_ascii_alphanumeric() || c == '_') {
            value.push(c);
        }
        Some(Keyword::from_str(&value).map_or(Token::Ident(value.to_lowercase()), Token::Keyword))
    }

    // 扫描符号
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c {
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            '=' => Some(Token::Equal),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::Lexer;
    use crate::{
        error::RSDBResult,
        sql::parser::lexer::{Keyword, Token},
    };

    #[test]
    fn test_lexer_create_table() -> RSDBResult<()> {
        let tokens1 = Lexer::new(
            "CREATE table tbl
                (
                    id1 int primary key,
                    id2 integer
                );
                ",
        )
        .peekable()
        .collect::<RSDBResult<Vec<_>>>()?;

        assert_eq!(
            tokens1,
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
                Token::Semicolon
            ]
        );

        let tokens2 = Lexer::new(
            "CREATE table tbl
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
                        );
                        ",
        )
        .peekable()
        .collect::<RSDBResult<Vec<_>>>()?;

        assert!(tokens2.len() > 0);

        Ok(())
    }

    #[test]
    fn test_lexer_insert_into() -> RSDBResult<()> {
        let tokens1 = Lexer::new("insert into tbl values (1, 2, '3', true, false, 4.55);")
            .peekable()
            .collect::<RSDBResult<Vec<_>>>()?;

        assert_eq!(
            tokens1,
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

        let tokens2 = Lexer::new("INSERT INTO       tbl (id, name, age) values (100, 'db', 10);")
            .peekable()
            .collect::<RSDBResult<Vec<_>>>()?;

        assert_eq!(
            tokens2,
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
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_select() -> RSDBResult<()> {
        let tokens1 = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<RSDBResult<Vec<_>>>()?;

        assert_eq!(
            tokens1,
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
