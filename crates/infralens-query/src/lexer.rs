//! Hand-written lexer for IQL.  Converts a UTF-8 string into a `Vec<Token>`.

use crate::error::QueryError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select, From, Where, And, Or, Not, Is, Null, In, Between, Like,
    Join, Inner, Left, Right, On, As, Group, By, Having, Order, Asc, Desc,
    Limit, Offset, True, False, Distinct, Interval,

    // Identifiers and literals
    Ident(String),
    IntLit(i64),
    FloatLit(f64),
    StrLit(String),

    // Operators
    Plus, Minus, Star, Slash, Percent,
    Eq, NotEq, Lt, LtEq, Gt, GtEq,
    LParen, RParen, Comma, Dot, Semicolon,

    // Specials
    Eof,
}

pub struct Lexer<'a> {
    src:  &'a [u8],
    pos:  usize,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Self { src: src.as_bytes(), pos: 0 }
    }

    pub fn tokenise(&mut self) -> Result<Vec<Token>, QueryError> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace_and_comments();
            if self.pos >= self.src.len() {
                tokens.push(Token::Eof);
                break;
            }
            let tok = self.next_token()?;
            tokens.push(tok);
        }
        Ok(tokens)
    }

    fn peek(&self) -> u8 { if self.pos < self.src.len() { self.src[self.pos] } else { 0 } }
    fn peek2(&self) -> u8 { if self.pos + 1 < self.src.len() { self.src[self.pos + 1] } else { 0 } }
    fn advance(&mut self) -> u8 { let c = self.peek(); self.pos += 1; c }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            match self.peek() {
                b' ' | b'\t' | b'\n' | b'\r' => { self.pos += 1; }
                b'-' if self.peek2() == b'-' => {
                    while self.pos < self.src.len() && self.peek() != b'\n' { self.pos += 1; }
                }
                _ => break,
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, QueryError> {
        let c = self.peek();
        match c {
            b'+' => { self.advance(); Ok(Token::Plus)    }
            b'-' => { self.advance(); Ok(Token::Minus)   }
            b'*' => { self.advance(); Ok(Token::Star)    }
            b'/' => { self.advance(); Ok(Token::Slash)   }
            b'%' => { self.advance(); Ok(Token::Percent) }
            b'(' => { self.advance(); Ok(Token::LParen)  }
            b')' => { self.advance(); Ok(Token::RParen)  }
            b',' => { self.advance(); Ok(Token::Comma)   }
            b'.' => { self.advance(); Ok(Token::Dot)     }
            b';' => { self.advance(); Ok(Token::Semicolon) }
            b'=' => { self.advance(); Ok(Token::Eq) }
            b'<' => {
                self.advance();
                if self.peek() == b'=' { self.advance(); Ok(Token::LtEq) }
                else if self.peek() == b'>' { self.advance(); Ok(Token::NotEq) }
                else { Ok(Token::Lt) }
            }
            b'>' => {
                self.advance();
                if self.peek() == b'=' { self.advance(); Ok(Token::GtEq) }
                else { Ok(Token::Gt) }
            }
            b'!' => {
                self.advance();
                if self.peek() == b'=' { self.advance(); Ok(Token::NotEq) }
                else { Err(QueryError::Lex(format!("unexpected '!' at {}", self.pos))) }
            }
            b'\'' => self.lex_string(),
            b'0'..=b'9' => self.lex_number(),
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => self.lex_ident_or_keyword(),
            b'"' => self.lex_quoted_ident(),
            other => Err(QueryError::Lex(format!(
                "unexpected byte {other:#x} ('{}')", other as char
            ))),
        }
    }

    fn lex_string(&mut self) -> Result<Token, QueryError> {
        self.advance(); // consume opening '
        let mut s = String::new();
        loop {
            match self.peek() {
                0 => return Err(QueryError::Lex("unterminated string literal".into())),
                b'\'' => {
                    self.advance();
                    if self.peek() == b'\'' { self.advance(); s.push('\''); } // '' = escaped '
                    else { break; }
                }
                c => { s.push(c as char); self.advance(); }
            }
        }
        Ok(Token::StrLit(s))
    }

    fn lex_number(&mut self) -> Result<Token, QueryError> {
        let start = self.pos;
        while self.peek().is_ascii_digit() { self.advance(); }
        if self.peek() == b'.' && self.peek2().is_ascii_digit() {
            self.advance();
            while self.peek().is_ascii_digit() { self.advance(); }
            let s = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
            return Ok(Token::FloatLit(s.parse().map_err(|e| QueryError::Lex(format!("{e}")))?));
        }
        let s = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        Ok(Token::IntLit(s.parse().map_err(|e| QueryError::Lex(format!("{e}")))?))
    }

    fn lex_ident_or_keyword(&mut self) -> Result<Token, QueryError> {
        let start = self.pos;
        while matches!(self.peek(), b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_') {
            self.advance();
        }
        let word = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        Ok(match word.to_ascii_uppercase().as_str() {
            "SELECT"   => Token::Select,   "FROM"     => Token::From,
            "WHERE"    => Token::Where,    "AND"      => Token::And,
            "OR"       => Token::Or,       "NOT"      => Token::Not,
            "IS"       => Token::Is,       "NULL"     => Token::Null,
            "IN"       => Token::In,       "BETWEEN"  => Token::Between,
            "LIKE"     => Token::Like,     "JOIN"     => Token::Join,
            "INNER"    => Token::Inner,    "LEFT"     => Token::Left,
            "RIGHT"    => Token::Right,    "ON"       => Token::On,
            "AS"       => Token::As,       "GROUP"    => Token::Group,
            "BY"       => Token::By,       "HAVING"   => Token::Having,
            "ORDER"    => Token::Order,    "ASC"      => Token::Asc,
            "DESC"     => Token::Desc,     "LIMIT"    => Token::Limit,
            "OFFSET"   => Token::Offset,   "TRUE"     => Token::True,
            "FALSE"    => Token::False,    "DISTINCT" => Token::Distinct,
            "INTERVAL" => Token::Interval,
            _          => Token::Ident(word.to_string()),
        })
    }

    fn lex_quoted_ident(&mut self) -> Result<Token, QueryError> {
        self.advance(); // consume opening "
        let mut s = String::new();
        loop {
            match self.peek() {
                0    => return Err(QueryError::Lex("unterminated quoted identifier".into())),
                b'"' => { self.advance(); break; }
                c    => { s.push(c as char); self.advance(); }
            }
        }
        Ok(Token::Ident(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenise_simple_select() {
        let mut l = Lexer::new("SELECT foo, bar FROM logs WHERE x = 1");
        let tokens = l.tokenise().unwrap();
        assert!(matches!(tokens[0], Token::Select));
        assert!(matches!(&tokens[1], Token::Ident(s) if s == "foo"));
        assert!(matches!(tokens[3], Token::From));
        assert!(matches!(&tokens[4], Token::Ident(s) if s == "logs"));
    }

    #[test]
    fn tokenise_operators() {
        let mut l = Lexer::new("<= >= != <> =");
        let tokens = l.tokenise().unwrap();
        assert_eq!(tokens[0], Token::LtEq);
        assert_eq!(tokens[1], Token::GtEq);
        assert_eq!(tokens[2], Token::NotEq);
        assert_eq!(tokens[3], Token::NotEq);
        assert_eq!(tokens[4], Token::Eq);
    }

    #[test]
    fn tokenise_string_with_escape() {
        let mut l = Lexer::new("'it''s'");
        let tokens = l.tokenise().unwrap();
        assert!(matches!(&tokens[0], Token::StrLit(s) if s == "it's"));
    }
}
