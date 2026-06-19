//! Recursive-descent parser for IQL.
//!
//! Precedence (lowest → highest):
//!   OR → AND → NOT → comparison → IS/BETWEEN/LIKE/IN → addition → multiplication → unary → primary

use crate::{
    ast::*,
    error::QueryError,
    lexer::Token,
};

pub struct Parser {
    tokens: Vec<Token>,
    pos:    usize,
}

type PResult<T> = Result<T, QueryError>;

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    // ── Entry point ───────────────────────────────────────────────────────────

    pub fn parse_statement(&mut self) -> PResult<Statement> {
        let stmt = match self.peek() {
            Token::Select => Statement::Select(self.parse_select()?),
            other => return Err(QueryError::Parse(format!("expected SELECT, got {other:?}"))),
        };
        self.eat_optional(Token::Semicolon);
        self.expect(Token::Eof)?;
        Ok(stmt)
    }

    // ── SELECT statement ──────────────────────────────────────────────────────

    fn parse_select(&mut self) -> PResult<SelectStatement> {
        self.expect(Token::Select)?;
        self.eat_optional(Token::Distinct);

        let projections = self.parse_projections()?;
        self.expect(Token::From)?;
        let from  = self.parse_table_ref()?;
        let joins = self.parse_joins()?;

        let filter = if self.peek() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else { None };

        let group_by = if self.peek() == &Token::Group {
            self.advance();
            self.expect(Token::By)?;
            self.parse_comma_separated(|p| p.parse_expr())?
        } else { vec![] };

        let having = if self.peek() == &Token::Having {
            self.advance();
            Some(self.parse_expr()?)
        } else { None };

        let order_by = if self.peek() == &Token::Order {
            self.advance();
            self.expect(Token::By)?;
            self.parse_comma_separated(|p| p.parse_order_by_item())?
        } else { vec![] };

        let limit = if self.peek() == &Token::Limit {
            self.advance();
            Some(self.expect_int()? as u64)
        } else { None };

        let offset = if self.peek() == &Token::Offset {
            self.advance();
            Some(self.expect_int()? as u64)
        } else { None };

        Ok(SelectStatement { projections, from, joins, filter, group_by, having, order_by, limit, offset })
    }

    fn parse_projections(&mut self) -> PResult<Vec<Projection>> {
        self.parse_comma_separated(|p| p.parse_projection())
    }

    fn parse_projection(&mut self) -> PResult<Projection> {
        if self.peek() == &Token::Star {
            self.advance();
            return Ok(Projection::Star);
        }
        let expr  = self.parse_expr()?;
        let alias = if self.peek() == &Token::As {
            self.advance();
            Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else { None };
        Ok(Projection::Expr { expr, alias })
    }

    fn parse_table_ref(&mut self) -> PResult<TableRef> {
        let name  = self.expect_ident()?;
        let alias = if self.peek() == &Token::As {
            self.advance();
            Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else { None };
        Ok(TableRef { name, alias })
    }

    fn parse_joins(&mut self) -> PResult<Vec<JoinClause>> {
        let mut joins = Vec::new();
        loop {
            let kind = match self.peek() {
                Token::Join  => { self.advance(); JoinKind::Inner }
                Token::Inner => { self.advance(); self.expect(Token::Join)?; JoinKind::Inner }
                Token::Left  => { self.advance(); self.eat_optional(Token::Inner); self.expect(Token::Join)?; JoinKind::Left }
                Token::Right => { self.advance(); self.eat_optional(Token::Inner); self.expect(Token::Join)?; JoinKind::Right }
                _ => break,
            };
            let table = self.parse_table_ref()?;
            self.expect(Token::On)?;
            let condition = self.parse_expr()?;
            joins.push(JoinClause { kind, table, condition });
        }
        Ok(joins)
    }

    fn parse_order_by_item(&mut self) -> PResult<OrderByItem> {
        let expr = self.parse_expr()?;
        let asc  = match self.peek() {
            Token::Asc  => { self.advance(); true  }
            Token::Desc => { self.advance(); false }
            _           => true,
        };
        Ok(OrderByItem { expr, asc })
    }

    // ── Expression parsing (precedence climbing) ──────────────────────────────

    fn parse_expr(&mut self) -> PResult<Expr> { self.parse_or() }

    fn parse_or(&mut self) -> PResult<Expr> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinOp { op: BinOp::Or, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> PResult<Expr> {
        let mut left = self.parse_not()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_not()?;
            left = Expr::BinOp { op: BinOp::And, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> PResult<Expr> {
        if self.peek() == &Token::Not {
            self.advance();
            let operand = self.parse_not()?;
            return Ok(Expr::UnaryOp { op: UnaryOp::Not, operand: Box::new(operand) });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> PResult<Expr> {
        let left = self.parse_addition()?;
        let op = match self.peek() {
            Token::Eq     => BinOp::Eq,
            Token::NotEq  => BinOp::NotEq,
            Token::Lt     => BinOp::Lt,
            Token::LtEq   => BinOp::LtEq,
            Token::Gt     => BinOp::Gt,
            Token::GtEq   => BinOp::GtEq,
            Token::Is     => {
                self.advance();
                let negated = if self.peek() == &Token::Not { self.advance(); true } else { false };
                self.expect(Token::Null)?;
                return Ok(if negated { Expr::IsNotNull(Box::new(left)) } else { Expr::IsNull(Box::new(left)) });
            }
            Token::Between => {
                self.advance();
                let low  = self.parse_addition()?;
                self.expect(Token::And)?;
                let high = self.parse_addition()?;
                return Ok(Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high) });
            }
            Token::Not => {
                // NOT IN / NOT LIKE / NOT BETWEEN
                self.advance();
                match self.peek() {
                    Token::In => {
                        self.advance();
                        let list = self.parse_in_list()?;
                        return Ok(Expr::UnaryOp {
                            op:      UnaryOp::Not,
                            operand: Box::new(Expr::In { expr: Box::new(left), list }),
                        });
                    }
                    Token::Like => {
                        self.advance();
                        let pattern = self.expect_string()?;
                        return Ok(Expr::Like { expr: Box::new(left), pattern, negated: true });
                    }
                    _ => return Err(QueryError::Parse("expected IN or LIKE after NOT".into())),
                }
            }
            Token::In => {
                self.advance();
                let list = self.parse_in_list()?;
                return Ok(Expr::In { expr: Box::new(left), list });
            }
            Token::Like => {
                self.advance();
                let pattern = self.expect_string()?;
                return Ok(Expr::Like { expr: Box::new(left), pattern, negated: false });
            }
            _ => return Ok(left),
        };
        self.advance();
        let right = self.parse_addition()?;
        Ok(Expr::BinOp { op, left: Box::new(left), right: Box::new(right) })
    }

    fn parse_in_list(&mut self) -> PResult<Vec<Expr>> {
        self.expect(Token::LParen)?;
        let list = self.parse_comma_separated(|p| p.parse_expr())?;
        self.expect(Token::RParen)?;
        Ok(list)
    }

    fn parse_addition(&mut self) -> PResult<Expr> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.peek() {
                Token::Plus  => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _            => break,
            };
            self.advance();
            let right = self.parse_multiplication()?;
            left = Expr::BinOp { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> PResult<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star    => BinOp::Mul,
                Token::Slash   => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _              => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinOp { op, left: Box::new(left), right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> PResult<Expr> {
        if self.peek() == &Token::Minus {
            self.advance();
            let operand = self.parse_unary()?;
            return Ok(Expr::UnaryOp { op: UnaryOp::Neg, operand: Box::new(operand) });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> PResult<Expr> {
        match self.peek().clone() {
            Token::IntLit(n) => { self.advance(); Ok(Expr::Literal(Literal::Int(n))) }
            Token::FloatLit(f) => { self.advance(); Ok(Expr::Literal(Literal::Float(f))) }
            Token::StrLit(s)  => { self.advance(); Ok(Expr::Literal(Literal::Str(s))) }
            Token::True       => { self.advance(); Ok(Expr::Literal(Literal::Bool(true))) }
            Token::False      => { self.advance(); Ok(Expr::Literal(Literal::Bool(false))) }
            Token::Null       => { self.advance(); Ok(Expr::Literal(Literal::Null)) }
            Token::Star       => { self.advance(); Ok(Expr::Star) }
            Token::LParen     => {
                self.advance();
                let e = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(e)
            }
            Token::Interval => {
                self.advance();
                let s = self.expect_string()?;
                let ns = parse_interval(&s)?;
                Ok(Expr::Interval(ns))
            }
            Token::Ident(name) => {
                self.advance();
                // Qualified column: table.column
                if self.peek() == &Token::Dot {
                    self.advance();
                    let col = self.expect_ident()?;
                    return Ok(Expr::Column { table: Some(name), name: col });
                }
                // Function call
                if self.peek() == &Token::LParen {
                    self.advance();
                    let args = if self.peek() == &Token::RParen {
                        vec![]
                    } else {
                        self.parse_comma_separated(|p| p.parse_expr())?
                    };
                    self.expect(Token::RParen)?;
                    return Ok(Expr::Function { name: name.to_ascii_lowercase(), args });
                }
                Ok(Expr::Column { table: None, name })
            }
            other => Err(QueryError::Parse(format!("unexpected token in expression: {other:?}"))),
        }
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    fn peek(&self) -> &Token { &self.tokens[self.pos] }
    fn advance(&mut self) -> &Token {
        let t = &self.tokens[self.pos];
        if self.pos + 1 < self.tokens.len() { self.pos += 1; }
        t
    }
    fn eat_optional(&mut self, tok: Token) -> bool {
        if self.peek() == &tok { self.advance(); true } else { false }
    }
    fn expect(&mut self, tok: Token) -> PResult<()> {
        if self.peek() == &tok { self.advance(); Ok(()) }
        else { Err(QueryError::Parse(format!("expected {tok:?}, got {:?}", self.peek()))) }
    }
    fn expect_ident(&mut self) -> PResult<String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            other           => Err(QueryError::Parse(format!("expected identifier, got {other:?}"))),
        }
    }
    fn expect_int(&mut self) -> PResult<i64> {
        match self.advance().clone() {
            Token::IntLit(n) => Ok(n),
            other            => Err(QueryError::Parse(format!("expected integer, got {other:?}"))),
        }
    }
    fn expect_string(&mut self) -> PResult<String> {
        match self.advance().clone() {
            Token::StrLit(s) => Ok(s),
            other            => Err(QueryError::Parse(format!("expected string, got {other:?}"))),
        }
    }
    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> PResult<Vec<T>>
    where F: FnMut(&mut Self) -> PResult<T>
    {
        let mut items = vec![f(self)?];
        while self.peek() == &Token::Comma { self.advance(); items.push(f(self)?); }
        Ok(items)
    }
}

/// Parse an interval string like "5 minutes", "1h", "30s", "1 day" to nanoseconds.
pub fn parse_interval(s: &str) -> PResult<i64> {
    let s = s.trim();
    let split_pos = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    let num_str = s[..split_pos].trim();
    let unit    = s[split_pos..].trim().to_ascii_lowercase();

    let num: i64 = num_str.parse()
        .map_err(|_| QueryError::Parse(format!("invalid interval number in: '{s}'")))?;

    let ns_per_unit: i64 = match unit.as_str() {
        "us" | "microsecond"  | "microseconds"              => 1_000,
        "ms" | "millisecond"  | "milliseconds" | "millisec" => 1_000_000,
        "s"  | "sec" | "second"  | "seconds"               => 1_000_000_000,
        "m"  | "min" | "minute"  | "minutes"               => 60 * 1_000_000_000,
        "h"  | "hr"  | "hour"    | "hours"                 => 3_600 * 1_000_000_000,
        "d"  | "day" | "days"                               => 86_400 * 1_000_000_000,
        other => return Err(QueryError::Parse(format!("unknown interval unit: {other}"))),
    };
    Ok(num * ns_per_unit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;

    fn parse(sql: &str) -> Statement {
        let tokens = Lexer::new(sql).tokenise().unwrap();
        Parser::new(tokens).parse_statement().unwrap()
    }

    #[test]
    fn parse_simple_select() {
        let stmt = parse("SELECT ts, body FROM logs WHERE severity_number >= 9");
        let Statement::Select(s) = stmt;
        assert_eq!(s.projections.len(), 2);
        assert!(s.filter.is_some());
    }

    #[test]
    fn parse_function_call() {
        let stmt = parse("SELECT count(*) AS cnt FROM logs");
        let Statement::Select(s) = stmt;
        match &s.projections[0] {
            Projection::Expr { expr: Expr::Function { name, .. }, alias } => {
                assert_eq!(name, "count");
                assert_eq!(alias.as_deref(), Some("cnt"));
            }
            _ => panic!("expected function projection"),
        }
    }

    #[test]
    fn parse_between() {
        let stmt = parse("SELECT * FROM traces WHERE duration_ns BETWEEN 1000 AND 9999");
        let Statement::Select(s) = stmt;
        assert!(matches!(s.filter, Some(Expr::Between { .. })));
    }

    #[test]
    fn parse_join() {
        let stmt = parse("SELECT l.body, t.name FROM logs l JOIN traces t ON l.trace_id = t.trace_id");
        let Statement::Select(s) = stmt;
        assert_eq!(s.joins.len(), 1);
        assert_eq!(s.joins[0].kind, JoinKind::Inner);
    }

    #[test]
    fn parse_interval_units() {
        assert_eq!(parse_interval("5 minutes").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(parse_interval("1h").unwrap(),        3_600 * 1_000_000_000);
        assert_eq!(parse_interval("30s").unwrap(),       30 * 1_000_000_000);
    }

    #[test]
    fn parse_group_by_order_by_limit() {
        let stmt = parse(
            "SELECT time_bucket('1m', ts) AS m, count(*) FROM logs \
             GROUP BY m ORDER BY m DESC LIMIT 100"
        );
        let Statement::Select(s) = stmt;
        assert_eq!(s.group_by.len(), 1);
        assert_eq!(s.order_by.len(), 1);
        assert!(!s.order_by[0].asc);
        assert_eq!(s.limit, Some(100));
    }
}
