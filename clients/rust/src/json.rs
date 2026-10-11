//! A small, strict JSON parser (RFC 8259) — enough for DistKV's API without
//! pulling in dependencies.

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
    Arr(Vec<Value>),
    Obj(Vec<(String, Value)>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub pos: usize,
    pub msg: &'static str,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "json: {} at byte {}", self.msg, self.pos)
    }
}

impl std::error::Error for Error {}

impl Value {
    pub fn get(&self, key: &str) -> &Value {
        match self {
            Value::Obj(kv) => kv.iter().find(|(k, _)| k == key).map(|(_, v)| v).unwrap_or(&Value::Null),
            _ => &Value::Null,
        }
    }
    pub fn as_f64(&self) -> f64 {
        if let Value::Num(n) = self { *n } else { 0.0 }
    }
    pub fn as_u64(&self) -> u64 {
        self.as_f64().max(0.0) as u64
    }
    pub fn as_str(&self) -> &str {
        if let Value::Str(s) = self { s } else { "" }
    }
    pub fn as_bool(&self) -> bool {
        matches!(self, Value::Bool(true))
    }
    pub fn as_array(&self) -> &[Value] {
        if let Value::Arr(a) = self { a } else { &[] }
    }
    pub fn as_object(&self) -> &[(String, Value)] {
        if let Value::Obj(o) = self { o } else { &[] }
    }
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

pub fn parse(s: &str) -> Result<Value, Error> {
    let mut p = Parser { b: s.as_bytes(), i: 0, depth: 0 };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.b.len() {
        return Err(p.err("trailing characters"));
    }
    Ok(v)
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn err(&self, msg: &'static str) -> Error {
        Error { pos: self.i, msg }
    }
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }
    fn expect(&mut self, lit: &[u8]) -> Result<(), Error> {
        if self.b[self.i..].starts_with(lit) {
            self.i += lit.len();
            Ok(())
        } else {
            Err(self.err("invalid literal"))
        }
    }
    fn value(&mut self) -> Result<Value, Error> {
        self.depth += 1;
        if self.depth > 256 {
            return Err(self.err("nesting too deep"));
        }
        let v = match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => self.string().map(Value::Str),
            Some(b't') => self.expect(b"true").map(|_| Value::Bool(true)),
            Some(b'f') => self.expect(b"false").map(|_| Value::Bool(false)),
            Some(b'n') => self.expect(b"null").map(|_| Value::Null),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            _ => Err(self.err("unexpected character")),
        };
        self.depth -= 1;
        v
    }
    fn object(&mut self) -> Result<Value, Error> {
        self.i += 1;
        let mut out = Vec::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(Value::Obj(out));
        }
        loop {
            self.ws();
            if self.peek() != Some(b'"') {
                return Err(self.err("expected key"));
            }
            let k = self.string()?;
            self.ws();
            if self.peek() != Some(b':') {
                return Err(self.err("expected ':'"));
            }
            self.i += 1;
            self.ws();
            let v = self.value()?;
            out.push((k, v));
            self.ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b'}') => {
                    self.i += 1;
                    return Ok(Value::Obj(out));
                }
                _ => return Err(self.err("expected ',' or '}'")),
            }
        }
    }
    fn array(&mut self) -> Result<Value, Error> {
        self.i += 1;
        let mut out = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(Value::Arr(out));
        }
        loop {
            self.ws();
            out.push(self.value()?);
            self.ws();
            match self.peek() {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    return Ok(Value::Arr(out));
                }
                _ => return Err(self.err("expected ',' or ']'")),
            }
        }
    }
    fn hex4(&mut self) -> Result<u32, Error> {
        let s = self.b.get(self.i..self.i + 4).ok_or_else(|| self.err("short \\u escape"))?;
        let s = std::str::from_utf8(s).map_err(|_| self.err("bad \\u escape"))?;
        let v = u32::from_str_radix(s, 16).map_err(|_| self.err("bad \\u escape"))?;
        self.i += 4;
        Ok(v)
    }
    fn string(&mut self) -> Result<String, Error> {
        self.i += 1;
        let mut out = String::new();
        loop {
            let start = self.i;
            while self.i < self.b.len() && self.b[self.i] != b'"' && self.b[self.i] != b'\\' && self.b[self.i] >= 0x20 {
                self.i += 1;
            }
            out.push_str(std::str::from_utf8(&self.b[start..self.i]).map_err(|_| self.err("invalid utf-8"))?);
            match self.peek() {
                Some(b'"') => {
                    self.i += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.i += 1;
                    let c = self.peek().ok_or_else(|| self.err("unterminated escape"))?;
                    self.i += 1;
                    match c {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let mut cp = self.hex4()?;
                            if (0xD800..0xDC00).contains(&cp) {
                                if !self.b[self.i..].starts_with(b"\\u") {
                                    return Err(self.err("lone surrogate"));
                                }
                                self.i += 2;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(self.err("bad surrogate pair"));
                                }
                                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                            }
                            out.push(char::from_u32(cp).ok_or_else(|| self.err("invalid code point"))?);
                        }
                        _ => return Err(self.err("invalid escape")),
                    }
                }
                _ => return Err(self.err("unterminated string")),
            }
        }
    }
    fn number(&mut self) -> Result<Value, Error> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E' | b'+' | b'-') {
                self.i += 1;
            } else {
                break;
            }
        }
        let s = std::str::from_utf8(&self.b[start..self.i]).unwrap();
        s.parse::<f64>().map(Value::Num).map_err(|_| Error { pos: start, msg: "invalid number" })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_documents() {
        let v = parse(r#"{"a": [1, -2.5e3, true, null], "s": "x\"y\u00e9\ud83d\ude80", "o": {}}"#).unwrap();
        assert_eq!(v.get("a").as_array().len(), 4);
        assert_eq!(v.get("a").as_array()[1].as_f64(), -2500.0);
        assert_eq!(v.get("s").as_str(), "x\"yé🚀");
        assert!(v.get("missing").is_null());
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", "{", "[1,]", "{\"a\" 1}", "tru", "\"unterminated", "1 2", "\"\\ud800\""] {
            assert!(parse(bad).is_err(), "{bad:?} should fail");
        }
    }
}
