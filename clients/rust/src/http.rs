//! Minimal HTTP/1.1 client over `std::net::TcpStream` (plain `http://`
//! only), supporting Content-Length and chunked responses.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Splits `http://host:port/path` into (host:port, path).
pub fn split_url(url: &str) -> Result<(String, String), String> {
    let rest = url.strip_prefix("http://").ok_or_else(|| format!("only http:// URLs are supported: {url}"))?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let hostport = if hostport.contains(':') { hostport.to_string() } else { format!("{hostport}:80") };
    Ok((hostport, path.to_string()))
}

pub fn request(method: &str, url: &str, headers: &[(&str, String)], body: Option<&[u8]>, timeout: Duration) -> Result<Response, String> {
    let (hostport, path) = split_url(url)?;
    let addr = hostport.to_socket_addrs().map_err(|e| e.to_string())?.next().ok_or("unresolvable host")?;
    let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(|e| e.to_string())?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    stream.set_nodelay(true).ok();

    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {hostport}\r\nConnection: close\r\nUser-Agent: distkv-rust/0.2\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    let body = body.unwrap_or(&[]);
    if method != "GET" || !body.is_empty() {
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;

    let mut r = BufReader::new(stream);
    let mut line = String::new();
    r.read_line(&mut line).map_err(|e| e.to_string())?;
    let status: u16 = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).ok_or_else(|| format!("bad status line {line:?}"))?;
    let mut headers = Vec::new();
    loop {
        line.clear();
        r.read_line(&mut line).map_err(|e| e.to_string())?;
        let l = line.trim_end();
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let mut resp = Response { status, headers, body: Vec::new() };
    if resp.header("Transfer-Encoding").map(|v| v.eq_ignore_ascii_case("chunked")).unwrap_or(false) {
        loop {
            line.clear();
            r.read_line(&mut line).map_err(|e| e.to_string())?;
            let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or("0"), 16).map_err(|_| "bad chunk size")?;
            if size == 0 {
                break;
            }
            let mut chunk = vec![0; size];
            r.read_exact(&mut chunk).map_err(|e| e.to_string())?;
            resp.body.extend_from_slice(&chunk);
            let mut crlf = [0; 2];
            r.read_exact(&mut crlf).map_err(|e| e.to_string())?;
        }
    } else if let Some(n) = resp.header("Content-Length").and_then(|v| v.parse::<usize>().ok()) {
        resp.body = vec![0; n];
        r.read_exact(&mut resp.body).map_err(|e| e.to_string())?;
    } else {
        r.read_to_end(&mut resp.body).map_err(|e| e.to_string())?;
    }
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_urls() {
        assert_eq!(split_url("http://h:1/x?y").unwrap(), ("h:1".into(), "/x?y".into()));
        assert_eq!(split_url("http://h").unwrap(), ("h:80".into(), "/".into()));
        assert!(split_url("https://h").is_err());
    }
}
