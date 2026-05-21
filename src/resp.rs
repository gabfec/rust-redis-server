pub struct Resp;

impl Resp {
    pub fn string(s: &str) -> String {
        format!("+{}\r\n", s)
    }

    pub fn error(msg: &str) -> String {
        format!("-{}\r\n", msg)
    }

    pub fn integer(i: usize) -> String {
        format!(":{}\r\n", i)
    }

    pub fn bulk_string(s: &str) -> String {
        format!("${}\r\n{}\r\n", s.len(), s)
    }

    pub fn null_bulk() -> &'static str {
        "$-1\r\n"
    }

    pub fn null_array() -> &'static str {
        "*-1\r\n"
    }

    pub fn array(len: usize) -> String {
        format!("*{}\r\n", len)
    }
}
