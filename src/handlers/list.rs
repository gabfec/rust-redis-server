use std::io::Result as IoResult;
use std::io::Write;
use std::net::TcpStream;
use std::time::Instant;

use crate::db::{Cv, Db, Entry, RedisValue};
use crate::resp::Resp;

use super::RedisWrite;

pub fn handle_rpush(
    stream: &mut TcpStream,
    db: &Db,
    cv: &Cv,
    key: String,
    values: Vec<String>,
) -> IoResult<()> {
    let mut map = db.lock().unwrap();

    let entry = map.entry(key).or_insert(Entry {
        value: RedisValue::List(Vec::new()),
        created_at: Instant::now(),
        expires_in: None,
    });

    if let RedisValue::List(ref mut list) = entry.value {
        for val in values {
            list.push(val);
        }
        // RESP Integer format: ":<number>\r\n"
        stream.write_resp(Resp::integer(list.len()))?;

        cv.notify_all(); // Wake up any BLPOP waiters
        Ok(())
    } else {
        // Technically Redis returns an error if you RPUSH to a key
        // that already holds a String, but for now, we can just return an error.
        stream.write_resp(Resp::error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }
}

pub fn handle_lpush(
    stream: &mut TcpStream,
    db: &Db,
    cv: &Cv,
    key: String,
    values: Vec<String>,
) -> IoResult<()> {
    let mut map = db.lock().unwrap();

    let entry = map.entry(key).or_insert(Entry {
        value: RedisValue::List(Vec::new()),
        created_at: Instant::now(),
        expires_in: None,
    });

    if let RedisValue::List(ref mut list) = entry.value {
        for val in values {
            list.insert(0, val);
        }
        stream.write_resp(Resp::integer(list.len()))?;

        cv.notify_all(); // Wake up any BLPOP waiters
        Ok(())
    } else {
        stream.write_resp(Resp::error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }
}

pub fn handle_lrange(
    stream: &mut TcpStream,
    db: &Db,
    key: String,
    start: i64,
    stop: i64,
) -> IoResult<()> {
    let db_lock = db.lock().unwrap();

    match db_lock.get(&key) {
        Some(entry) => {
            if let RedisValue::List(ref list) = entry.value {
                let len = list.len() as i64;

                // Normalize and clamp in one step per variable
                let start_idx = (if start < 0 { len + start } else { start })
                    .clamp(0, len)
                    as usize;
                let stop_idx = (if stop < 0 { len + stop } else { stop })
                    .clamp(0, len - 1)
                    as usize;

                if start_idx >= list.len() || start_idx > stop_idx {
                    stream.write_resp(Resp::array(0))
                } else {
                    let elements = &list[start_idx..=stop_idx];

                    // Encode as RESP Array: *<count>\r\n
                    let mut response = Resp::array(elements.len());
                    for el in elements {
                        response.push_str(&Resp::bulk_string(el));
                    }
                    stream.write_resp(response)
                }
            } else {
                // If the key is a String, Redis returns an error
                stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))
            }
        }
        None => {
            // If list doesn't exist, return empty array
            stream.write_resp(Resp::array(0))
        }
    }
}

pub fn handle_llen(stream: &mut TcpStream, db: &Db, key: String) -> IoResult<()> {
    let db_lock = db.lock().unwrap();

    match db_lock.get(&key) {
        Some(entry) => {
            if let RedisValue::List(ref list) = entry.value {
                stream.write_resp(Resp::integer(list.len()))
            } else {
                stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))
            }
        }
        None => {
            // Redis returns 0 for non-existent keys
            stream.write_resp(Resp::integer(0))
        }
    }
}

pub fn handle_lpop(
    stream: &mut TcpStream,
    db: &Db,
    key: String,
    count: Option<usize>,
) -> IoResult<()> {
    let mut db_lock = db.lock().unwrap();

    match db_lock.get_mut(&key) {
        Some(entry) => {
            if let RedisValue::List(ref mut list) = entry.value {
                match count {
                    None => {
                        // LPOP without count
                        if list.is_empty() {
                            // List exists but is empty
                            stream.write_resp(Resp::null_bulk())
                        } else {
                            // Remove the first element
                            let val = list.remove(0);
                            stream.write_resp(Resp::bulk_string(&val))
                        }
                    }
                    Some(num) => {
                        // LPOP with count
                        let take_count = std::cmp::min(num, list.len());
                        // Remove the first 'n' elements from the vector
                        let popped_elements: Vec<String> =
                            list.drain(0..take_count).collect();

                        let mut response = Resp::array(popped_elements.len());
                        for el in popped_elements {
                            response.push_str(&Resp::bulk_string(&el));
                        }
                        stream.write_resp(response)
                    }
                }
            } else {
                stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))
            }
        }
        None => {
            stream.write_resp(Resp::null_bulk())
        }
    }
}

pub fn handle_blpop(
    stream: &mut TcpStream,
    db: &Db,
    cv: &Cv,
    keys: Vec<String>,
    timeout: f64,
) -> IoResult<()> {
    let mut map = db.lock().unwrap();

    let timeout_duration = std::time::Duration::from_secs_f64(timeout);
    let start_time = std::time::Instant::now();

    loop {
        // Try to find a non-empty list
        for key in &keys {
            if let Some(Entry {
                            value: RedisValue::List(list),
                            ..
                        }) = map.get_mut(key)
            {
                if !list.is_empty() {
                    let val = list.remove(0);
                    // BLPOP returns a 2-element array: [key, value]
                    let mut response = Resp::array(2);
                    response.push_str(&Resp::bulk_string(key));
                    response.push_str(&Resp::bulk_string(&val));

                    stream.write_resp(response)?;
                    return Ok(());
                }
            }
        }

        // Check if we already timed out
        let elapsed = start_time.elapsed();
        if timeout > 0.0 && elapsed >= timeout_duration {
            stream.write_resp(Resp::null_array())?; // Redis returns Null Bulk String on timeout
            return Ok(());
        }

        // Wait to be notified or for timeout
        if timeout == 0.0 {
            map = cv.wait(map).unwrap();
        } else {
            let remaining = timeout_duration - elapsed;
            let (new_map, _) = cv.wait_timeout(map, remaining).unwrap();
            map = new_map;
        }
    }
}
