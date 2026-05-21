use std::collections::HashMap;
use std::io::Result as IoResult;
use std::io::Write;
use std::net::TcpStream;
use std::time::{Instant, SystemTime};

use crate::db::{Db, Entry, RedisValue, StreamEntry};
use crate::resp::Resp;

use super::RedisWrite;

pub fn handle_xadd(
    stream: &mut TcpStream,
    db: &Db,
    key: String,
    id: String,
    fields: HashMap<String, String>,
) -> IoResult<()> {
    let mut db_lock = db.lock().unwrap();

    let entry = db_lock.entry(key).or_insert(Entry {
        value: RedisValue::Stream(Vec::new()),
        created_at: Instant::now(),
        expires_in: None,
    });

    if let RedisValue::Stream(ref mut entries) = entry.value {
        let last_entry = entries.last();

        let (ms, seq) = if id == "*" {
            let now_ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            match last_entry {
                Some(last) if last.id_ms == now_ms => (now_ms, last.id_seq + 1),
                _ => (now_ms, 0),
            }
        } else {
            // Handle "ms-seq" or "ms-*"
            let parts: Vec<&str> = id.split('-').collect();
            let ms = parts[0].parse::<u64>().unwrap_or(0);
            let seq_str = parts.get(1).cloned().unwrap_or("");

            let seq = if seq_str == "*" {
                match last_entry {
                    Some(last) if last.id_ms == ms => last.id_seq + 1,
                    _ if ms == 0 => 1,
                    _ => 0,
                }
            } else {
                seq_str.parse::<u64>().unwrap_or(0)
            };
            (ms, seq)
        };

        let mut is_valid = true;

        // Validate against "0-0"
        if id != "*" && ms == 0 && seq == 0 {
            stream.write_resp(Resp::error("ERR The ID specified in XADD must be greater than 0-0"))?;
            is_valid = false;
        }
        else if let Some(last) = last_entry {
            // Must be strictly greater than the top item
            let is_greater = ms > last.id_ms || (ms == last.id_ms && seq > last.id_seq);
            if !is_greater {
                stream.write_resp(Resp::error("ERR The ID specified in XADD is equal or smaller than the target stream top item"))?;
                is_valid = false;
            }
        }

        if is_valid {
            entries.push(StreamEntry { id_ms: ms, id_seq: seq, fields });

            let response_id = format!("{}-{}", ms, seq);
            stream.write_resp(Resp::bulk_string(&response_id))?;
        }
    } else {
        stream.write_resp(Resp::error("WRONGTYPE Operation against a key holding the wrong kind of value"))?;
    }

    Ok(())
}

pub fn handle_xrange(
    stream: &mut TcpStream,
    db: &Db,
    key: String,
    start: String,
    end: String,
) -> IoResult<()> {
    let db_lock = db.lock().unwrap();

    // Helper to parse inclusive range boundaries
    let parse_boundary = |s: &str, is_start: bool| -> (u64, u64) {
        let parts: Vec<&str> = s.split('-').collect();
        let ms = parts[0].parse::<u64>().unwrap_or(0);
        let seq = if parts.len() > 1 {
            parts[1].parse::<u64>().unwrap_or(0)
        } else if is_start {
            0
        } else {
            u64::MAX
        };
        (ms, seq)
    };

    let (start_ms, start_seq) = parse_boundary(&start, true);
    let (end_ms, end_seq) = parse_boundary(&end, false);

    if let Some(Entry { value: RedisValue::Stream(entries), .. }) = db_lock.get(&key) {
        // Filter entries based on the range
        let filtered: Vec<&StreamEntry> = entries
            .iter()
            .filter(|e| {
                let after_start = e.id_ms > start_ms || (e.id_ms == start_ms && e.id_seq >= start_seq);
                let before_end = e.id_ms < end_ms || (e.id_ms == end_ms && e.id_seq <= end_seq);
                after_start && before_end
            })
            .collect();

        // Construct the nested RESP array
        let mut response = Resp::array(filtered.len());
        for entry in filtered {
            // Each entry is a 2-element array: [ID, [fields]]
            response.push_str(&Resp::array(2));
            response.push_str(&Resp::bulk_string(&format!("{}-{}", entry.id_ms, entry.id_seq)));

            // Fields are an array of strings: [key1, val1, key2, val2...]
            response.push_str(&Resp::array(entry.fields.len() * 2));
            for (f, v) in &entry.fields {
                response.push_str(&Resp::bulk_string(f));
                response.push_str(&Resp::bulk_string(v));
            }
        }
        stream.write_resp(response)
    } else {
        stream.write_resp(Resp::array(0))
    }
}

pub fn handle_xread(
    stream: &mut TcpStream,
    db: &Db,
    keys: Vec<String>,
    mut ids: Vec<String>,
    block_timeout: Option<u64>,
) -> IoResult<()> {
    let start_time = std::time::Instant::now();

    // Distinguish between no block, blocking with a timeout, and blocking indefinitely (0)
    let is_blocking = block_timeout.is_some();
    let is_indefinite = block_timeout == Some(0);
    let timeout = block_timeout.map(std::time::Duration::from_millis);

    // Resolve '$' IDs before entering the polling loop
    {
        let db_lock = db.lock().unwrap();
        for (i, key) in keys.iter().enumerate() {
            if ids[i] == "$" {
                if let Some(Entry { value: RedisValue::Stream(entries), .. }) = db_lock.get(key) {
                    if let Some(last) = entries.last() {
                        // Replace '$' with the actual highest ID currently in the stream
                        ids[i] = format!("{}-{}", last.id_ms, last.id_seq);
                    } else {
                        // Stream exists but is empty
                        ids[i] = "0-0".to_string();
                    }
                } else {
                    // Stream doesn't exist yet
                    ids[i] = "0-0".to_string();
                }
            }
        }
    }

    let mut final_response = String::new();
    let mut data_found = false;

    loop {
        let db_lock = db.lock().unwrap();
        let mut response_array = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            // ids[i] now contains either the explicit ID or the resolved '$' ID
            let start_id_str = &ids[i];

            // Parse the boundary ID
            let parts: Vec<&str> = start_id_str.split('-').collect();
            let start_ms = parts[0].parse::<u64>().unwrap_or(0);
            let start_seq = parts.get(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);

            if let Some(Entry { value: RedisValue::Stream(entries), .. }) = db_lock.get(key) {
                // Filter: Strictly GREATER than start_id
                let filtered: Vec<&StreamEntry> = entries
                    .iter()
                    .filter(|e| {
                        e.id_ms > start_ms || (e.id_ms == start_ms && e.id_seq > start_seq)
                    })
                    .collect();

                if !filtered.is_empty() {
                    // Stream result: [key, [entries]]
                    let mut stream_res = Resp::array(2);
                    stream_res.push_str(&Resp::bulk_string(key));

                    // Entries array
                    let mut entries_res = Resp::array(filtered.len());
                    for entry in filtered {
                        let mut entry_data = Resp::array(2);
                        entry_data.push_str(&Resp::bulk_string(&format!("{}-{}", entry.id_ms, entry.id_seq)));

                        let mut fields_res = Resp::array(entry.fields.len() * 2);
                        for (f, v) in &entry.fields {
                            fields_res.push_str(&Resp::bulk_string(f));
                            fields_res.push_str(&Resp::bulk_string(v));
                        }
                        entry_data.push_str(&fields_res);
                        entries_res.push_str(&entry_data);
                    }
                    stream_res.push_str(&entries_res);
                    response_array.push(stream_res);
                }
            }
        }

        // Check if we captured anything across any of the requested streams
        if !response_array.is_empty() {
            // Wrap the collected streams into the final RESP array
            let mut final_resp = Resp::array(response_array.len());
            for s in response_array {
                final_resp.push_str(&s);
            }
            final_response = final_resp;
            data_found = true;
            break; // Break loop to send data back
        }

        // Drop the db_lock explicitly so other incoming connection threads (like XADD) can mutate the DB
        drop(db_lock);

        // If no data was found, decide if we should block or time out
        if is_blocking {
            if !is_indefinite {
                if let Some(t) = timeout {
                    if start_time.elapsed() >= t {
                        // Timeout expired, break out to return Null Array
                        break;
                    }
                }
            }
            // Sleep briefly to yield execution to other threads
            std::thread::sleep(std::time::Duration::from_millis(50));
        } else {
            // No block specified, immediately break out
            break;
        }
    }

    // Send the resulting payload back over the TCP stream
    if data_found {
        stream.write_resp(final_response)?;
    } else {
        stream.write_resp(Resp::null_array().to_string())?;
    }

    Ok(())
}
