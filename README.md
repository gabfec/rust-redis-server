# Redis Clone in Rust

[![Tests](https://github.com/gabfec/rust-redis-server/actions/workflows/rust.yml/badge.svg)](https://github.com/gabfec/rust-redis-server/actions)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Made with Rust](https://img.shields.io/badge/Made%20with-Rust-red.svg)]()
[![Rust](https://img.shields.io/badge/rust-1.92+-orange.svg)](https://www.rust-lang.org)

A Redis-compatible server written in Rust, built from scratch on the
standard library alone: no async runtime, no external crates.

It implements:

- RESP protocol parser and encoder
- Thread-per-connection TCP server
- Strings, lists, streams, sorted sets and bitmaps
- Key expiry
- Transactions (`MULTI`/`EXEC`) and optimistic locking (`WATCH`)
- Pub/Sub
- Leader/follower replication
- RDB loading and AOF persistence
- Geospatial indexing (geohash scores, haversine distance)
- ACL users and password authentication (hand-written SHA-256)

Works with the standard `redis-cli`.

---

## Features

### Commands

| Family | Commands |
|--------|----------|
Connection | `PING`, `ECHO`
Strings | `SET` (`EX`, `PX`), `GET`, `INCR`, `STRLEN`
Keys | `KEYS`, `TYPE`
Lists | `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP`
Streams | `XADD`, `XRANGE`, `XREAD` (`BLOCK`)
Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`
Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
Sorted sets | `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`
Bitmaps | `SETBIT`, `GETBIT`, `BITCOUNT`, `BITOP` (`AND`, `OR`)
Geospatial | `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH` (`FROMLONLAT`, `BYRADIUS`)
Authentication | `AUTH`, `ACL WHOAMI`, `ACL GETUSER`, `ACL SETUSER`
Replication | `REPLCONF`, `PSYNC`, `WAIT`
Server | `INFO replication`, `CONFIG GET`

---

### Server options

| Option | Description |
|------|-------------|
`--port <port>` | port to listen on (default `6379`)
`--replicaof "<host> <port>"` | run as a replica of that master
`--dir <dir>` | directory of the RDB file
`--dbfilename <name>` | RDB file to load at startup
`--appendonly yes\|no` | enable AOF persistence (default `no`)
`--appenddirname <dir>` | AOF directory (default `appendonlydir`)
`--appendfilename <name>` | AOF base name (default `appendonly.aof`)
`--appendfsync always\|everysec\|no` | when to fsync the AOF (default `everysec`)

---

## Example usage

Start the server:

```bash
./your_program.sh --port 6379
```

Talk to it with `redis-cli`:

```bash
redis-cli SET greeting hello
redis-cli GET greeting
redis-cli GEOADD places 13.361389 38.115556 Palermo
```

Start a replica:

```bash
./your_program.sh --port 6380 --replicaof "localhost 6379"
```

---

## Project Structure

```
src/
├── main.rs          accept loop, one thread per connection
├── config.rs        command-line options
├── server.rs        state shared by all connections
├── connection.rs    per-connection read/dispatch/reply loop
├── resp/            RESP parser and values
├── store/           keyspace: strings, lists, streams, sorted sets
├── commands/        one file per command, grouped by family
├── replication/     master side and replica handshake
├── pubsub.rs        channel subscriptions
├── rdb.rs           RDB file loading
├── aof.rs           append-only file persistence
├── geo.rs           geohash encoding and haversine distance
├── acl.rs           users, passwords and logins
└── sha256.rs        SHA-256 for password hashing
```

---

## Tests

Run the unit tests:

```bash
cargo test
```

## Educational Purpose

This project was built as part of the CodeCrafters
["Build Your Own Redis"](https://codecrafters.io/challenges/redis)
challenge, which focuses on implementing real-world systems from scratch.

The goal was to understand how Redis works internally, including the wire
protocol, blocking commands, replication handshakes, persistence formats
and geohash encoding, without relying on external libraries.

https://codecrafters.io

---

## License

MIT License — see the `LICENSE` file.
