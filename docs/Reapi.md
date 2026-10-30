# REAPI (experimental)

sccache built with the `reapi` feature can use a REAPI v2.1 server for Rust compiles, set in `[dist.reapi]` (see [Configuration](Configuration.md)).

sccache connects to the server and hashes each toolchain's files once while it runs. The digests are kept in `<dist cache_dir>/reapi-digests.json`, so after a restart only changed files are hashed again. Compiles don't use the server yet: they run locally with the arguments a REAPI worker uses, with a copy of the Rust sysroot under `<dist cache_dir>/reapi-sysroot-copies`, which is never removed. `sccache --show-stats` lists, by reason, the compiles that couldn't use the server.

A failed connection is retried after 5 to 60 seconds, and a failed hashing of a toolchain after a minute. A server older than REAPI v2.1, or a server that refuses the token, isn't used until sccache restarts. A toolchain without rustc or std isn't used until it changes. A rustc whose `rustc -vV` prints `commit-hash: unknown` always compiles locally.
