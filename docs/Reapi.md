# REAPI (experimental)

sccache can use a [Remote Execution API](https://github.com/bazelbuild/remote-apis) (REAPI) v2.1 server, such as Buildbarn or NativeLink, for Rust compiles and for the C and C++ compiles of cargo build scripts. A compile that misses sccache's cache is looked up in the server's action cache, and on a miss it runs on the server's workers, so that every client gets its result.

## Build

The `reapi` cargo feature isn't in the default features or in release binaries:

```sh
cargo install sccache --features reapi
```

## Configuration

`[dist.reapi]` in the client config turns it on, and turns sccache-dist off (see [Configuration](Configuration.md) for every key). A minimal config:

```toml
[dist.reapi]
endpoint = "grpcs://remote.example.com:443"
auth_token_file = "/home/user/.config/sccache/reapi-token"

[dist.reapi.platform]
OSFamily = "linux"
```

The `platform` properties pick the workers, and the server's documentation lists the ones it matches on. Each action carries them and the `salt`, so clients with a different salt or platform don't share action cache entries. In a pool with workers of more than one CPU architecture, add the property that tells them apart, such as `Arch` or `ISA`: otherwise an arm64 client's actions can land on amd64 workers, fail there, and then compile locally at every miss.

## What runs through REAPI

An rlib compile runs through REAPI when:

- it's run by rustc, not by a driver such as clippy-driver, and its `rustc -vV` prints a commit hash;
- the client doesn't run Windows;
- its sources and dependencies are under the cwd or cargo's target dir, laid out as cargo lays them out;
- it has no `-C target-cpu=native`, `-Z tune-cpu=native`, PGO profile, `--remap-path-prefix`, custom target spec, or absolute path in a `-C` or `-Z` option other than the linker's;
- its env vars are UTF-8, and none looks like a secret: a name with `TOKEN`, `SECRET`, `KEY` and the like, or a URL with credentials;
- `SCCACHE_RECACHE` and `SCCACHE_NO_CACHE` are unset. With either, it compiles locally, without a lookup or an execution.

A C or C++ compile that a cargo build script runs, as the `cc` crate does, runs through REAPI when:

- it's run by gcc or clang on a Linux client;
- gcc finds its `cc1` (`cc1plus` for C++) and `as`, and clang reads no config file;
- its cwd is the package's `CARGO_MANIFEST_DIR`, its `OUT_DIR` is in cargo's target dir, and its source and object are under one of the two;
- it compiles one C or C++ source into an object, with no `-x` after the source, and writes nothing else besides a depfile;
- it has none of the options a worker's compile can't match: `-march=native` and the like, `-arch`, a precompiled header, profiling or coverage, macro debug info such as `-g3`, its own `-f*-prefix-map`, an option that reads a file besides the source, such as `-fauto-profile`, an option that writes a file besides the object, such as `-fstack-usage`, `-save-temps` or `-fdump-*`, an option passed to gcc's internal programs, verbose output, an option that records the command line, or an absolute path the mapping can't place;
- its arguments are UTF-8, and no env var changes which programs gcc runs, such as `GCC_EXEC_PREFIX` or `COMPILER_PATH`;
- `SCCACHE_RECACHE` and `SCCACHE_NO_CACHE` are unset.

The client preprocesses the source again, with its paths relative to the package or the target dir, and the worker compiles that, so it needs no headers. The driver, the programs gcc runs (`cc1`, `cc1plus`, `as`) and their shared libraries, without glibc's, go to the workers as the client has them, so workers need the client's CPU architecture and a glibc at least as new as the client's. The `cc` crate archives the objects with the local `ar`; an `ar` that writes timestamps into the archive makes each build's archive differ, which costs the hits of the `-sys` crate's rlib that bundles it.

Other compiles run locally. `sccache --show-stats` counts the action cache hits and the executions, and lists, by reason, the compiles that ran locally.

## Lookups and execution

sccache connects to the server and hashes each toolchain's files once while it runs. The digests are kept in `<dist cache_dir>/reapi-digests.json`, so after a restart only changed files are hashed again. On a hit, sccache writes the compile's outputs and caches them as it would a local compile's.

On a miss, with `on_miss = "execute"` (the default), sccache uploads the compile's inputs and toolchain to the server's CAS and runs it on a worker, and the server caches the result. Execution needs a server that executes actions, and a Linux client or a Linux toolchain for the client's rustc (see below). With `on_miss = "local"`, a miss compiles locally and sccache never writes to the server.

Before a toolchain's first execution after sccache starts, sccache runs its `rustc -vV` on a worker. If the workers can't run it, for example on another CPU architecture, or it reports another commit hash than the local rustc, that toolchain's misses compile locally until sccache restarts, and its lookups continue. A check that fails to reach a worker runs again at the next miss. For gcc and clang, the check compares the driver's target, its version, and where it finds the programs it runs, with the local driver's answers, so a worker that runs its own `cc1` or `as` fails the check, and it compiles a line of preprocessed source, so a `cc1` or `as` that doesn't run on the workers fails it too. A worker root with a `:` or `=`, which the paths the compile gets can't hold, fails it as well.

A compile that can't run through REAPI, or fails on a worker, runs locally with the arguments a worker uses, so that its outputs match a worker's. It uses a copy of the Rust sysroot under `<dist cache_dir>/reapi-sysroot-copies`, which is never removed; a crash while making a copy can leave a `<name>.XXXX` directory there too. A C or C++ compile that could run through REAPI and runs locally compiles its preprocessed source as a worker does, in a temp dir under `<dist cache_dir>/reapi-preprocessed`, so that its object matches a worker's; if that fails, the compile's own command runs. A compile that fails on a worker runs on a worker again, then locally, at every build. cargo's `--diagnostic-width` isn't sent, so that clients with different terminals share results; a replayed warning wraps at the width of the terminal that ran it first.

## Clients that don't run Linux

A client that doesn't run Linux, such as a Mac, takes the toolchain its workers run from a `[[dist.toolchains]]` entry of `type = "path_override"`, as sccache-dist does (see [DistributedQuickstart.md](DistributedQuickstart.md)):

- `compiler_executable` is the client's `<sysroot>/bin/rustc`, where `rustc --print sysroot` prints the sysroot;
- `archive` is a tar.gz of a Linux toolchain of the same rustc version, with the client's target added, such as `aarch64-apple-darwin`;
- `archive_compiler_executable` is its rustc, which must be a `<sysroot>/bin/rustc` path in the archive.

Other entry types are ignored, and of two entries for the same rustc, the first is used. sccache unpacks that sysroot, and nothing else, under `<dist cache_dir>/reapi-worker-toolchains` the first time a compile needs it, and again when the archive file changes, removing the earlier copy; compiles don't wait for that past `ready_wait_secs`. An archive with a symlink that resolves out of the sysroot, or nowhere, isn't used, and one that fails to unpack is tried again after a minute.

With it, that rustc's lookups and executions use the Linux toolchain, unless the archive's std for the compile's target has other file names or sizes than the local one, which means another rustc version: then those compiles run locally. A compile that loads a proc macro runs locally too, since a proc macro is a library for the client's OS. Without an entry, the client's lookups use its own toolchain, and its misses compile locally.

C and C++ compiles of such a client always run locally.

## Slots

`local_slots` is the most compiles sccache runs locally at once, C and C++ ones included, and also while the server can't be reached. A miss that could execute compiles locally while fewer run, so it never reaches the server's action cache: for other clients to get hits, keep `local_slots` below cargo's `-j`. A miss that finds no free local slot executes once one of `remote_slots` is free, or compiles locally if a local slot frees first, or after `queue_timeout_secs` without a free slot. Without `local_slots`, a miss executes whenever it can, and local compiles run as cargo starts them. Without `remote_slots`, executions aren't limited.

## Client-side mode

In [client-side mode](Architecture.md#client-side-mode-sccache_client_side), each sccache CLI maps its compile for REAPI, and the sccache daemon looks it up or executes it with its own connection to the server, so the slots and the pauses after failures cover every CLI. A CLI sends compiles to the daemon only with `[dist.reapi]` in its own config, and compiles locally if the daemon can't answer.

## Credentials

A `grpcs://` server's certificate must chain to one of the certificates in `tls_ca_file`, or, without it, to one of the system's root certificates. A server that asks for a client certificate gets the one in `tls_client_certificate_file`. With `grpc://`, the credentials and everything else travel in plain text.

Each call carries the token in `auth_token_file` and the headers in `[dist.reapi.headers]`, whose values sccache leaves out of logs and errors. A syntax error in the config file can still print the line it's on, so keep secrets in `auth_token_file` or a credential helper rather than in `[dist.reapi.headers]`. sccache reads `auth_token_file` again every 10 seconds at most, and uses the token there if it changed, so a job can replace a short-lived token. The job should write the new token to another file and rename it over `auth_token_file`, so that sccache never reads half a token.

Instead of `auth_token_file`, `credential_helper` can name a [Bazel credential helper](https://github.com/EngFlow/credential-helper-spec), as an absolute path or a name on the sccache server's `PATH`. sccache runs it with `get` and `{"uri": "https://<host>[:<port>]"}` on its stdin before it connects. That is the URI Bazel sends for the same endpoint, without port 443, and it starts with `https` for a `grpc://` endpoint too. sccache sends the headers the helper prints with each call, in place of configured headers of the same name. It runs the helper again:

- a minute before its `expires`, but not within 10 seconds;
- 30 minutes later, without an `expires`;
- after the server refuses its headers.

A helper that fails or takes more than 10 seconds when sccache has connected leaves the headers it printed before in use, and runs again 10 seconds later at the earliest. When it fails as sccache connects, compiles run locally, and sccache tries again as after a failed connection. Its stderr is discarded, so run `<helper> get` by hand to see why it fails.

Each call carries a `RequestMetadata` with sccache's name and version and an ID for the sccache server's run, and lookups and executions also carry the action's digest, so the server's logs can tell sccache's calls apart.

## Failures and retries

- An action cache lookup, an upload, or the start of a download that the server answers within a second with UNAVAILABLE or RESOURCE_EXHAUSTED is tried twice more, after 200 and 400 ms.
- A failed connection is retried after 5 to 60 seconds, and a failed hashing of a toolchain after a minute.
- After 3 action cache lookups fail in a row, compiles skip the lookup for 5 seconds, doubling up to 60 while lookups keep failing. Executions pause the same way, and misses compile locally meanwhile.
- A miss that the server reports queued for longer than `queue_timeout_secs` (60 by default) compiles locally, and counts as a failed execution toward the pause.
- Each action has a 9-minute timeout. An action that runs out of time compiles locally and doesn't count toward the pause, and sccache stops waiting for an execution after 10 minutes.
- A server older than REAPI v2.1 isn't used until sccache restarts.
- A server that refuses the credentials when sccache connects isn't used until sccache restarts or the credentials change: a new auth token, or new headers from the credential helper. A server that refuses them later, with UNAUTHENTICATED, or with PERMISSION_DENIED to a lookup or a download, is connected to again, and if it refuses them again, the same applies.
- A server that answers PERMISSION_DENIED to an upload or an execution, as for credentials that may only read, is used only for lookups until sccache restarts.
- A toolchain without rustc or std isn't used until it changes. A rustc whose `rustc -vV` prints `commit-hash: unknown` always compiles locally.

## Limits

- sccache downloads a result's stdout or stderr up to 8 MiB, an output file up to 4 GiB, and a server's answer with a result up to 32 MiB. A compile whose result is larger runs locally.
- The compile's stdout and stderr from the server lose any terminal control sequence except colors, so a server can't set the terminal's title or clipboard, or rewrite what it shows. Worker paths in them are replaced with local ones, but a local path with a `"` or `\` isn't escaped again in JSON diagnostics.
- A C or C++ compile run through REAPI names its sources relative to the package or the target dir in its warnings and errors, and doesn't show macro expansions in them. The workers' messages come in the C locale. Compiling the preprocessed source can warn where the original didn't, such as on macro expansions: cargo then shows the extra warnings, and with `-Werror` the compile fails on the worker and as a worker before the compile's own command runs, at every miss.
- A proc macro that reads env vars or files that rustc doesn't record in the dep-info gets the worker's, not the client's, and they aren't part of the action's key.

## Trust

Clients never write to the action cache: only the server does, from its workers' results. Anyone who can write to the server's action cache or CAS, run its workers, or, with `grpc://`, read and change the traffic, can put code into every client's rlibs and the binaries linked from them. Use `grpcs://` and a server whose action cache only its workers write to.
