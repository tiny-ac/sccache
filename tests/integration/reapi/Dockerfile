# NativeLink on rust:latest. Its worker runs actions without a sandbox, so the rustc and
# proc macros that sccache uploads need the client's system libraries. The test client runs in
# this image too.
FROM ghcr.io/tracemachina/nativelink:v1.7.4 AS nativelink

FROM rust:latest
COPY --from=nativelink /nix /nix
RUN ln -s "$(ls -d /nix/store/*/bin/nativelink | head -n1)" /usr/local/bin/nativelink \
    && /usr/local/bin/nativelink --version
COPY nativelink.json5 /etc/nativelink.json5
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/nativelink", "/etc/nativelink.json5"]
