FROM amazonlinux:latest

COPY target/release/sub-sim-server-rust /usr/local/bin/sub-sim-server-rust
RUN chmod +x /usr/local/bin/sub-sim-server-rust

ENTRYPOINT ["/usr/local/bin/sub-sim-server-rust"]
