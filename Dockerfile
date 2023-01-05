## Builder
FROM rust:latest AS builder

# Create appuser
ENV USER=sidecar
ENV UID=10001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"


WORKDIR /sidecar

COPY ./ .

RUN cargo build --release

## Final image
FROM scratch

# Import from builder.
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

WORKDIR /sidecar

# Copy our build
COPY --from=builder /sidecar/target/release/casper-event-sidecar ./

# Use an unprivileged user.
USER sidecar:sidecar

ENV CONFIG_FILE=/sidecar/config.toml

CMD ["/sidecar/casper-event-sidecar", "$CONFIG_FILE"]
