FROM rust:bookworm as builder
WORKDIR /usr/src/refresh-iceberg-datafusion-sql
COPY ./refresh-iceberg-datafusion-sql/ .
RUN cargo install --path .
FROM rust:slim-bookworm
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/refresh-iceberg-datafusion-sql /usr/local/bin/refresh-iceberg-datafusion-sql
CMD ["refresh-iceberg-datafusion-sql"]
