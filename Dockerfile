FROM rust:1.44

WORKDIR /usr/src/web-crawler

COPY . .
RUN cargo install --path .

CMD ["./start.sh"]
