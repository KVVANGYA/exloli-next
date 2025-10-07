FROM rust:bullseye as builder

WORKDIR /app

# 安装 sccache 加速编译
RUN cargo install sccache --locked
ENV RUSTC_WRAPPER=sccache
ENV SCCACHE_DIR=/sccache

# 先复制依赖文件，利用 Docker 层缓存
COPY Cargo.toml Cargo.lock ./

# 创建虚拟 main.rs 来构建依赖
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > src/lib.rs && \
    cargo build --release && \
    rm -rf src

# 复制真实源代码
COPY . .

# 构建实际应用（使用 release 和编译优化）
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/sccache \
    cargo build --release --bin exloli && \
    cp target/release/exloli /usr/local/bin/exloli

FROM debian:bullseye-slim
ENV RUST_BACKTRACE=full
WORKDIR /app
RUN apt-get update \
    && apt-get install -y libsqlite3-0 libssl1.1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*  \
    && rm -rf /var/cache/apt/archives/*
RUN echo '/etc/ssl/openssl.cnf \
system_default = system_default_sect \
\
[system_default_sect] \
MinProtocol = TLSv1.2 \
CipherString = DEFAULT@SECLEVEL=1 \
' >> /etc/ssl/openssl.cnf
COPY --from=builder /usr/local/bin/exloli /usr/local/bin/exloli
CMD ["exloli"]
