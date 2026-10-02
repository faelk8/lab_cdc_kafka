FROM maven:3.9.11-eclipse-temurin-17 AS plugins
WORKDIR /build
COPY plugin/pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B dependency:go-offline
COPY plugin/src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B package -DskipTests && cp target/cdc-connect-sinks-*.jar target/plugin/

# rpk solicitado: baixado no build, sem ZIP ou binário versionado no repositório.
FROM debian:bookworm-slim AS rpk
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl unzip && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp
RUN curl -fLO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip && \
    mkdir -p /out && unzip rpk-linux-amd64.zip -d /out && chmod +x /out/rpk && rm rpk-linux-amd64.zip

FROM quay.io/debezium/connect:3.3.2.Final
COPY --from=plugins --chown=1001:0 /build/target/plugin/ /kafka/connect/cdc-study-sinks/
COPY --from=rpk --chmod=755 /out/rpk /usr/local/bin/rpk
ENV PATH="/usr/local/bin:${PATH}"
