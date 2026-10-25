# syntax=docker/dockerfile:1.7

# Multi-stage build keeps the runtime image minimal — the producer
# only needs confluent-kafka + the pydantic event classes, not the
# full Spark stack.

FROM python:3.11-slim AS build

WORKDIR /app

# System deps for confluent-kafka's librdkafka.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[generator,producer]"


FROM python:3.11-slim AS runtime

# Non-root user — matches the Pod securityContext.
RUN useradd --system --no-create-home --uid 10001 rides

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=build /usr/local/bin /usr/local/bin
COPY --from=build /app /app

WORKDIR /app
USER 10001

# Overridable at Pod level — defaults match the local Redpanda compose.
ENV RIDES_KAFKA_BOOTSTRAP=kafka:9092
ENV RIDES_MONTH=2024-01
ENV RIDES_MAX_TRIPS=0
ENV RIDES_SEED=42

ENTRYPOINT ["python", "-m", "rides_telemetry.producer"]
CMD ["--month", "2024-01", "--max-trips", "0", "--seed", "42"]
