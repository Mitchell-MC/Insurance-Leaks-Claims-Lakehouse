# Local Docker simulation of the Databricks Jobs environment (infra/jobs.tf).
# See docs/architecture_ideal_vs_actual.md's "Local validation (Docker
# Compose)" section for exactly what is and isn't simulated here -- storage
# is a plain Docker volume, not real ADLS Gen2 (Azurite emulation was tried
# and rejected; see that doc section for why); Delta config is injected via
# docker/spark-defaults.conf, not a real Databricks cluster's ambient
# config; invocation runs from source-mounted code, not the built wheel
# infra/jobs.tf's python_wheel_task actually uploads and runs.

FROM python:3.11-slim-bookworm AS builder

# Reason: matches the astral-sh/setup-uv idiom .github/workflows/ci.yml
# already uses, just via a multi-stage COPY instead of a GitHub Action.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
RUN uv pip install -e . --python /app/.venv/bin/python


FROM python:3.11-slim-bookworm AS runtime

# Reason: this repo pins no Java version anywhere (confirmed via repo
# search) -- JDK 17 is what was verified compatible with the exact
# pyspark/delta-spark versions this project's uv.lock resolves to (pyspark
# 4.1.1 / delta-spark 4.3.1), so this Dockerfile is where that previously
# implicit assumption becomes explicit and reproducible.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/app/.venv/bin:${PATH}"
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python
# Reason: JVM network/crypto init can block on blocking-entropy
# /dev/random in a container with limited entropy sources -- a well-
# documented Java-in-Docker issue. /dev/urandom is non-blocking and
# cryptographically fine for this use.
ENV JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom"

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv

COPY docker/spark-defaults.conf /opt/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/spark-conf

# Reason: pre-resolves Delta's jars into this image layer at build time, so
# no container run ever needs a live Maven/Ivy network call -- see
# docker/warm_delta_cache.py. The script itself is removed afterward; the
# populated jar cache it left behind is the point of this layer and is kept.
COPY docker/warm_delta_cache.py /tmp/warm_delta_cache.py
RUN python /tmp/warm_delta_cache.py && rm /tmp/warm_delta_cache.py

COPY src/ src/

# Reason: mirrors the exact console-script invocation infra/jobs.tf's real
# Databricks Jobs use (python_wheel_task's entry_point = "lakehouse") --
# just from a container instead of a cluster.
ENTRYPOINT ["lakehouse"]
