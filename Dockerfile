# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        zip \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# ── Smart pip installer ───────────────────────────────────────────────────────
COPY docker/pip_install.sh /usr/local/bin/pip_install
RUN chmod +x /usr/local/bin/pip_install

# ── Heavy deps (PySpark, pandas, pyarrow, grpcio) ────────────────────────────
# Separate layer — only rebuilds when requirements-heavy.txt changes.
# pip_install skips any package already at the required version.
COPY requirements-heavy.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip_install requirements-heavy.txt

# ── Lightweight deps ──────────────────────────────────────────────────────────
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip_install requirements.txt

# ── Bake Spark config ─────────────────────────────────────────────────────────
COPY conf/ conf/
ENV SPARK_CONF_DIR=/app/conf

# ── Download Spark JARs (cached on host, baked into image) ───────────────────
# BuildKit cache mount persists downloaded JARs on the host across builds.
# First build: downloads from Maven Central.
# Subsequent builds: copies from host cache — no network hit.
# JARs land in $SPARK_HOME/jars/ so Spark auto-loads them; spark.jars.packages
# is commented out in the image so Ivy never runs at runtime.
RUN --mount=type=cache,target=/root/.spark-jars-cache \
    SPARK_JARS=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    CACHE=/root/.spark-jars-cache && \
    M=https://repo1.maven.org/maven2 && \
    for jar in \
        "org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar" \
        "org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar" \
        "org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar" \
        "software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar" \
        "software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.2.1/analyticsaccelerator-s3-1.2.1.jar" \
        "org/wildfly/openssl/wildfly-openssl/2.1.4.Final/wildfly-openssl-2.1.4.Final.jar" \
        "org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar" \
        "org/checkerframework/checker-qual/3.49.5/checker-qual-3.49.5.jar" \
    ; do \
        name=$(basename "$jar"); \
        if [ ! -f "$CACHE/$name" ]; then \
            echo "  [download] $name"; \
            curl -fsSL -o "$CACHE/$name" "$M/$jar"; \
        else \
            echo "  [cached]   $name"; \
        fi; \
        cp "$CACHE/$name" "$SPARK_JARS/"; \
    done && \
    sed -i 's|^spark\.jars\.packages|# spark.jars.packages|' /app/conf/spark-defaults.conf

# ── Application code ──────────────────────────────────────────────────────────
COPY ingestion/ ingestion/
COPY ui/ ui/
RUN cd /app && zip -r ingestion.zip ingestion/

# ── Docker helpers ────────────────────────────────────────────────────────────
COPY docker/vault_init.py    docker/vault_init.py
COPY docker/entrypoint.sh    /entrypoint.sh
COPY docker/ui-entrypoint.sh /ui-entrypoint.sh
RUN chmod +x /entrypoint.sh /ui-entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
