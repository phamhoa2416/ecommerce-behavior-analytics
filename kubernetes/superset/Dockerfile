FROM apache/superset:latest

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libsasl2-dev \
    libldap2-dev \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV DOCKERIZE_VERSION=v0.6.1

RUN curl -L https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz \
    | tar -C /usr/local/bin -xz

RUN . /app/.venv/bin/activate && \
    uv pip install \
    clickhouse-connect>=0.6.8 \
    SQLAlchemy==1.4.46 \
    trino \
    psycopg2-binary

USER superset
