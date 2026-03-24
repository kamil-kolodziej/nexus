FROM python:3.11-slim AS builder

WORKDIR /app

COPY packages/nexus-common/ /app/packages/nexus-common/
COPY packages/nexus-ingestion/ /app/packages/nexus-ingestion/

RUN pip install --no-cache-dir \
    /app/packages/nexus-common \
    /app/packages/nexus-ingestion

FROM python:3.11-slim

RUN groupadd -r nexus && useradd -r -g nexus nexus

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY packages/nexus-ingestion/nexus_ingestion/ /app/nexus_ingestion/

USER nexus

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

ENTRYPOINT ["python", "-m", "nexus_ingestion.main"]
