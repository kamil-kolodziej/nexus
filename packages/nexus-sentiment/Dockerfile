FROM python:3.11-slim AS base

WORKDIR /app

# Install dependencies
COPY packages/nexus-common/ packages/nexus-common/
COPY packages/nexus-sentiment/ packages/nexus-sentiment/
COPY data/ data/

RUN pip install --no-cache-dir \
    -e packages/nexus-common \
    -e packages/nexus-sentiment

ENTRYPOINT ["python", "-m", "nexus_sentiment.main"]
