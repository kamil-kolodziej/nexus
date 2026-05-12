FROM python:3.11-slim AS base

WORKDIR /app

# Selects which sentiment processor's deps are baked into the image.
# Same variable is read at runtime by pydantic-settings to pick the processor —
# wire both off this single value so the image and the running service agree.
# "finbert" pulls transformers + torch (~750 MB image bloat, ~440 MB model on
# first run). "vader" keeps the image slim.
ARG NEXUS_PROCESSOR_TYPE=vader

COPY packages/nexus-common/ packages/nexus-common/
COPY packages/nexus-sentiment/ packages/nexus-sentiment/
COPY data/ data/

RUN if [ "$NEXUS_PROCESSOR_TYPE" = "finbert" ]; then \
        pip install --no-cache-dir \
            -e packages/nexus-common \
            -e "packages/nexus-sentiment[finbert]"; \
    else \
        pip install --no-cache-dir \
            -e packages/nexus-common \
            -e packages/nexus-sentiment; \
    fi

ENTRYPOINT ["python", "-m", "nexus_sentiment.main"]
