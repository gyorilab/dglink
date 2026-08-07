# Build context is applications/, not this directory, so that shared/static/ is
# reachable (see shared/README.md). Every COPY source below is therefore relative
# to applications/. compose.yaml sets `context: .` + `dockerfile:` to match.
FROM python:3.11-slim
WORKDIR /app
COPY genomic_index/genomic_index_query_frontend/requirements.txt .
RUN pip install -r requirements.txt
COPY genomic_index/genomic_index_query_frontend/app.py .
COPY genomic_index/genomic_index_query_frontend/templates/ ./templates/
# Shared assets first, then this app's own, so app-specific files win on any clash.
COPY shared/static/ ./static/
COPY genomic_index/genomic_index_query_frontend/static/ ./static/
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base && rm -rf /var/lib/apt/lists/*
CMD ["python", "app.py"]
