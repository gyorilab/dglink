# Build context is applications/, not this directory, so that shared/static/ is
# reachable (see shared/README.md). Every COPY source below is therefore relative
# to applications/. compose.yaml sets `context: .` + `dockerfile:` to match.
FROM python:3.10-slim

WORKDIR /app
COPY semantic_search/frontend/requirements.txt .
RUN pip install -r requirements.txt



COPY semantic_search/frontend/app.py .
COPY semantic_search/frontend/templates/ templates/
# Shared assets first, then this app's own, so app-specific files win on any clash.
COPY shared/static/ static/
COPY semantic_search/frontend/static/ static/
ARG PORTAL_NAME
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base && rm -rf /var/lib/apt/lists/*
RUN envsubst '${PORTAL_NAME}' < /app/templates/index_template.html > /app/templates/index.html

CMD ["python", "app.py"]
