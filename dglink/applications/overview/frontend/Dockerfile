# Build context is applications/, not this directory, so that shared/static/ is
# reachable (see shared/README.md). Every COPY source below is therefore relative
# to applications/. compose.yaml sets `context: .` + `dockerfile:` to match.
FROM python:3.11-slim
WORKDIR /app
COPY overview/frontend/requirements.txt .
COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /bin/
RUN uv venv && uv pip install -r requirements.txt
COPY overview/frontend/app.py .
# Saved agent responses for the example queries, shown in the page's dropdowns so the
# demo needs no live LLM call. app.py degrades gracefully if this is ever absent.
COPY overview/frontend/example_answers.json .
COPY overview/frontend/templates/ ./templates/
# Shared assets first, then this app's own, so app-specific files win on any clash.
COPY shared/static/ ./static/
COPY overview/frontend/static/ ./static/
ARG FONT_SIZE
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base && rm -rf /var/lib/apt/lists/*
RUN envsubst '${FONT_SIZE}' < /app/templates/index_template.html > /app/templates/index.html
EXPOSE 5003
CMD ["uv", "run", "python", "app.py"]
