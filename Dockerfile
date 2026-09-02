# Lead Enrichment Pipeline — one image, two ways to run it:
#   - the `enrich` compose service: one-shot job, `docker run`, does its job, exits.
#   - the `mcp` compose service: long-running MCP server (mcp_server/app.py),
#     for direct use from Claude instead of through Hermes — see its module
#     docstring. Same image, same enrich_pipeline.py underneath, different
#     entrypoint (set per-service in docker-compose.yml).
# Keys are never baked in — always passed at runtime via --env-file.

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY enrich_pipeline.py .
COPY mcp_server/ ./mcp_server/

# /data is where Hermes mounts the raw input files and where the CSV output,
# the sqlite cache, and (for the mcp service) uploaded artifacts land, so
# all of it persists across container runs.
VOLUME ["/data"]
WORKDIR /data

ENTRYPOINT ["python", "/app/enrich_pipeline.py"]
