# Lead Enrichment Pipeline — one-shot job container.
# Not a server: Hermes runs this with `docker run`, it does its job, and exits.
# Keys are never baked in — always pass them at runtime via --env-file.

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ ./pipeline/

# /data is where Hermes mounts the raw input files and where the CSV output
# and the sqlite cache land, so both persist across container runs.
VOLUME ["/data"]
WORKDIR /data

ENTRYPOINT ["python", "/app/pipeline/enrich_pipeline.py"]
