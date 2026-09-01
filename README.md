# Tactik Lead Enrichment Pipeline

One-shot job, not a server. Hermes runs it via `docker run`, it enriches a source,
writes a CSV, optionally pushes to Airtable, then exits. No UI — Airtable is the
visualization layer once rows land there.

Field mappings in `pipeline/enrich_pipeline.py` are verified against the real
uploaded files (not guessed), and each source's real header row is documented
in its loader's docstring.

## 1. Local test (do this before Docker, before real API keys)

```bash
pip install -r requirements.txt --break-system-packages

# Dry run — no API calls, just proves the file parses and the CSV comes out clean
python pipeline/enrich_pipeline.py \
  --source vsbn --input /path/to/vsbn_final.xlsx --output vsbn_test.csv \
  --dry-run --limit 20
```

Valid `--source` values: `fresh`, `mazenod`, `transcend`, `vsbn`, `bali`.

Once real keys are set (see `.env.example`), drop `--dry-run` and run the same
`--limit 20` command to validate real enrichment on a small sample before a
full run — same "test on ~20 before Hermes runs it at volume" rule you already
use for everything else.

## 2. Build the container

```bash
docker build -t tactik-lead-enrichment .
```

## 3. Run it locally against real data (still no Hermes yet)

```bash
mkdir -p data
cp vsbn_final.xlsx data/

docker run --rm \
  --env-file .env \
  -v "$(pwd)/data:/data" \
  tactik-lead-enrichment \
  --source vsbn --input /data/vsbn_final.xlsx --output /data/vsbn_enriched.csv --limit 20
```

Check `data/vsbn_enriched.csv` and `data/enrich_cache.db` landed correctly, then
scale `--limit` up and drop it entirely once you trust the output.

## 4. Push the image to the hub server (192.168.0.4)

```bash
docker save tactik-lead-enrichment | ssh udara@192.168.0.4 docker load
# or, if this repo is on GitHub and the hub server has SSH access to it:
# git pull && docker build -t tactik-lead-enrichment .
```

Put the real `.env` on the hub server directly (never in git) — e.g.
`~/.hermes/secrets/lead-enrichment.env`.

## 5. Give Hermes the invocation

Hermes already has terminal access — no API server needed. Either as a one-off
chat command or a skill/cron entry, the exact command it runs is:

```bash
docker run --rm \
  --env-file ~/.hermes/secrets/lead-enrichment.env \
  -v /path/to/leads:/data \
  tactik-lead-enrichment \
  --source <source> --input /data/<file> --output /data/<source>_enriched.csv \
  --airtable-push --airtable-table "Leads - <Source Name>"
```

Document this exact command in the Obsidian vault (per the "architecture before
deployment" rule) alongside whichever Hermes skill/cron entry calls it, so it's
not just known to you.

## Notes

- Mazenod is intentionally *not* enriched by API calls — confirmed not viable
  at scale (0% emails, 4% LinkedIn, no employer field). Its rows still get
  normalized into the CSV/Airtable, just flagged in `source`-specific notes for
  manual review rather than burning API budget on it.
- Fresh Networking is ~98% complete already — its "enrichment" step is mostly
  normalization, not paid API calls.
- The sqlite cache (`enrich_cache.db`) must persist across runs (it's in the
  mounted `/data` volume) — deleting it means re-paying for every lookup.
