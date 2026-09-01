# Tactik Lead Enrichment Pipeline

One-shot job, not a server. Hermes runs it via `docker compose run`, it enriches
a source, writes a CSV, optionally pushes to Airtable, then exits. No UI —
Airtable is the visualization layer once rows land there.

Field mappings in `enrich_pipeline.py` are verified against the real uploaded
files (not guessed), and each source's real header row is documented in its
loader's docstring.

## 0. Setup

```bash
git clone <this-repo-url>
cd tactik-enrich_pipeline
cp .env.example .env
```

Fill in `.env` with real values — `HUNTER_API_KEY`, `APOLLO_API_KEY`,
`PDL_API_KEY`, and (only if you'll use `--airtable-push`) `AIRTABLE_API_KEY` /
`AIRTABLE_BASE_ID`. Never commit `.env` — it's already in `.gitignore`.

## 1. Local test, no Docker (do this first, before real API keys)

```bash
pip install -r requirements.txt --break-system-packages

# Dry run — no API calls, just proves the file parses and the CSV comes out clean
python enrich_pipeline.py \
  --source vsbn --input /path/to/vsbn_final.xlsx --output vsbn_test.csv \
  --dry-run --limit 20
```

Valid `--source` values: `fresh`, `mazenod`, `transcend`, `vsbn`, `bali`.

Once real keys are set in `.env` (see step 0), drop `--dry-run` and run the
same `--limit 20` command to validate real enrichment on a small sample
before a full run — same "test on ~20 before Hermes runs it at volume" rule
you already use for everything else.

## 2. Run it locally with Docker Compose

Docker Compose is used here purely as a repeatable build/run config — this
job still exits when it's done, there's nothing to `up`.

```bash
mkdir -p data
cp vsbn_final.xlsx data/

docker compose build

docker compose run --rm enrich \
  --source vsbn --input /data/vsbn_final.xlsx --output /data/vsbn_enriched.csv --limit 20
```

`docker compose run` reads `.env` automatically (see `docker-compose.yml`)
and mounts `./data` into the container at `/data`. Check
`data/vsbn_enriched.csv` and `data/enrich_cache.db` landed correctly, then
scale `--limit` up and drop it entirely once you trust the output.

## 3. Deploy to production (Docker Compose)

Compose makes production the same shape as local — no `docker save`/`docker
load` image shuffling, just a git checkout and a build on the server.

```bash
# on the hub server (192.168.0.4)
git clone <this-repo-url>
cd tactik-enrich_pipeline
cp .env.example .env   # fill in the real production keys, then: chmod 600 .env
docker compose build
```

If secrets already live elsewhere on that host by convention (e.g.
`~/.hermes/secrets/lead-enrichment.env`), symlink instead of duplicating them:

```bash
ln -s ~/.hermes/secrets/lead-enrichment.env .env
```

Either way, `.env` must never be committed — it's already gitignored.

To ship a code change later, no rebuild-and-copy dance is needed:

```bash
git pull && docker compose build
```

## 4. Give Hermes the invocation

Hermes already has terminal access — no API server needed. Either as a
one-off chat command or a skill/cron entry, the exact command it runs from
the deployed repo directory is:

```bash
docker compose run --rm enrich \
  --source <source> --input /data/<file> --output /data/<source>_enriched.csv \
  --airtable-push --airtable-table "Leads - <Source Name>"
```

Document this exact command in the Obsidian vault (per the "architecture
before deployment" rule) alongside whichever Hermes skill/cron entry calls
it, so it's not just known to you.

## Notes

- Mazenod is intentionally *not* enriched by API calls — confirmed not viable
  at scale (0% emails, 4% LinkedIn, no employer field). Its rows still get
  normalized into the CSV/Airtable, just flagged in `source`-specific notes for
  manual review rather than burning API budget on it.
- Fresh Networking is ~98% complete already — its "enrichment" step is mostly
  normalization, not paid API calls.
- The sqlite cache (`enrich_cache.db`) must persist across runs (it's in the
  mounted `/data` volume) — deleting it means re-paying for every lookup.
