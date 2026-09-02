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

Hermes already has terminal access — no API server needed. Rather than
having it remember the full `docker compose run` invocation (flag names,
output paths, exact Airtable table names), point it at `bin/enrich`, which
wraps all of that:

```bash
bin/enrich <source> <input-file> --push
```

e.g. `bin/enrich vsbn vsbn_final.xlsx --push`. It validates the source name,
resolves the input file into `./data` (copying it in if it's sent from
somewhere else — e.g. a Telegram upload saved elsewhere first), derives the
output filename and the Airtable table name automatically, and exits
non-zero if any row errored. Always validate with `--dry-run --limit 20`
first — same rule as everywhere else. Run `bin/enrich --help` for the full
flag list (`--limit`, `--table` to override the Airtable table, `--smtp-verify`).

Document this exact command in the Obsidian vault (per the "architecture
before deployment" rule) alongside whichever Hermes skill/cron entry calls
it, so it's not just known to you.

## 5. The MCP server — calling this directly from Claude, no Hermes involved

`bin/enrich` is for Hermes. For asking Claude directly ("enrich this list")
from your own Claude account and your boss's, separately, there's a second,
long-running service: `mcp_server/app.py`. Full mechanics (the upload flow,
why it's shaped the way it is, auth) are in that file's module docstring —
this section is just the setup steps, once per requirement below.

**a) Generate secrets and start the server**

```bash
# Two DIFFERENT random secrets — don't reuse one for both:
openssl rand -hex 32   # -> MCP_BEARER_TOKEN
openssl rand -hex 32   # -> MCP_UPLOAD_SECRET
```

Add those plus `MCP_PUBLIC_BASE_URL` (next step) to `.env`, then:

```bash
docker compose up -d mcp
docker compose logs -f mcp   # confirm "Application startup complete"
```

This one you `up`, not `run --rm` — it stays running, `restart: unless-stopped`.

**b) Get a public HTTPS URL — Tailscale Funnel**

The server binds to `127.0.0.1:8420` only; Funnel is what actually exposes
it publicly with a real TLS cert, no port-forwarding or domain purchase:

```bash
sudo tailscale funnel 8420
```

This prints the public URL (`https://<machine-name>.<tailnet>.ts.net`).
Put that exact URL (no trailing slash) into `.env` as `MCP_PUBLIC_BASE_URL`,
then `docker compose restart mcp` so it picks up the new value — the
upload URLs `request_upload` hands out are built from this, so it has to be
right before anyone tries to attach a file. `tailscale funnel status` shows
what's currently exposed; `tailscale funnel 8420 off` turns it back off.

**c) Add the connector — once per Claude account (yours, then your boss's)**

1. claude.ai → Settings → Connectors → Add custom connector.
2. URL: `<MCP_PUBLIC_BASE_URL>/mcp`
3. Under its credential/auth setup, paste `MCP_BEARER_TOKEN` as the token —
   this is a static shared secret, not an OAuth login.
4. To actually attach files (not just enrich ones already on the server):
   Settings → Capabilities → Code execution and file creation → turn it on
   → Additional allowed domains → add the host from `MCP_PUBLIC_BASE_URL`
   (just the domain, e.g. `<machine-name>.<tailnet>.ts.net`, no scheme).
   Skip this if you'll only ever reference files already sitting in `/data`.

**d) Try it**

In a chat on either account: *"List the lead sources you can enrich"* should
call `list_sources()`. Attach a small test file and say *"enrich this,
dry run, limit 5"* to exercise the full upload path before anything real.

## Notes

- Mazenod is intentionally *not* enriched by API calls — confirmed not viable
  at scale (0% emails, 4% LinkedIn, no employer field). Its rows still get
  normalized into the CSV/Airtable, just flagged in `source`-specific notes for
  manual review rather than burning API budget on it.
- Fresh Networking is ~98% complete already — its "enrichment" step is mostly
  normalization, not paid API calls.
- The sqlite cache (`enrich_cache.db`) must persist across runs (it's in the
  mounted `/data` volume) — deleting it means re-paying for every lookup.
