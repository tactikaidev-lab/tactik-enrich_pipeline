#!/usr/bin/env python3
"""
Tactik Lead Enrichment — MCP server
====================================

A long-running counterpart to enrich_pipeline.py's one-shot CLI, for direct
use from inside Claude (claude.ai / Claude Desktop custom connectors) rather
than through Hermes. Both entry points call the exact same
enrich_pipeline.run_pipeline() — there is one real implementation, not two.

WHY THIS EXISTS
  bin/enrich (the CLI wrapper) is what Hermes calls over terminal access —
  good for autonomous/scheduled runs. This server is for a human asking
  Claude directly: "enrich this list" — no Hermes, no Telegram, just the
  Claude account (yours or your boss's) with this server added as a custom
  connector under Settings -> Connectors.

TOOLS
  list_sources()     — what sources exist and what each needs
  request_upload()   — step 1 for a file you're about to attach in chat
  enrich()           — run enrichment on an uploaded artifact or a file
                        already sitting in /data

THE UPLOAD FLOW, AND WHY IT'S SHAPED THIS WAY
  MCP has no native client-to-server file upload primitive (as of the
  2025-11-25 spec) — there's a File Uploads working group, but nothing
  shipped yet. The documented, working pattern today for "the user attaches
  a file in a Claude.ai chat, and my MCP server needs its bytes":
    1. Claude's own sandbox receives the attachment via its internal
       container_upload mechanism — our server can't reach that directly.
    2. request_upload() returns a presigned URL (HMAC-signed, 5 min TTL).
    3. Claude's sandbox PUTs the file's raw bytes to that URL itself.
    4. We store it and hand back an artifact_id; enrich() takes that.
  This requires the account calling it to have "Code execution and file
  creation" enabled, AND this server's domain added under that account's
  Settings -> Capabilities -> Code execution -> Additional allowed domains.
  That's a real, non-obvious, per-account one-time setup step — see README.

AUTH
  Two separate, deliberately different mechanisms:
  - MCP tool calls (everything under /mcp): a single shared bearer token
    (MCP_BEARER_TOKEN), checked by BearerAuthMiddleware below. This is what
    you paste into Claude's connector setup as a "custom credential" — NOT
    full OAuth. Simpler, and matches what Claude's connector docs describe
    as supported for non-DCR servers; avoids standing up a Protected
    Resource Metadata / issuer_url story this single-shared-secret use case
    doesn't need.
  - Presigned upload PUTs (/uploads/<id>): authenticated by their own HMAC
    signature (MCP_UPLOAD_SECRET), not the bearer token — Claude's sandbox
    calls this directly via curl, without ever seeing the bearer token.

REQUIRED ENV VARS (in addition to whatever enrich_pipeline.py itself needs —
HUNTER_API_KEY etc. — see its own docstring)
  MCP_BEARER_TOKEN      shared secret for MCP tool calls — generate with
                        e.g. `openssl rand -hex 32`
  MCP_UPLOAD_SECRET     separate shared secret for signing upload URLs —
                        generate the same way, keep it different from the
                        bearer token
  MCP_PUBLIC_BASE_URL   the public HTTPS URL this server is reachable at
                        (e.g. your Tailscale Funnel *.ts.net URL) — used to
                        build the upload_url returned by request_upload()
  MCP_PORT              optional, defaults to 8420

The server refuses to start if any of the first three are unset — there is
no "runs wide open" mode.
"""

import hashlib
import hmac
import os
import re
import secrets
import sys
import time
import uuid
from pathlib import Path

# enrich_pipeline.py lives one directory up from this file — see Dockerfile.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import enrich_pipeline  # noqa: E402

from mcp.server.mcpserver import MCPServer  # noqa: E402
from mcp.server.mcpserver.exceptions import ToolError  # noqa: E402
from starlette.requests import Request  # noqa: E402
from starlette.responses import JSONResponse, Response  # noqa: E402

# A plain ValueError raised from inside a tool is NOT sent to the client —
# this SDK treats anything but its own ToolError as a crash and replaces the
# message with a generic "Error executing tool <name>", withholding the
# reason. Every deliberate validation failure below raises ToolError instead
# so Claude actually sees why a call failed and can explain it or retry
# correctly, per mcp/server/mcpserver/exceptions.py's own ToolError docstring.

DATA_DIR = Path(os.environ.get("MCP_DATA_DIR", "/data"))
UPLOAD_DIR = DATA_DIR / "uploads"

# enrich_pipeline.CACHE_DB ("enrich_cache.db") is a relative path — the CLI
# gets this right for free because the Docker image sets WORKDIR /data, but
# this server shouldn't silently depend on being launched from the right
# directory (a local test run, a future compose/entrypoint tweak, ...). Make
# it explicit instead of implicit: chdir into DATA_DIR ourselves, so the
# cache/artifacts table always lands in the same place regardless of cwd.
DATA_DIR.mkdir(parents=True, exist_ok=True)
os.chdir(DATA_DIR)
UPLOAD_TTL_SECONDS = 300  # 5 minutes — matches the documented container_upload pattern
MAX_UPLOAD_BYTES = 25 * 1024 * 1024  # generous for a lead-list spreadsheet, not for abuse

BEARER_TOKEN = os.environ.get("MCP_BEARER_TOKEN")
UPLOAD_SECRET = os.environ.get("MCP_UPLOAD_SECRET")
PUBLIC_BASE_URL = os.environ.get("MCP_PUBLIC_BASE_URL", "").rstrip("/")

if not BEARER_TOKEN or not UPLOAD_SECRET or not PUBLIC_BASE_URL:
    sys.exit(
        "MCP_BEARER_TOKEN, MCP_UPLOAD_SECRET, and MCP_PUBLIC_BASE_URL must all be set "
        "(see .env.example) — refusing to start wide open."
    )

# Kept in sync with bin/enrich's airtable_table_for() by hand — small,
# stable, changes rarely enough that a shared cross-language source of
# truth isn't worth the indirection.
DEFAULT_AIRTABLE_TABLES = {
    "fresh": "Leads - Fresh Networking",
    "mazenod": "Leads - Mazenod Alumni",
    "transcend": "Leads - Transcend",
    "vsbn": "Leads - VSBN",
    "bali": "Leads - Bali WhatsApp",
}


def _db():
    """Same enrich_cache.db every other lookup in this pipeline uses — one
    extra table (artifacts) for the upload flow, same "everything persists
    in one sqlite file under /data" design as the rest of the project."""
    conn = enrich_pipeline.init_cache()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS artifacts (
            artifact_id TEXT PRIMARY KEY,
            filename TEXT,
            path TEXT,
            status TEXT,
            bytes INTEGER,
            created_at REAL,
            expires_at REAL
        )
    """)
    conn.commit()
    return conn


def _sign(artifact_id, expires_at):
    msg = f"{artifact_id}:{int(expires_at)}".encode()
    return hmac.new(UPLOAD_SECRET.encode(), msg, hashlib.sha256).hexdigest()


mcp = MCPServer(
    name="tactik-lead-enrichment",
    instructions=(
        "Enriches lead lists (fills in missing email/company/LinkedIn/website) for Tactik AI Dev. "
        "Call list_sources() first if unsure which source applies to a list. To enrich a file "
        "you're about to attach: call request_upload(filename) first, send/attach the file, then "
        "call enrich() with the returned artifact_id. To enrich a file already on the server, call "
        "enrich() with filename instead of artifact_id. Always try dry_run=True with a small limit "
        "before a real run — dry_run makes no paid API calls."
    ),
)


@mcp.tool()
def list_sources() -> dict:
    """List the lead-list sources this server knows how to enrich, and what
    each one realistically needs (free vs. requires a paid key)."""
    return {
        "known_sources": {
            "fresh": "Fresh Networking — already ~98% has an email; enrichment just normalizes it. Free.",
            "mazenod": "Mazenod Alumni — confirmed not enrichable at scale (0% email anchor, 4% LinkedIn); rows are flagged for manual review, not looked up.",
            "transcend": "Transcend — domain-based email lookup: Hunter.io if HUNTER_API_KEY is set, else a free site-scrape fallback (real-world hit rate tested near zero — see enrich_pipeline.py's docstring).",
            "vsbn": "VSBN — same approach as transcend.",
            "bali": "Bali WhatsApp — phone-based lookup via Apollo + PDL; no free fallback exists for this one.",
        },
        "auto": "Any other well-formed .xlsx/.xls/.csv that ISN'T one of the 5 above — auto-maps common column names (Email, Company, Website, Phone, ...). Use source='auto' for a new/unfamiliar list.",
    }


@mcp.tool()
def request_upload(filename: str) -> dict:
    """Step 1 of enriching a file you're about to attach in this
    conversation. Call this first with the file's name (e.g.
    "new_leads.xlsx"). Returns a short-lived (5 minute) presigned upload URL
    — once the file is uploaded to it, call enrich() with the returned
    artifact_id. Requires this server's domain to be allowed under this
    account's code-execution settings; if the upload doesn't happen, that's
    almost always why."""
    ext = Path(filename).suffix.lower()
    if ext not in (".xlsx", ".xls", ".csv", ".json"):
        raise ToolError(f"unsupported file type '{ext}' — expected .xlsx, .xls, .csv, or .json")

    artifact_id = uuid.uuid4().hex
    expires_at = time.time() + UPLOAD_TTL_SECONDS
    conn = _db()
    conn.execute(
        "INSERT INTO artifacts (artifact_id, filename, path, status, bytes, created_at, expires_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (artifact_id, filename, "", "pending", 0, time.time(), expires_at),
    )
    conn.commit()

    sig = _sign(artifact_id, expires_at)
    upload_url = f"{PUBLIC_BASE_URL}/uploads/{artifact_id}?expires={int(expires_at)}&sig={sig}"
    return {
        "artifact_id": artifact_id,
        "upload_url": upload_url,
        "method": "PUT",
        "expires_in_seconds": UPLOAD_TTL_SECONDS,
        "next_step": "PUT the file's raw bytes to upload_url, then call enrich(source=..., artifact_id=...).",
    }


def _resolve_artifact(artifact_id: str):
    conn = _db()
    row = conn.execute(
        "SELECT filename, path, status FROM artifacts WHERE artifact_id=?", (artifact_id,)
    ).fetchone()
    if not row:
        raise ToolError(f"unknown artifact_id: {artifact_id}")
    filename, path, status = row
    if status != "uploaded":
        raise ToolError(
            f"artifact {artifact_id} has status '{status}', not 'uploaded' — "
            f"upload the file to its upload_url first (see request_upload)"
        )
    return Path(path), filename


@mcp.tool()
def enrich(
    source: str,
    artifact_id: str | None = None,
    filename: str | None = None,
    dry_run: bool = False,
    limit: int | None = None,
    push: bool = False,
    table: str | None = None,
    smtp_verify: bool = False,
) -> dict:
    """Enrich a lead list. Give EXACTLY ONE of artifact_id (from
    request_upload, after the file has actually been uploaded) or filename
    (a file already present on the server's /data — e.g. one Hermes placed
    there). source is one of list_sources()'s known_sources keys, or "auto"
    for anything else.

    Set push=True to also push the results into Airtable — table is picked
    automatically for the 5 known sources, but is REQUIRED when source is
    "auto" (there's no one natural table for an arbitrary list).

    Always try dry_run=True with a small limit (e.g. 20) before a real run
    — dry_run makes no paid API calls or Airtable push. Returns the same run
    summary written to <output>.summary.json: rows enriched/flagged/errored,
    per-field coverage before vs. after, and specific per-row errors if any.
    """
    if bool(artifact_id) == bool(filename):
        raise ToolError("give exactly one of artifact_id or filename")

    if artifact_id:
        input_path, original_name = _resolve_artifact(artifact_id)
        stem = Path(original_name).stem
    else:
        input_path = DATA_DIR / filename
        if not input_path.exists():
            raise ToolError(f"no file named '{filename}' in /data")
        stem = input_path.stem

    stem = re.sub(r"[^A-Za-z0-9_-]+", "_", stem)[:60] or "list"
    output_path = DATA_DIR / f"{source}_{stem}_enriched.csv"

    if push and not table:
        table = DEFAULT_AIRTABLE_TABLES.get(source)
        if not table:
            raise ToolError(
                f"push=True needs an explicit table for source='{source}' — there's no automatic one for it"
            )

    try:
        summary = enrich_pipeline.run_pipeline(
            source, input_path, output_path,
            limit=limit, dry_run=dry_run, smtp_verify=smtp_verify,
            airtable_push_flag=push, airtable_table=table,
            print_progress=True,
        )
    except ValueError as exc:
        # run_pipeline()'s own validation (bad source, missing --airtable-table,
        # ...) — a plain ValueError, correct for a library function, but it
        # needs to become a ToolError here or the client never sees why.
        raise ToolError(str(exc)) from exc
    except Exception as exc:
        raise ToolError(
            f"failed to load/parse this file as source='{source}': {type(exc).__name__}: {exc}. "
            f"If this isn't one of the 5 known lists, try source='auto' instead."
        ) from exc
    return summary


@mcp.custom_route("/uploads/{artifact_id}", methods=["PUT"])
async def handle_upload(request: Request) -> Response:
    """Receives the presigned-URL PUT — authenticated by its own HMAC
    signature (see module docstring), not the MCP bearer token."""
    artifact_id = request.path_params["artifact_id"]
    expires = request.query_params.get("expires")
    sig = request.query_params.get("sig")
    if not expires or not sig:
        return JSONResponse({"error": "missing expires/sig"}, status_code=400)
    try:
        expires_f = float(expires)
    except ValueError:
        return JSONResponse({"error": "bad expires"}, status_code=400)

    if not hmac.compare_digest(sig, _sign(artifact_id, expires_f)):
        return JSONResponse({"error": "bad signature"}, status_code=403)
    if time.time() > expires_f:
        return JSONResponse({"error": "upload URL expired — call request_upload again"}, status_code=410)

    conn = _db()
    row = conn.execute("SELECT filename, status FROM artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()
    if not row:
        return JSONResponse({"error": "unknown artifact_id"}, status_code=404)
    filename, status = row
    if status == "uploaded":
        return JSONResponse({"error": "already uploaded"}, status_code=409)

    body = await request.body()
    if not body:
        return JSONResponse({"error": "empty upload"}, status_code=400)
    if len(body) > MAX_UPLOAD_BYTES:
        return JSONResponse({"error": f"file too large — max {MAX_UPLOAD_BYTES} bytes"}, status_code=413)

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    ext = Path(filename).suffix.lower()
    dest = UPLOAD_DIR / f"{artifact_id}{ext}"
    dest.write_bytes(body)

    conn.execute(
        "UPDATE artifacts SET status='uploaded', path=?, bytes=? WHERE artifact_id=?",
        (str(dest), len(body), artifact_id),
    )
    conn.commit()
    return JSONResponse({"status": "uploaded", "artifact_id": artifact_id, "bytes": len(body)})


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok", "service": "tactik-lead-enrichment-mcp"})


class BearerAuthMiddleware:
    """Static-bearer-token check on everything except the presigned upload
    endpoint (authenticates via its own HMAC signature instead) and the
    health check. Deliberately NOT using mcp's built-in OAuth-shaped
    token_verifier/AuthSettings machinery — that requires standing up a full
    Protected Resource Metadata / issuer_url story that a single shared
    static credential doesn't need, and matches what Claude's connector
    docs describe as "custom credentials for non-DCR servers": you paste
    this token into Claude's connector setup once per account."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        path = scope["path"]
        if path.startswith("/uploads/") or path == "/health":
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers") or [])
        auth = headers.get(b"authorization", b"").decode()
        if not secrets.compare_digest(auth, f"Bearer {BEARER_TOKEN}"):
            response = JSONResponse({"error": "unauthorized"}, status_code=401)
            return await response(scope, receive, send)
        return await self.app(scope, receive, send)


def build_app():
    return BearerAuthMiddleware(mcp.streamable_http_app())


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("MCP_PORT", "8420"))
    uvicorn.run(build_app(), host="0.0.0.0", port=port, log_level="info")
