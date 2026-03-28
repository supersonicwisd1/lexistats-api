"""
Lexistats API — receives samples from GitHub Actions, serves stats dashboard + API.

Endpoints:
  POST /api/v1/samples       — ingest a sample (requires API key)
  GET  /api/v1/stats         — full stats blob (same shape as old stats.json)
  GET  /api/v1/rankings      — lexicon rankings (events/sec, unique users)
  GET  /api/v1/history/:nsid — time series for a specific lexicon
  GET  /api/v1/lexicons      — list all known lexicons with latest stats
  GET  /health               — health check
  GET  /                     — dashboard page
"""

import os
from datetime import datetime
from contextlib import contextmanager
from typing import Optional
from decimal import Decimal

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Lexistats API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

DB_DSN = os.environ.get("DATABASE_URL", "")

DID_CAP_PER_NSID = 10_000
DID_EXPIRE_DAYS = 30


@contextmanager
def get_db():
    conn = psycopg2.connect(DB_DSN)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def verify_api_key(api_key: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM api_keys WHERE key = %s", (api_key,))
        if not cur.fetchone():
            raise HTTPException(status_code=401, detail="Invalid API key")


def decimal_to_float(obj):
    """Convert Decimal values for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimal_to_float(i) for i in obj]
    return obj


# --- Models ---

class SamplePayload(BaseModel):
    ts: str
    duration_sec: int = 60
    total: int
    counts: dict[str, int]
    unique_dids: Optional[dict[str, list[str]]] = None


# --- Ingest ---

@app.post("/api/v1/samples")
def ingest_sample(payload: SamplePayload, x_api_key: str = Header(...)):
    verify_api_key(x_api_key)

    ts = datetime.fromisoformat(payload.ts)
    eps = round(payload.total / max(payload.duration_sec, 1), 2)

    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO lexicon_samples (ts, duration_sec, total_events, events_per_sec) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (ts, payload.duration_sec, payload.total, eps)
        )
        sample_id = cur.fetchone()[0]

        rows = []
        for nsid, count in payload.counts.items():
            nsid_eps = round(count / max(payload.duration_sec, 1), 2)
            rows.append((sample_id, nsid, count, nsid_eps))

        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO lexicon_counts (sample_id, nsid, event_count, events_per_sec) VALUES %s",
            rows
        )

        if payload.unique_dids:
            for nsid, dids in payload.unique_dids.items():
                cur.execute(
                    "SELECT COUNT(*) FROM lexicon_unique_dids WHERE nsid = %s", (nsid,)
                )
                current_count = cur.fetchone()[0]

                for did in dids:
                    if current_count >= DID_CAP_PER_NSID:
                        cur.execute(
                            "UPDATE lexicon_unique_dids SET last_seen = %s "
                            "WHERE nsid = %s AND did = %s",
                            (ts, nsid, did)
                        )
                    else:
                        cur.execute(
                            "INSERT INTO lexicon_unique_dids (nsid, did, first_seen, last_seen) "
                            "VALUES (%s, %s, %s, %s) "
                            "ON CONFLICT (nsid, did) DO UPDATE SET last_seen = %s",
                            (nsid, did, ts, ts, ts)
                        )
                        if cur.rowcount == 1:
                            current_count += 1

        cur.execute(
            "DELETE FROM lexicon_unique_dids WHERE last_seen < now() - interval '%s days'",
            (DID_EXPIRE_DAYS,)
        )

    return {"ok": True, "sample_id": sample_id, "lexicons": len(payload.counts)}


# --- Stats (compatible with old stats.json shape) ---

@app.get("/api/v1/stats")
def get_stats():
    """Full stats blob — same shape as the old stats.json for dashboard compatibility."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Totals
        cur.execute("""
            SELECT COUNT(*) as total_samples,
                   COALESCE(SUM(total_events), 0) as total_events,
                   MAX(ts) as last_updated
            FROM lexicon_samples
        """)
        totals = cur.fetchone()

        # Per-collection aggregates
        cur.execute("""
            SELECT c.nsid,
                   SUM(c.event_count) as count,
                   ROUND(SUM(c.event_count)::numeric / NULLIF(
                       (SELECT SUM(total_events) FROM lexicon_samples), 0
                   ) * 100, 1) as pct,
                   MIN(s.ts) as first_seen,
                   MAX(s.ts) as last_seen
            FROM lexicon_counts c
            JOIN lexicon_samples s ON s.id = c.sample_id
            GROUP BY c.nsid
            ORDER BY count DESC
        """)
        collections_rows = cur.fetchall()

        collections = {}
        for r in collections_rows:
            collections[r["nsid"]] = {
                "count": int(r["count"]),
                "pct": float(r["pct"] or 0),
                "first_seen": r["first_seen"].isoformat(),
                "last_seen": r["last_seen"].isoformat(),
            }

        # History (all samples with per-collection breakdown)
        cur.execute("""
            SELECT s.id, s.ts, s.duration_sec, s.total_events,
                   s.events_per_sec
            FROM lexicon_samples s
            ORDER BY s.ts ASC
        """)
        samples = cur.fetchall()

        # Get counts for all samples in one query
        cur.execute("""
            SELECT sample_id, nsid, event_count, events_per_sec
            FROM lexicon_counts
            ORDER BY sample_id
        """)
        all_counts = cur.fetchall()

        # Group counts by sample_id
        counts_by_sample = {}
        for row in all_counts:
            sid = row["sample_id"]
            if sid not in counts_by_sample:
                counts_by_sample[sid] = {}
            counts_by_sample[sid][row["nsid"]] = {
                "count": int(row["event_count"]),
                "eps": float(row["events_per_sec"] or 0)
            }

        history = []
        for s in samples:
            sid = s["id"]
            sample_counts = counts_by_sample.get(sid, {})
            history.append({
                "ts": s["ts"].isoformat(),
                "duration_sec": s["duration_sec"],
                "total": int(s["total_events"]),
                "eps": float(s["events_per_sec"] or 0),
                "counts": {nsid: d["count"] for nsid, d in sample_counts.items()},
                "counts_per_sec": {nsid: d["eps"] for nsid, d in sample_counts.items()},
            })

        # Unique users per NSID (trailing 7 days)
        cur.execute("""
            SELECT nsid, COUNT(*) as unique_users
            FROM lexicon_unique_dids
            WHERE last_seen > now() - interval '7 days'
            GROUP BY nsid
        """)
        unique_users = {r["nsid"]: int(r["unique_users"]) for r in cur.fetchall()}

    return {
        "last_updated": totals["last_updated"].isoformat() if totals["last_updated"] else None,
        "total_samples": int(totals["total_samples"]),
        "total_events": int(totals["total_events"]),
        "collections": collections,
        "history": history,
        "unique_users_7d": unique_users,
    }


# --- Query ---

@app.get("/api/v1/rankings")
def get_rankings(
    period_hours: int = Query(default=168),
    limit: int = Query(default=50)
):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT c.nsid,
                   ROUND(AVG(c.events_per_sec), 2) as avg_eps,
                   SUM(c.event_count) as total_events,
                   COUNT(DISTINCT s.id) as sample_count,
                   MIN(s.ts) as first_sample,
                   MAX(s.ts) as last_sample
            FROM lexicon_counts c
            JOIN lexicon_samples s ON s.id = c.sample_id
            WHERE s.ts > now() - make_interval(hours => %s)
            GROUP BY c.nsid
            ORDER BY avg_eps DESC
            LIMIT %s
        """, (period_hours, limit))
        rankings = cur.fetchall()

        nsids = [r["nsid"] for r in rankings]
        uniques = {}
        if nsids:
            cur.execute("""
                SELECT nsid, COUNT(*) as unique_users
                FROM lexicon_unique_dids
                WHERE nsid = ANY(%s)
                  AND last_seen > now() - make_interval(hours => %s)
                GROUP BY nsid
            """, (nsids, period_hours))
            for row in cur.fetchall():
                uniques[row["nsid"]] = int(row["unique_users"])

        result = []
        for r in rankings:
            result.append({
                "nsid": r["nsid"],
                "avg_eps": float(r["avg_eps"]),
                "total_events": int(r["total_events"]),
                "sample_count": int(r["sample_count"]),
                "first_sample": r["first_sample"].isoformat(),
                "last_sample": r["last_sample"].isoformat(),
                "unique_users_trailing": uniques.get(r["nsid"], 0),
            })

    return {"period_hours": period_hours, "rankings": result}


@app.get("/api/v1/history/{nsid:path}")
def get_history(nsid: str, period_hours: int = Query(default=168)):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT s.ts, c.event_count, c.events_per_sec
            FROM lexicon_counts c
            JOIN lexicon_samples s ON s.id = c.sample_id
            WHERE c.nsid = %s AND s.ts > now() - make_interval(hours => %s)
            ORDER BY s.ts ASC
        """, (nsid, period_hours))
        rows = [{"ts": r["ts"].isoformat(), "event_count": int(r["event_count"]),
                 "events_per_sec": float(r["events_per_sec"])} for r in cur.fetchall()]

    return {"nsid": nsid, "period_hours": period_hours, "history": rows}


@app.get("/api/v1/lexicons")
def list_lexicons():
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            WITH latest AS (
                SELECT id FROM lexicon_samples ORDER BY ts DESC LIMIT 1
            ),
            trailing_7d AS (
                SELECT c.nsid,
                       ROUND(AVG(c.events_per_sec), 2) as avg_eps_7d,
                       SUM(c.event_count) as total_events_7d
                FROM lexicon_counts c
                JOIN lexicon_samples s ON s.id = c.sample_id
                WHERE s.ts > now() - interval '7 days'
                GROUP BY c.nsid
            )
            SELECT t.nsid,
                   t.avg_eps_7d,
                   t.total_events_7d,
                   c.event_count as latest_count,
                   c.events_per_sec as latest_eps
            FROM trailing_7d t
            LEFT JOIN latest l ON true
            LEFT JOIN lexicon_counts c ON c.sample_id = l.id AND c.nsid = t.nsid
            ORDER BY t.avg_eps_7d DESC
        """)
        rows = [decimal_to_float(dict(r)) for r in cur.fetchall()]

    return {"lexicons": rows}


@app.get("/api/v1/lexicon-meta")
def get_lexicon_meta():
    """All lexicons with schema metadata, descriptions, categories, and links."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT m.nsid, m.authority, m.domain, m.description, m.category,
                   m.tags, m.schema_type, m.lexicon_url,
                   m.total_events, m.unique_users_7d,
                   m.first_seen, m.last_seen, m.spidered_at,
                   m.schema_json IS NOT NULL as has_schema,
                   m.record_example IS NOT NULL as has_example
            FROM lexicon_meta m
            ORDER BY m.unique_users_7d DESC NULLS LAST, m.total_events DESC NULLS LAST
        """)
        rows = cur.fetchall()

    result = []
    for r in rows:
        result.append({
            "nsid": r["nsid"],
            "authority": r["authority"],
            "domain": r["domain"],
            "description": r["description"],
            "category": r["category"],
            "tags": r["tags"] or [],
            "schema_type": r["schema_type"],
            "lexicon_url": r["lexicon_url"],
            "total_events": int(r["total_events"] or 0),
            "unique_users_7d": int(r["unique_users_7d"] or 0),
            "first_seen": r["first_seen"].isoformat() if r["first_seen"] else None,
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
            "has_schema": r["has_schema"],
            "has_example": r["has_example"],
            "spidered": r["spidered_at"] is not None,
        })

    return {"lexicons": result}


@app.get("/api/v1/lexicon-meta/{nsid:path}")
def get_lexicon_detail(nsid: str):
    """Full detail for a single lexicon including schema and example record."""
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM lexicon_meta WHERE nsid = %s
        """, (nsid,))
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Lexicon not found")

    return {
        "nsid": row["nsid"],
        "authority": row["authority"],
        "domain": row["domain"],
        "description": row["description"],
        "category": row["category"],
        "tags": row["tags"] or [],
        "schema_type": row["schema_type"],
        "lexicon_url": row["lexicon_url"],
        "schema": row["schema_json"],
        "example_record": row["record_example"],
        "total_events": int(row["total_events"] or 0),
        "unique_users_7d": int(row["unique_users_7d"] or 0),
        "first_seen": row["first_seen"].isoformat() if row["first_seen"] else None,
        "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None,
        "spidered_at": row["spidered_at"].isoformat() if row["spidered_at"] else None,
    }


@app.get("/api/v1/feed/collections")
def feed_collections():
    """Return non-bsky authority wildcards for Jetstream wantedCollections."""
    official = {'app.bsky', 'chat.bsky', 'com.atproto', 'tools.ozone'}
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT authority, SUM(total_events) as events
            FROM lexicon_meta
            WHERE authority != ALL(%s)
            GROUP BY authority
            ORDER BY SUM(total_events) DESC NULLS LAST
        """, (list(official),))
        rows = cur.fetchall()

    collections = [r["authority"] + ".*" for r in rows]
    return {"collections": collections, "count": len(collections)}


@app.get("/health")
def health():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lexicon_samples")
        count = cur.fetchone()[0]
    return {"status": "ok", "samples": count}


# --- Dashboard ---

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


@app.get("/chooser", response_class=HTMLResponse)
def chooser():
    return CHOOSER_HTML


@app.get("/chooser/embed.js")
def chooser_embed_js():
    from fastapi.responses import Response
    return Response(content=CHOOSER_EMBED_JS, media_type="application/javascript")


@app.get("/feed", response_class=HTMLResponse)
def feed():
    return FEED_HTML


@app.get("/feed/view", response_class=HTMLResponse)
def feed_view(uri: str = Query(..., description="AT URI to view")):
    """Single-record viewer: fetches the record from PDS, shows nice preview + raw JSON."""
    return RECORD_VIEW_HTML.replace("__AT_URI__", uri)


DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LexiStats — ATProto Lexicon Usage</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #0d1117;
            color: #c9d1d9;
        }
        h1 { color: #58a6ff; margin: 0 0 2px 0; }
        h2 { color: #c9d1d9; margin-top: 40px; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
        .subtitle { color: #8b949e; margin: 0 0 12px 0; }
        .topbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
        .topbar-left { display: flex; align-items: center; gap: 8px; }
        .topbar-right { font-size: 0.8em; color: #8b949e; }
        .topbar-right a { color: #8b949e; }
        .topbar-right img { height: 14px; vertical-align: middle; margin-right: 3px; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 20px;
        }
        .stat-value { font-size: 2em; font-weight: bold; color: #58a6ff; }
        .stat-label { color: #8b949e; font-size: 0.9em; }
        .chart-container {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            padding: 20px;
            margin-bottom: 30px;
        }
        .authority-section {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 6px;
            margin-bottom: 20px;
            overflow: hidden;
        }
        .authority-header {
            background: #21262d;
            padding: 12px 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
        }
        .authority-header:hover { background: #30363d; }
        .authority-name { font-family: monospace; font-size: 1.1em; color: #58a6ff; }
        .authority-stats { color: #8b949e; font-size: 0.9em; }
        .authority-domain { color: #8b949e; font-size: 0.85em; margin-left: 10px; }
        .authority-domain a { color: #8b949e; }
        .authority-domain a:hover { color: #58a6ff; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px 16px; text-align: left; border-bottom: 1px solid #30363d; }
        th { color: #8b949e; font-weight: 500; background: #161b22; }
        .collection-name { font-family: monospace; }
        .collection-name a { color: #79c0ff; text-decoration: none; }
        .collection-name a:hover { text-decoration: underline; }
        .collection-name .no-link { color: #8b949e; }
        .lex-desc { color: #8b949e; font-size: 0.8em; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin-top: 2px; }
        .pct-bar {
            background: #21262d; border-radius: 3px; height: 8px;
            width: 80px; display: inline-block; margin-right: 8px; vertical-align: middle;
        }
        .pct-bar-fill { background: #238636; height: 100%; border-radius: 3px; }
        .official-badge {
            background: #238636; color: white; font-size: 0.7em;
            padding: 2px 6px; border-radius: 3px; margin-left: 8px;
        }
        .third-party-badge {
            background: #6e40c9; color: white; font-size: 0.7em;
            padding: 2px 6px; border-radius: 3px; margin-left: 8px;
        }
        .unique-badge {
            background: #1f6feb; color: white; font-size: 0.7em;
            padding: 2px 6px; border-radius: 3px; margin-left: 8px;
        }
        .loading { text-align: center; padding: 50px; color: #8b949e; }
        a { color: #58a6ff; }
        .api-section {
            background: #161b22; border: 1px solid #30363d; border-radius: 6px;
            padding: 20px 24px; margin-top: 40px;
        }
        .api-section h2 { margin-top: 0; }
        .api-section h3 { color: #c9d1d9; font-size: 1em; margin-top: 20px; margin-bottom: 8px; }
        .api-table { margin-bottom: 12px; }
        .api-table td { padding: 6px 12px; border-bottom: 1px solid #21262d; }
        .api-table code { color: #79c0ff; }
        .api-table code a { color: #79c0ff; }
        .endpoint-example {
            display: block; background: #0d1117; border: 1px solid #30363d;
            padding: 10px 14px; border-radius: 4px; color: #79c0ff;
            font-size: 0.9em; word-break: break-all;
        }
        footer { margin-top: 40px; text-align: center; color: #8b949e; font-size: 0.9em; }
        .expand-icon { color: #8b949e; transition: transform 0.2s; }
        .expanded .expand-icon { transform: rotate(90deg); }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="topbar-left"><h1>LexiStats</h1> <span style="font-size:0.85em;color:#8b949e;margin-left:12px;"><b style="color:#c9d1d9">Stats</b> · <a href="/feed" style="color:#8b949e">Live Feed</a> · <a href="/chooser" style="color:#8b949e">Chooser</a> · <a href="https://lexicon.garden" style="color:#8b949e" target="_blank">Nick's Garden ↗</a></span></div>
        <div class="topbar-right"><a href="https://linkedtrust.us"><img src="https://linkedtrust.us/static/img/logo.523bee24fbc7.svg" alt="">by LinkedTrust.us</a></div>
    </div>
    <p class="subtitle">ATProto Lexicon Usage from Jetstream</p>

    <div id="content" class="loading">Loading stats...</div>

    <div class="api-section">
        <h2>API &amp; Data Sources</h2>
        <p>Our data comes from <a href="https://github.com/bluesky-social/jetstream">Jetstream</a> sampling,
        enriched with schema data from <a href="https://lexicon.garden">Lexicon Garden</a> and
        direct PDS record fetches.</p>
        <h3>Our API</h3>
        <table class="api-table">
            <tr><td><code><a href="/api/v1/lexicon-meta">GET /api/v1/lexicon-meta</a></code></td><td>All lexicons with descriptions, categories, links, usage stats</td></tr>
            <tr><td><code>GET /api/v1/lexicon-meta/{nsid}</code></td><td>Full detail for one lexicon (schema, example record)</td></tr>
            <tr><td><code><a href="/api/v1/stats">GET /api/v1/stats</a></code></td><td>Full stats blob (history, per-collection counts)</td></tr>
            <tr><td><code><a href="/api/v1/rankings">GET /api/v1/rankings</a></code></td><td>Lexicon rankings by events/sec</td></tr>
            <tr><td><code>GET /api/v1/history/{nsid}</code></td><td>Time series for a specific lexicon</td></tr>
        </table>
        <h3>Lexicon Garden XRPC</h3>
        <p>Resolve any lexicon schema definition (the source our spider uses):</p>
        <code class="endpoint-example">GET https://lexicon.garden/xrpc/com.atproto.lexicon.resolveLexicon?nsid={nsid}</code>
        <p style="margin-top:8px">Example:
            <a href="https://lexicon.garden/xrpc/com.atproto.lexicon.resolveLexicon?nsid=app.bsky.feed.post" target="_blank" rel="noopener">
                app.bsky.feed.post
            </a>
            &middot;
            <a href="https://lexicon.garden/xrpc/com.atproto.lexicon.resolveLexicon?nsid=com.linkedclaims.claim" target="_blank" rel="noopener">
                com.linkedclaims.claim
            </a>
        </p>
    </div>

    <footer>
        Data sampled from <a href="https://github.com/bluesky-social/jetstream">Jetstream</a>.
        <a href="https://github.com/Cooperation-org/lexistats">Sampler</a> |
        <a href="https://github.com/Cooperation-org/lexistats-api">API</a> on GitHub.
        By <a href="https://linkedtrust.us">LinkedTrust.us</a>
    </footer>

    <script>
        const OFFICIAL_AUTHORITIES = ['app.bsky', 'chat.bsky', 'com.atproto', 'tools.ozone'];

        // Lexicon metadata loaded from our spider
        let lexiconMeta = {};

        function getAuthority(nsid) {
            const parts = nsid.split('.');
            return parts.length >= 2 ? parts.slice(0, 2).join('.') : nsid;
        }
        function authorityToDomain(authority) {
            return authority.split('.').reverse().join('.');
        }
        function getLexiconUrl(nsid) {
            // Use spidered metadata if available
            const meta = lexiconMeta[nsid];
            if (meta && meta.lexicon_url) {
                return { url: meta.lexicon_url, source: meta.lexicon_url.includes('github.com') ? 'github' : 'lexicon-garden' };
            }
            const authority = getAuthority(nsid);
            if (OFFICIAL_AUTHORITIES.includes(authority)) {
                const path = nsid.replace(/\\./g, '/');
                return { url: `https://github.com/bluesky-social/atproto/blob/main/lexicons/${path}.json`, source: 'github' };
            }
            return { url: null, source: 'unknown' };
        }
        function isOfficial(authority) { return OFFICIAL_AUTHORITIES.includes(authority); }

        async function loadStats() {
            try {
                const [statsRes, metaRes] = await Promise.all([
                    fetch('/api/v1/stats'),
                    fetch('/api/v1/lexicon-meta')
                ]);
                if (!statsRes.ok) throw new Error('API error');
                const stats = await statsRes.json();
                if (metaRes.ok) {
                    const metaData = await metaRes.json();
                    for (const lex of metaData.lexicons) {
                        lexiconMeta[lex.nsid] = lex;
                    }
                }
                render(stats);
            } catch (e) {
                document.getElementById('content').innerHTML = '<p>Error loading stats. Try refreshing.</p>';
            }
        }

        function render(stats) {
            const collections = Object.entries(stats.collections);
            const maxPct = collections.length > 0 ? collections[0][1].pct : 100;
            const uniqueUsers = stats.unique_users_7d || {};

            const byAuthority = {};
            for (const [nsid, data] of collections) {
                const auth = getAuthority(nsid);
                if (!byAuthority[auth]) byAuthority[auth] = { collections: [], totalCount: 0, totalPct: 0 };
                byAuthority[auth].collections.push([nsid, data]);
                byAuthority[auth].totalCount += data.count;
                byAuthority[auth].totalPct += data.pct;
            }
            const sortedAuthorities = Object.entries(byAuthority).sort((a, b) => b[1].totalCount - a[1].totalCount);

            // Count total unique users tracked
            const totalUniqueTracked = Object.values(uniqueUsers).reduce((a, b) => a + b, 0);

            let html = `
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-value">${stats.total_events.toLocaleString()}</div>
                        <div class="stat-label">Total Events</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${stats.total_samples}</div>
                        <div class="stat-label">Samples</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${collections.length}</div>
                        <div class="stat-label">Unique Lexicons</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${sortedAuthorities.length}</div>
                        <div class="stat-label">Authorities</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">${totalUniqueTracked.toLocaleString()}</div>
                        <div class="stat-label">Unique Users (7d sampled)</div>
                    </div>
                </div>
            `;

            // Top lexicons by unique users chart
            const sortedByUniques = Object.entries(uniqueUsers)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 20);

            if (sortedByUniques.length > 0) {
                html += `
                    <div class="chart-container">
                        <h3>Third-Party Lexicons by Unique Users (7d sampled) <a href="/feed" style="font-size:0.8em;font-weight:normal;">Live Feed →</a></h3>
                        <canvas id="uniqueUsersChart"></canvas>
                    </div>
                `;
            }

            if (stats.history && stats.history.length >= 1) {
                const thirdPartyLexicons = Object.entries(stats.collections)
                    .filter(([name]) => !OFFICIAL_AUTHORITIES.includes(getAuthority(name)));

                if (thirdPartyLexicons.length > 0) {
                    html += `
                        <div class="chart-container">
                            <h3>Third-Party Lexicon Distribution</h3>
                            <canvas id="thirdPartyBarChart"></canvas>
                        </div>
                        <div class="chart-container">
                            <h3>Third-Party Lexicons Over Time (events/sec)</h3>
                            <canvas id="thirdPartyChart"></canvas>
                        </div>
                        <div class="chart-container">
                            <h3>Third-Party Domains</h3>
                            <canvas id="domainPieChart"></canvas>
                        </div>
                    `;
                }

                html += `
                    <div class="chart-container">
                        <h3>Network Activity (events/sec)</h3>
                        <canvas id="historyChart"></canvas>
                    </div>
                    <div class="chart-container">
                        <h3>Top Lexicons Over Time (events/sec)</h3>
                        <canvas id="collectionsChart"></canvas>
                    </div>
                    <div class="chart-container">
                        <h3>All Lexicon Distribution</h3>
                        <canvas id="barChart"></canvas>
                    </div>
                `;
            }

            html += '<h2>Lexicons by Authority</h2>';

            for (const [authority, data] of sortedAuthorities) {
                const domain = authorityToDomain(authority);
                const official = isOfficial(authority);
                const badge = official
                    ? '<span class="official-badge">Official</span>'
                    : '<span class="third-party-badge">Third Party</span>';

                // Sum unique users for this authority
                const authUniques = data.collections.reduce((sum, [nsid]) => sum + (uniqueUsers[nsid] || 0), 0);
                const uniqueStr = authUniques > 0 ? ` &middot; ${authUniques.toLocaleString()} unique users (7d sampled)` : '';

                html += `
                    <div class="authority-section" data-authority="${authority}">
                        <div class="authority-header" onclick="toggleAuthority('${authority}')">
                            <div>
                                <span class="expand-icon">&#9654;</span>
                                <span class="authority-name">${authority}</span>
                                ${badge}
                                <span class="authority-domain">
                                    (<a href="https://${domain}" target="_blank" rel="noopener">${domain}</a>)
                                </span>
                            </div>
                            <div class="authority-stats">
                                ${data.collections.length} lexicon${data.collections.length !== 1 ? 's' : ''} &middot;
                                ${data.totalCount.toLocaleString()} events &middot;
                                ${data.totalPct.toFixed(1)}%${uniqueStr}
                            </div>
                        </div>
                        <div class="authority-content" style="display: none;">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Lexicon</th>
                                        <th>Count</th>
                                        <th>Share</th>
                                        <th>Unique Users (7d sampled)</th>
                                        <th>First Seen</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${data.collections.map(([nsid, cdata]) => {
                                        const lexUrl = getLexiconUrl(nsid);
                                        const meta = lexiconMeta[nsid];
                                        const shortName = nsid.substring(authority.length + 1);
                                        const desc = meta && meta.description ? meta.description : nsid;
                                        const cat = meta && meta.category && meta.category !== 'other' ? ' [' + meta.category + ']' : '';
                                        const tooltip = desc + cat;
                                        let nameHtml;
                                        if (lexUrl.url) {
                                            const srcLabel = lexUrl.source === 'github' ? 'GitHub' : 'Lexicon Garden';
                                            nameHtml = '<a href="' + lexUrl.url + '" target="_blank" rel="noopener" title="' + tooltip.replace(/"/g, '&quot;') + ' — view on ' + srcLabel + '">' + shortName + '</a>';
                                        } else {
                                            nameHtml = '<span class="no-link" title="' + tooltip.replace(/"/g, '&quot;') + '">' + shortName + '</span>';
                                        }
                                        if (meta && meta.description) {
                                            nameHtml += '<div class="lex-desc">' + meta.description.substring(0, 80) + (meta.description.length > 80 ? '...' : '') + '</div>';
                                        }
                                        const uniq = uniqueUsers[nsid] || 0;
                                        const uniqHtml = uniq > 0 ? uniq.toLocaleString() : '<span style="color:#484f58">-</span>';
                                        return '<tr>' +
                                            '<td class="collection-name">' + nameHtml + '</td>' +
                                            '<td>' + cdata.count.toLocaleString() + '</td>' +
                                            '<td><span class="pct-bar"><span class="pct-bar-fill" style="width:' + (cdata.pct / maxPct * 100) + '%"></span></span>' + cdata.pct + '%</td>' +
                                            '<td>' + uniqHtml + '</td>' +
                                            '<td>' + new Date(cdata.first_seen).toLocaleDateString() + '</td>' +
                                            '</tr>';
                                    }).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            }

            document.getElementById('content').innerHTML = html;

            if (sortedAuthorities.length > 0) toggleAuthority(sortedAuthorities[0][0]);
            // Aggregate unique users by authority (lexicon family), excluding official bsky/atproto
            const uniquesByAuthority = {};
            for (const [nsid, count] of Object.entries(uniqueUsers)) {
                const auth = getAuthority(nsid);
                if (OFFICIAL_AUTHORITIES.includes(auth)) continue;
                uniquesByAuthority[auth] = (uniquesByAuthority[auth] || 0) + count;
            }
            const sortedAuthByUniques = Object.entries(uniquesByAuthority)
                .sort((a, b) => b[1] - a[1]);

            if (sortedAuthByUniques.length > 0 && document.getElementById('uniqueUsersChart')) {
                const colors = ['#58a6ff','#238636','#f0883e','#a371f7','#f85149','#3fb950','#79c0ff','#d29922','#ff7b72','#7ee787','#a5d6ff','#ffa657','#56d364','#bc8cff','#ff9bce','#ffdf5d','#39d353','#8b949e','#da3633','#1f6feb'];
                new Chart(document.getElementById('uniqueUsersChart'), {
                    type: 'bar',
                    data: {
                        labels: sortedAuthByUniques.map(([auth]) => {
                            const domain = authorityToDomain(auth);
                            return domain + ' (' + auth + ')';
                        }),
                        datasets: [{
                            label: 'Unique Users (7d sampled)',
                            data: sortedAuthByUniques.map(([, count]) => count),
                            backgroundColor: colors.slice(0, sortedAuthByUniques.length),
                            borderRadius: 4
                        }]
                    },
                    options: {
                        responsive: true,
                        indexAxis: 'y',
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                            y: { grid: { display: false }, ticks: { color: '#c9d1d9', font: { size: 11 } } }
                        }
                    }
                });
            }

            if (stats.history && stats.history.length >= 1) renderCharts(stats);
        }

        function toggleAuthority(authority) {
            const section = document.querySelector('[data-authority="' + authority + '"]');
            const content = section.querySelector('.authority-content');
            const header = section.querySelector('.authority-header');
            if (content.style.display === 'none') {
                content.style.display = 'block';
                header.classList.add('expanded');
            } else {
                content.style.display = 'none';
                header.classList.remove('expanded');
            }
        }

        function renderCharts(stats) {
            const colors = ['#58a6ff','#238636','#f0883e','#a371f7','#f85149','#3fb950','#79c0ff','#d29922','#ff7b72','#7ee787','#a5d6ff','#ffa657'];
            const thirdPartyLexicons = Object.entries(stats.collections)
                .filter(([name]) => !OFFICIAL_AUTHORITIES.includes(getAuthority(name)));
            const thirdPartyNames = thirdPartyLexicons.map(([n]) => n).slice(0, 12);

            if (thirdPartyLexicons.length > 0 && document.getElementById('thirdPartyBarChart')) {
                const top12 = thirdPartyLexicons.slice(0, 12);
                new Chart(document.getElementById('thirdPartyBarChart'), {
                    type: 'bar',
                    data: {
                        labels: top12.map(([n]) => authorityToDomain(getAuthority(n)) + ': ' + n.split('.').slice(2).join('.')),
                        datasets: [{ label: 'Events', data: top12.map(([,d]) => d.count), backgroundColor: colors, borderRadius: 4 }]
                    },
                    options: { responsive: true, indexAxis: 'y', plugins: { legend: { display: false } },
                        scales: { x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                                  y: { grid: { display: false }, ticks: { color: '#c9d1d9', font: { size: 11 } } } } }
                });
            }

            if (thirdPartyNames.length > 0 && document.getElementById('thirdPartyChart')) {
                new Chart(document.getElementById('thirdPartyChart'), {
                    type: 'line',
                    data: {
                        labels: stats.history.map(h => { const d = new Date(h.ts); return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}); }),
                        datasets: thirdPartyNames.slice(0, 8).map((name, i) => ({
                            label: authorityToDomain(getAuthority(name)) + ': ' + name.split('.').slice(-1)[0],
                            data: stats.history.map(h => h.counts_per_sec[name] || 0),
                            borderColor: colors[i % colors.length], tension: 0.3, fill: false
                        }))
                    },
                    options: { responsive: true, plugins: { legend: { labels: { color: '#c9d1d9' } } },
                        scales: { x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                                  y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' }, title: { display: true, text: 'events/sec', color: '#8b949e' } } } }
                });
            }

            if (thirdPartyLexicons.length > 0 && document.getElementById('domainPieChart')) {
                const domainCounts = {};
                for (const [name, data] of thirdPartyLexicons) {
                    const domain = authorityToDomain(getAuthority(name));
                    domainCounts[domain] = (domainCounts[domain] || 0) + data.count;
                }
                const sorted = Object.entries(domainCounts).sort((a, b) => b[1] - a[1]);
                new Chart(document.getElementById('domainPieChart'), {
                    type: 'doughnut',
                    data: { labels: sorted.map(([d]) => d), datasets: [{ data: sorted.map(([,c]) => c), backgroundColor: colors }] },
                    options: { responsive: true, plugins: { legend: { position: 'right', labels: { color: '#c9d1d9' } } } }
                });
            }

            new Chart(document.getElementById('historyChart'), {
                type: 'line',
                data: {
                    labels: stats.history.map(h => { const d = new Date(h.ts); return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}); }),
                    datasets: [{ label: 'Events/sec', data: stats.history.map(h => h.eps), borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,0.1)', fill: true, tension: 0.3 }]
                },
                options: { responsive: true, plugins: { legend: { display: false } },
                    scales: { x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                              y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' }, title: { display: true, text: 'events/sec', color: '#8b949e' } } } }
            });

            const topLexicons = Object.entries(stats.collections).slice(0, 10);
            new Chart(document.getElementById('barChart'), {
                type: 'bar',
                data: {
                    labels: topLexicons.map(([n]) => n.split('.').slice(-2).join('.')),
                    datasets: [{ label: 'Share %', data: topLexicons.map(([,d]) => d.pct), backgroundColor: colors, borderRadius: 4 }]
                },
                options: { responsive: true, indexAxis: 'y', plugins: { legend: { display: false } },
                    scales: { x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e', callback: v => v + '%' } },
                              y: { grid: { display: false }, ticks: { color: '#c9d1d9' } } } }
            });

            const topNames = Object.keys(stats.collections).slice(0, 12);
            if (document.getElementById('collectionsChart')) {
                new Chart(document.getElementById('collectionsChart'), {
                    type: 'line',
                    data: {
                        labels: stats.history.map(h => { const d = new Date(h.ts); return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}); }),
                        datasets: topNames.map((name, i) => ({
                            label: name.split('.').slice(-2).join('.'),
                            data: stats.history.map(h => h.counts_per_sec[name] || 0),
                            borderColor: colors[i % colors.length], tension: 0.3, fill: false
                        }))
                    },
                    options: { responsive: true, plugins: { legend: { labels: { color: '#c9d1d9' } } },
                        scales: { x: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' } },
                                  y: { grid: { color: '#30363d' }, ticks: { color: '#8b949e' }, title: { display: true, text: 'events/sec', color: '#8b949e' } } } }
                });
            }
        }

        loadStats();
    </script>
</body>
</html>
"""


CHOOSER_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lexicon Chooser — Find ATProto Lexicons to Reuse</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1100px; margin: 0 auto; padding: 20px;
            background: #0d1117; color: #c9d1d9;
        }
        h1 { color: #58a6ff; margin-bottom: 4px; }
        .subtitle { color: #8b949e; margin-bottom: 20px; }
        a { color: #58a6ff; }

        /* Search and filters */
        .controls {
            background: #161b22; border: 1px solid #30363d; border-radius: 6px;
            padding: 16px; margin-bottom: 20px;
        }
        .search-row { display: flex; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
        .search-row input[type="text"] {
            flex: 1; min-width: 250px; padding: 8px 12px;
            background: #0d1117; border: 1px solid #30363d; border-radius: 4px;
            color: #c9d1d9; font-size: 1em;
        }
        .search-row input:focus { border-color: #58a6ff; outline: none; }
        .search-row select {
            padding: 8px 12px; background: #0d1117; border: 1px solid #30363d;
            border-radius: 4px; color: #c9d1d9; font-size: 0.9em;
        }
        .filter-pills { display: flex; flex-wrap: wrap; gap: 6px; }
        .pill {
            padding: 4px 10px; border-radius: 12px; font-size: 0.8em;
            background: #21262d; border: 1px solid #30363d; color: #8b949e;
            cursor: pointer; transition: all 0.15s;
        }
        .pill:hover { border-color: #58a6ff; color: #c9d1d9; }
        .pill.active { background: #1f6feb; border-color: #1f6feb; color: white; }
        .result-count { color: #8b949e; font-size: 0.9em; margin-bottom: 12px; }

        /* Cards */
        .cards { display: flex; flex-direction: column; gap: 8px; }
        .card {
            background: #161b22; border: 1px solid #30363d; border-radius: 6px;
            padding: 14px 18px; transition: border-color 0.15s;
        }
        .card:hover { border-color: #58a6ff; }
        .card-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; }
        .card-nsid { font-family: monospace; font-size: 1.05em; }
        .card-nsid a { color: #79c0ff; text-decoration: none; }
        .card-nsid a:hover { text-decoration: underline; }
        .card-nsid .no-link { color: #c9d1d9; }
        .card-badges { display: flex; gap: 6px; flex-shrink: 0; flex-wrap: wrap; }
        .badge {
            font-size: 0.7em; padding: 2px 8px; border-radius: 3px;
            white-space: nowrap;
        }
        .badge-cat { background: #6e40c9; color: white; }
        .badge-type { background: #21262d; color: #8b949e; border: 1px solid #30363d; }
        .badge-schema { background: #238636; color: white; }
        .badge-example { background: #1f6feb; color: white; }
        .card-desc { color: #8b949e; font-size: 0.9em; margin-top: 6px; }
        .card-stats { display: flex; gap: 20px; margin-top: 8px; font-size: 0.8em; color: #8b949e; }
        .card-stat strong { color: #c9d1d9; }
        .card-domain { color: #8b949e; font-size: 0.85em; }
        .card-domain a { color: #8b949e; }
        .card-domain a:hover { color: #58a6ff; }

        /* Detail panel */
        .card-detail {
            margin-top: 10px; padding-top: 10px; border-top: 1px solid #30363d;
            display: none;
        }
        .card.expanded .card-detail { display: block; }
        .detail-section { margin-bottom: 10px; }
        .detail-section h4 { color: #c9d1d9; font-size: 0.85em; margin: 0 0 4px 0; }
        .detail-json {
            background: #0d1117; border: 1px solid #30363d; border-radius: 4px;
            padding: 10px; font-family: monospace; font-size: 0.8em;
            overflow-x: auto; max-height: 300px; overflow-y: auto; color: #79c0ff;
        }
        .detail-fields { display: flex; flex-wrap: wrap; gap: 6px; }
        .field-chip {
            background: #21262d; border: 1px solid #30363d; border-radius: 4px;
            padding: 2px 8px; font-family: monospace; font-size: 0.8em; color: #c9d1d9;
        }
        .field-type { color: #8b949e; }

        /* Embed info */
        .embed-section {
            background: #161b22; border: 1px solid #30363d; border-radius: 6px;
            padding: 16px; margin-top: 30px;
        }
        .embed-section h3 { color: #c9d1d9; margin-top: 0; }
        .embed-code {
            background: #0d1117; border: 1px solid #30363d; border-radius: 4px;
            padding: 12px; font-family: monospace; font-size: 0.85em; color: #79c0ff;
            word-break: break-all;
        }

        footer { margin-top: 30px; text-align: center; color: #8b949e; font-size: 0.9em; }
        .loading { text-align: center; padding: 40px; color: #8b949e; }
    </style>
</head>
<body>
    <div style="display:flex;justify-content:space-between;align-items:center;">
        <div style="display:flex;align-items:center;gap:12px;"><h1><a href="/" style="text-decoration:none; color: #58a6ff;">LexiStats</a></h1> <span style="font-size:0.85em;color:#8b949e;"><a href="/" style="color:#8b949e">Stats</a> · <a href="/feed" style="color:#8b949e">Live Feed</a> · <b style="color:#c9d1d9">Chooser</b> · <a href="https://lexicon.garden" style="color:#8b949e" target="_blank">Nick's Garden ↗</a></span></div>
        <span style="font-size:0.8em;color:#8b949e;"><a href="https://linkedtrust.us" style="color:#8b949e;"><img src="https://linkedtrust.us/static/img/logo.523bee24fbc7.svg" alt="" style="height:14px;vertical-align:middle;margin-right:3px;">by LinkedTrust.us</a></span>
    </div>
    <p class="subtitle">Search and discover ATProto lexicons to reuse in your app. Sorted by real usage data from the network.</p>

    <div class="controls">
        <div class="search-row">
            <input type="text" id="search" placeholder="Search lexicons by name, description, or authority..." autofocus>
            <select id="sortBy">
                <option value="users">Most users (7d)</option>
                <option value="events">Most events</option>
                <option value="newest">Newest first</option>
                <option value="alpha">A-Z</option>
            </select>
        </div>
        <div class="filter-pills" id="categoryPills"></div>
    </div>

    <div class="result-count" id="resultCount"></div>
    <div class="cards" id="cards"><div class="loading">Loading lexicons...</div></div>

    <div class="embed-section">
        <h3>Embed this chooser</h3>
        <p style="color:#8b949e; font-size:0.9em;">Add the lexicon chooser to your site:</p>
        <div class="embed-code">&lt;div id="lexicon-chooser"&gt;&lt;/div&gt;<br>&lt;script src="https://lexistats.linkedtrust.us/chooser/embed.js"&gt;&lt;/script&gt;</div>
    </div>

    <footer>
        Powered by <a href="/">LexiStats</a>.
        Data from <a href="https://github.com/bluesky-social/jetstream">Jetstream</a> +
        <a href="https://lexicon.garden">Lexicon Garden</a>.
        <a href="https://github.com/Cooperation-org/lexistats-api">GitHub</a> |
        By <a href="https://linkedtrust.us">LinkedTrust.us</a>
    </footer>

    <script>
        let allLexicons = [];
        let activeCategory = null;

        async function load() {
            try {
                const res = await fetch('/api/v1/lexicon-meta');
                if (!res.ok) throw new Error('API error');
                const data = await res.json();
                allLexicons = data.lexicons;
                buildCategoryPills();
                renderCards();
            } catch (e) {
                document.getElementById('cards').innerHTML = '<p>Error loading. Try refreshing.</p>';
            }
        }

        function buildCategoryPills() {
            const cats = {};
            for (const lex of allLexicons) {
                const cat = lex.category || 'other';
                cats[cat] = (cats[cat] || 0) + 1;
            }
            const sorted = Object.entries(cats).sort((a, b) => b[1] - a[1]);
            const container = document.getElementById('categoryPills');
            container.innerHTML = '<span class="pill active" onclick="filterCategory(null, this)">All (' + allLexicons.length + ')</span>' +
                sorted.map(([cat, count]) =>
                    '<span class="pill" onclick="filterCategory(\\'' + cat + '\\', this)">' + cat + ' (' + count + ')</span>'
                ).join('');
        }

        function filterCategory(cat, el) {
            activeCategory = cat;
            document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
            el.classList.add('active');
            renderCards();
        }

        function renderCards() {
            const query = document.getElementById('search').value.toLowerCase().trim();
            const sortBy = document.getElementById('sortBy').value;

            let filtered = allLexicons.filter(lex => {
                if (activeCategory && lex.category !== activeCategory) return false;
                if (!query) return true;
                return (lex.nsid && lex.nsid.toLowerCase().includes(query)) ||
                       (lex.description && lex.description.toLowerCase().includes(query)) ||
                       (lex.authority && lex.authority.toLowerCase().includes(query)) ||
                       (lex.domain && lex.domain.toLowerCase().includes(query)) ||
                       (lex.category && lex.category.toLowerCase().includes(query)) ||
                       (lex.tags && lex.tags.some(t => t.toLowerCase().includes(query)));
            });

            filtered.sort((a, b) => {
                if (sortBy === 'users') return (b.unique_users_7d || 0) - (a.unique_users_7d || 0);
                if (sortBy === 'events') return (b.total_events || 0) - (a.total_events || 0);
                if (sortBy === 'newest') return (b.first_seen || '').localeCompare(a.first_seen || '');
                return a.nsid.localeCompare(b.nsid);
            });

            document.getElementById('resultCount').textContent =
                filtered.length + ' lexicon' + (filtered.length !== 1 ? 's' : '') +
                (query ? ' matching "' + query + '"' : '') +
                (activeCategory ? ' in ' + activeCategory : '');

            const shown = filtered.slice(0, 100);
            const html = shown.map(lex => {
                const nsidHtml = lex.lexicon_url
                    ? '<a href="' + lex.lexicon_url + '" target="_blank" rel="noopener">' + lex.nsid + '</a>'
                    : '<span class="no-link">' + lex.nsid + '</span>';

                let badges = '';
                if (lex.category && lex.category !== 'other')
                    badges += '<span class="badge badge-cat">' + lex.category + '</span>';
                if (lex.schema_type)
                    badges += '<span class="badge badge-type">' + lex.schema_type + '</span>';
                if (lex.has_schema)
                    badges += '<span class="badge badge-schema">schema</span>';
                if (lex.has_example)
                    badges += '<span class="badge badge-example">example</span>';

                const desc = lex.description
                    ? '<div class="card-desc">' + escHtml(lex.description) + '</div>'
                    : '';

                const users = lex.unique_users_7d || 0;
                const events = lex.total_events || 0;
                const firstSeen = lex.first_seen ? new Date(lex.first_seen).toLocaleDateString() : '?';

                return '<div class="card" data-nsid="' + lex.nsid + '" onclick="toggleDetail(this, \\'' + lex.nsid + '\\')">' +
                    '<div class="card-header">' +
                        '<div><span class="card-nsid">' + nsidHtml + '</span>' +
                        '<span class="card-domain"> — <a href="https://' + lex.domain + '" target="_blank" rel="noopener">' + lex.domain + '</a></span></div>' +
                        '<div class="card-badges">' + badges + '</div>' +
                    '</div>' +
                    desc +
                    '<div class="card-stats">' +
                        '<span><strong>' + users.toLocaleString() + '</strong> users (7d)</span>' +
                        '<span><strong>' + events.toLocaleString() + '</strong> events</span>' +
                        '<span>First seen: ' + firstSeen + '</span>' +
                    '</div>' +
                    '<div class="card-detail" id="detail-' + lex.nsid.replace(/\\./g, '-') + '"></div>' +
                '</div>';
            }).join('');

            document.getElementById('cards').innerHTML = html || '<p style="color:#8b949e;text-align:center;padding:30px;">No lexicons match your search.</p>';
            if (filtered.length > 100) {
                document.getElementById('cards').innerHTML += '<p style="color:#8b949e;text-align:center;">Showing first 100 of ' + filtered.length + ' results.</p>';
            }
        }

        async function toggleDetail(card, nsid) {
            if (card.classList.contains('expanded')) {
                card.classList.remove('expanded');
                return;
            }
            // Collapse others
            document.querySelectorAll('.card.expanded').forEach(c => c.classList.remove('expanded'));
            card.classList.add('expanded');

            const detailId = 'detail-' + nsid.replace(/\\./g, '-');
            const detailEl = document.getElementById(detailId);
            if (detailEl.dataset.loaded) return;

            detailEl.innerHTML = '<span style="color:#8b949e;">Loading...</span>';
            try {
                const res = await fetch('/api/v1/lexicon-meta/' + nsid);
                if (!res.ok) throw new Error('Not found');
                const data = await res.json();
                let html = '';

                if (data.schema) {
                    const defs = data.schema.defs || data.schema;
                    const mainDef = defs.main || defs;

                    // Show fields from schema
                    if (mainDef.record && mainDef.record.properties) {
                        html += '<div class="detail-section"><h4>Fields</h4><div class="detail-fields">';
                        for (const [name, prop] of Object.entries(mainDef.record.properties)) {
                            const type = prop.type || prop.ref || '?';
                            html += '<span class="field-chip">' + name + ' <span class="field-type">: ' + type + '</span></span>';
                        }
                        html += '</div></div>';
                    }

                    // Show inferred fields
                    if (data.schema.inferred_fields) {
                        html += '<div class="detail-section"><h4>Fields (inferred from example)</h4><div class="detail-fields">';
                        for (const [name, type] of Object.entries(data.schema.inferred_fields)) {
                            html += '<span class="field-chip">' + name + ' <span class="field-type">: ' + type + '</span></span>';
                        }
                        html += '</div></div>';
                    }

                    html += '<div class="detail-section"><h4>Schema</h4><div class="detail-json">' +
                        escHtml(JSON.stringify(data.schema, null, 2)) + '</div></div>';
                }

                if (data.example_record) {
                    html += '<div class="detail-section"><h4>Example Record</h4><div class="detail-json">' +
                        escHtml(JSON.stringify(data.example_record, null, 2)) + '</div></div>';
                }

                if (!data.schema && !data.example_record) {
                    html = '<p style="color:#8b949e;">No schema or example data available yet. We spider periodically.</p>';
                }

                // Links
                html += '<div class="detail-section" style="margin-top:8px;">';
                if (data.lexicon_url)
                    html += '<a href="' + data.lexicon_url + '" target="_blank" rel="noopener">View schema definition &rarr;</a> &nbsp; ';
                html += '<a href="/api/v1/lexicon-meta/' + nsid + '" target="_blank">JSON API &rarr;</a>';
                html += '</div>';

                detailEl.innerHTML = html;
                detailEl.dataset.loaded = '1';
            } catch (e) {
                detailEl.innerHTML = '<p style="color:#f85149;">Failed to load detail.</p>';
            }
        }

        function escHtml(s) {
            return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
        }

        document.getElementById('search').addEventListener('input', renderCards);
        document.getElementById('sortBy').addEventListener('change', renderCards);

        load();
    </script>
</body>
</html>
"""


CHOOSER_EMBED_JS = """\
(function() {
    const container = document.getElementById('lexicon-chooser');
    if (!container) return;
    const iframe = document.createElement('iframe');
    iframe.src = 'https://lexistats.linkedtrust.us/chooser';
    iframe.style.cssText = 'width:100%;min-height:700px;border:1px solid #30363d;border-radius:6px;';
    iframe.setAttribute('frameborder', '0');
    container.appendChild(iframe);
})();
"""


RECORD_VIEW_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATProto Record Viewer</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 700px; margin: 0 auto; padding: 20px;
            background: #0d1117; color: #c9d1d9;
        }
        a { color: #58a6ff; text-decoration: none; }
        a:hover { text-decoration: underline; }
        h1 { color: #58a6ff; font-size: 1.2em; margin-bottom: 4px; }
        .subtitle { color: #8b949e; font-size: 0.85em; margin-bottom: 16px; }
        .record-card {
            border: 1px solid #30363d; border-radius: 8px; padding: 16px;
            background: #161b22; margin-bottom: 16px;
        }
        .record-uri { font-family: monospace; font-size: 0.8em; color: #79c0ff;
            word-break: break-all; margin-bottom: 12px; padding: 8px;
            background: #0d1117; border-radius: 4px; }
        .record-meta { font-size: 0.8em; color: #8b949e; margin-bottom: 8px; }
        .record-meta span { margin-right: 12px; }
        .record-preview { margin: 12px 0; }
        .record-preview .doc-title { font-size: 1.1em; font-weight: 600; color: #c9d1d9; }
        .record-preview .doc-excerpt { font-size: 0.85em; color: #8b949e; line-height: 1.5; margin-top: 4px; }
        .record-preview .doc-meta { font-size: 0.75em; color: #484f58; margin-top: 6px; }
        .field { display: inline-block; margin: 2px 4px 2px 0; padding: 2px 8px;
            background: #0d1117; border: 1px solid #21262d; border-radius: 3px;
            font-size: 0.8em; }
        .field-name { color: #8b949e; }
        .field-val { color: #c9d1d9; }
        .json-section { margin-top: 16px; }
        .json-section h3 { font-size: 0.9em; color: #8b949e; margin-bottom: 6px; }
        .json-block {
            background: #0d1117; border: 1px solid #21262d; border-radius: 6px;
            padding: 12px; font-family: monospace; font-size: 0.75em;
            overflow-x: auto; max-height: 500px; overflow-y: auto; color: #79c0ff;
            white-space: pre-wrap; word-break: break-all;
        }
        .actions { display: flex; gap: 10px; margin-top: 12px; flex-wrap: wrap; }
        .btn {
            padding: 6px 14px; border-radius: 4px; border: 1px solid #30363d;
            background: #21262d; color: #c9d1d9; cursor: pointer; font-size: 0.85em;
            text-decoration: none; display: inline-block;
        }
        .btn:hover { border-color: #58a6ff; text-decoration: none; }
        .btn-claim { background: #a371f7; border-color: #a371f7; color: white; }
        .btn-claim:hover { background: #bc8cff; }
        .loading { color: #8b949e; text-align: center; padding: 40px; }
        .error { color: #f85149; padding: 20px; }
        footer { margin-top: 30px; text-align: center; color: #484f58; font-size: 0.75em; }
    </style>
</head>
<body>
    <h1><a href="/feed">LexiStats Feed</a> / Record Viewer</h1>
    <p class="subtitle">Viewing an ATProto record with full details.</p>

    <div id="content"><div class="loading">Loading record...</div></div>

    <footer>
        <a href="/feed">Back to feed</a> |
        <a href="/chooser">Lexicon Chooser</a> |
        <a href="https://github.com/Cooperation-org/lexistats-api">GitHub</a> |
        <a href="https://linkedtrust.us">LinkedTrust.us</a>
    </footer>

    <script>
    const AT_URI = '__AT_URI__';

    function esc(s) {
        if (!s) return '';
        return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }

    async function loadRecord() {
        const el = document.getElementById('content');
        try {
            // Parse AT URI: at://did/collection/rkey
            const m = AT_URI.match(/^at:\\/\\/([^/]+)\\/([^/]+)\\/(.+)$/);
            if (!m) { el.innerHTML = '<div class="error">Invalid AT URI</div>'; return; }
            const [, did, collection, rkey] = m;

            // Resolve DID to PDS
            const didDoc = await fetch('https://plc.directory/' + encodeURIComponent(did)).then(r => r.json());
            const pds = didDoc.service?.find(s => s.type === 'AtprotoPersonalDataServer')?.serviceEndpoint;
            if (!pds) { el.innerHTML = '<div class="error">Could not resolve PDS for DID</div>'; return; }

            // Fetch the record
            const recordUrl = pds + '/xrpc/com.atproto.repo.getRecord?repo=' + encodeURIComponent(did) + '&collection=' + encodeURIComponent(collection) + '&rkey=' + encodeURIComponent(rkey);
            const res = await fetch(recordUrl);
            if (!res.ok) { el.innerHTML = '<div class="error">Record not found (' + res.status + ')</div>'; return; }
            const data = await res.json();
            const record = data.value || {};

            // Get handle if available
            const handle = didDoc.alsoKnownAs?.find(a => a.startsWith('at://'))?.replace('at://', '') || did;

            // Fetch lexicon metadata from our API
            let meta = {};
            try {
                const metaRes = await fetch('/api/v1/lexicon-meta/' + encodeURIComponent(collection));
                if (metaRes.ok) meta = await metaRes.json();
            } catch {}

            // Build nice preview
            const authority = collection.split('.').slice(0,2).join('.');
            const domain = meta.domain || authority.split('.').reverse().join('.');
            const shortNsid = collection.substring(authority.length + 1);
            const name = record.title || record.name || record.trackName || record.podcastName || record.gameName || (shortNsid + ' by ' + domain);

            // Render fields preview
            let fieldsHtml = '';
            const skip = new Set(['$type', 'createdAt', 'updatedAt']);
            if (record.title) {
                fieldsHtml += '<div class="doc-title">' + esc(record.title) + '</div>';
                if (record.textContent) fieldsHtml += '<div class="doc-excerpt">' + esc(record.textContent.substring(0, 500)) + '</div>';
                if (record.publishedAt) fieldsHtml += '<div class="doc-meta">' + new Date(record.publishedAt).toLocaleDateString() + '</div>';
            } else {
                for (const [key, val] of Object.entries(record)) {
                    if (skip.has(key)) continue;
                    let v = typeof val === 'string' ? esc(val.substring(0, 100)) : typeof val === 'number' ? val : JSON.stringify(val).substring(0, 80);
                    fieldsHtml += '<span class="field"><span class="field-name">' + esc(key) + ':</span> <span class="field-val">' + v + '</span></span> ';
                }
            }

            const claimUrl = 'https://live.linkedtrust.us/claim?subject=' + encodeURIComponent(AT_URI) + '&name=' + encodeURIComponent(name);
            const bskyProfile = 'https://bsky.app/profile/' + encodeURIComponent(handle);

            el.innerHTML =
                '<div class="record-card">' +
                    '<div class="record-uri">' + esc(AT_URI) + '</div>' +
                    '<div class="record-meta">' +
                        '<span>Collection: <b>' + esc(collection) + '</b></span>' +
                        '<span>By: <a href="' + esc(bskyProfile) + '" target="_blank">' + esc(handle) + '</a></span>' +
                        (meta.description ? '<div style="margin-top:4px">' + esc(meta.description) + '</div>' : '') +
                    '</div>' +
                    '<div class="record-preview">' + fieldsHtml + '</div>' +
                    '<div class="actions">' +
                        '<a class="btn btn-claim" href="' + esc(claimUrl) + '" target="_blank">Make a Claim About This</a>' +
                        '<a class="btn" href="' + esc(bskyProfile) + '" target="_blank">View Author</a>' +
                        (meta.lexicon_url ? '<a class="btn" href="' + esc(meta.lexicon_url) + '" target="_blank">View Schema</a>' : '') +
                        (record.site && record.path ? '<a class="btn" href="' + esc(record.site + record.path) + '" target="_blank">View Original</a>' : '') +
                    '</div>' +
                '</div>' +
                '<div class="json-section">' +
                    '<h3>Raw Record JSON</h3>' +
                    '<div class="json-block">' + esc(JSON.stringify(record, null, 2)) + '</div>' +
                '</div>';

            document.title = name + ' — ATProto Record Viewer';
        } catch (e) {
            el.innerHTML = '<div class="error">Failed to load: ' + esc(e.message) + '</div>';
        }
    }

    loadRecord();
    </script>
</body>
</html>
"""

FEED_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ATProto Feed — Beyond Bluesky</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 700px; margin: 0 auto; padding: 16px;
            background: #0d1117; color: #c9d1d9;
        }
        a { color: #58a6ff; text-decoration: none; }
        a:hover { text-decoration: underline; }
        h1 { color: #58a6ff; margin-bottom: 4px; font-size: 1.3em; }
        .subtitle { color: #8b949e; margin-bottom: 16px; font-size: 0.9em; }

        /* Status bar */
        .status-bar {
            display: flex; justify-content: space-between; align-items: center;
            padding: 8px 0; margin-bottom: 8px; border-bottom: 1px solid #21262d;
            flex-wrap: wrap; gap: 8px;
        }
        .status-left { display: flex; align-items: center; gap: 8px; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
        .status-dot.connected { background: #3fb950; }
        .status-dot.connecting { background: #d29922; }
        .status-dot.disconnected { background: #f85149; }
        .status-text { color: #8b949e; font-size: 0.8em; }
        .controls { display: flex; gap: 6px; align-items: center; }
        .btn {
            padding: 4px 10px; border-radius: 4px; border: 1px solid #30363d;
            background: #21262d; color: #c9d1d9; cursor: pointer; font-size: 0.8em;
        }
        .btn:hover { border-color: #58a6ff; }
        .btn.active { background: #1f6feb; border-color: #1f6feb; color: white; }
        .btn.connect-ready { border-color: #3fb950; color: #3fb950; }
        .btn.connect-ready:hover { background: #3fb950; color: white; }

        /* New records pill */
        .new-pill {
            display: none; text-align: center; margin-bottom: 8px;
        }
        .new-pill button {
            background: #1f6feb; color: white; border: none; border-radius: 20px;
            padding: 8px 20px; cursor: pointer; font-size: 0.9em; font-weight: 600;
            transition: background 0.15s;
        }
        .new-pill button:hover { background: #388bfd; }
        .new-pill.visible { display: block; }

        /* Filters */
        .filter-bar {
            display: flex; gap: 8px; margin-bottom: 8px; flex-wrap: wrap; align-items: center;
        }
        .filter-bar input {
            flex: 1; min-width: 200px; padding: 5px 10px;
            background: #0d1117; border: 1px solid #21262d; border-radius: 4px;
            color: #c9d1d9; font-size: 0.85em;
        }
        .filter-bar input:focus { border-color: #58a6ff; outline: none; }
        .pills { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 10px; }
        .pill {
            padding: 2px 8px; border-radius: 10px; font-size: 0.7em;
            background: #21262d; border: 1px solid #30363d; color: #8b949e;
            cursor: pointer;
        }
        .pill:hover { border-color: #58a6ff; color: #c9d1d9; }
        .pill.active { background: #1f6feb; border-color: #1f6feb; color: white; }
        .pill-count { font-size: 0.85em; color: #8b949e; margin-left: 2px; }
        .pill.active .pill-count { color: rgba(255,255,255,0.7); }

        /* Feed — light line-between style */
        .feed { display: flex; flex-direction: column; }
        .record {
            padding: 10px 0; border-bottom: 1px solid #21262d;
        }
        .record:first-child { padding-top: 0; }
        .record:hover { background: #161b2208; }
        .record-top { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
        .record-left { display: flex; align-items: baseline; gap: 6px; flex-wrap: wrap; min-width: 0; }
        .record-domain {
            font-size: 0.7em; font-weight: 600; padding: 1px 5px;
            border-radius: 3px; white-space: nowrap; flex-shrink: 0;
        }
        .record-nsid { font-family: monospace; font-size: 0.85em; color: #79c0ff; }
        .record-nsid a { color: #79c0ff; }
        .record-desc { font-size: 0.75em; color: #8b949e; margin-top: 1px; }
        .record-time { font-size: 0.7em; color: #484f58; white-space: nowrap; flex-shrink: 0; }
        .record-fields { margin-top: 4px; }
        .field {
            display: inline-block; margin: 1px 3px 1px 0; padding: 1px 6px;
            background: #0d1117; border: 1px solid #21262d; border-radius: 3px;
            font-size: 0.75em; max-width: 350px; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap; vertical-align: top;
        }
        .field-name { color: #8b949e; }
        .field-val { color: #c9d1d9; }
        .field-val a { color: #58a6ff; }
        .record-footer {
            display: flex; justify-content: space-between; align-items: center;
            margin-top: 4px; font-size: 0.7em;
        }
        .record-did { color: #484f58; font-family: monospace; }
        .record-did a { color: #484f58; }
        .record-did a:hover { color: #58a6ff; }

        /* Three interaction icons */
        .record-actions { display: flex; gap: 12px; align-items: center; }
        .action-btn {
            display: flex; align-items: center; gap: 3px;
            color: #8b949e; background: none; border: none; cursor: pointer;
            padding: 2px 0; font-size: 0.85em;
        }
        .action-btn:hover { color: #58a6ff; }
        .action-btn svg { width: 14px; height: 14px; fill: currentColor; }
        .action-btn.claim-action { color: #a371f7; }
        .action-btn.claim-action:hover { color: #bc8cff; }
        .action-btn.like-action:hover { color: #f85149; }
        .action-btn.comment-action:hover { color: #3fb950; }

        /* Operation badges */
        .op-create { color: #3fb950; }
        .op-update { color: #d29922; }

        /* Expand detail */
        .record-detail { display: none; margin-top: 6px; }
        .record.expanded .record-detail { display: block; }
        .detail-json {
            background: #0d1117; border: 1px solid #21262d; border-radius: 4px;
            padding: 8px; font-family: monospace; font-size: 0.7em;
            overflow-x: auto; max-height: 200px; overflow-y: auto; color: #79c0ff;
            white-space: pre-wrap; word-break: break-all;
        }

        /* Category colors */
        .cat-social { background: #238636; color: white; }
        .cat-messaging { background: #1f6feb; color: white; }
        .cat-gaming { background: #a371f7; color: white; }
        .cat-streaming { background: #f85149; color: white; }
        .cat-publishing { background: #d29922; color: white; }
        .cat-identity { background: #3fb950; color: white; }
        .cat-iot { background: #79c0ff; color: #0d1117; }
        .cat-reputation { background: #bc8cff; color: #0d1117; }
        .cat-music { background: #f0883e; color: white; }
        .cat-curation { background: #56d364; color: #0d1117; }
        .cat-events { background: #ff7b72; color: white; }
        .cat-status { background: #ffa657; color: #0d1117; }
        .cat-podcasts { background: #8957e5; color: white; }
        .cat-other { background: #21262d; color: #8b949e; border: 1px solid #30363d; }

        .empty-state { text-align: center; padding: 60px 20px; color: #8b949e; }
        footer { margin-top: 30px; padding-top: 12px; border-top: 1px solid #21262d; text-align: center; color: #484f58; font-size: 0.75em; }
        footer a { color: #8b949e; }

        /* Custom renderer: document (standard.site) */
        .doc-render { padding: 2px 0; }
        .doc-render .doc-title { font-size: 1em; font-weight: 600; color: #c9d1d9; margin: 2px 0; line-height: 1.3; }
        .doc-render .doc-title a { color: #c9d1d9; }
        .doc-render .doc-title a:hover { color: #58a6ff; }
        .doc-render .doc-excerpt { font-size: 0.8em; color: #8b949e; line-height: 1.4; margin: 2px 0;
            display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; }
        .doc-render .doc-meta { font-size: 0.7em; color: #484f58; margin-top: 4px; display: flex; gap: 8px; align-items: center; }
        .doc-render .doc-tag { padding: 1px 5px; background: #21262d; border-radius: 8px; font-size: 0.9em; color: #8b949e; }

        /* Custom renderer: music (teal.fm, rocksky) */
        .music-render { display: flex; align-items: center; gap: 10px; padding: 2px 0; }
        .music-render .music-icon { font-size: 1.4em; flex-shrink: 0; }
        .music-render .music-info { min-width: 0; }
        .music-render .music-track { font-size: 0.9em; font-weight: 600; color: #c9d1d9; }
        .music-render .music-artist { font-size: 0.8em; color: #f0883e; }
        .music-render .music-album { font-size: 0.75em; color: #8b949e; }

        /* Custom renderer: streaming (place.stream) */
        .stream-render { display: flex; align-items: center; gap: 10px; padding: 2px 0; }
        .stream-render .stream-live { background: #f85149; color: white; font-size: 0.65em; font-weight: 700;
            padding: 1px 5px; border-radius: 3px; text-transform: uppercase; letter-spacing: 0.5px; }
        .stream-render .stream-count { font-size: 1.1em; font-weight: 700; color: #79c0ff; }
        .stream-render .stream-label { font-size: 0.8em; color: #8b949e; }

        /* Custom renderer: claims (linkedclaims) */
        .claim-render { padding: 2px 0; border-left: 2px solid #a371f7; padding-left: 8px; }
        .claim-render .claim-type { font-size: 0.8em; font-weight: 600; color: #a371f7; text-transform: capitalize; }
        .claim-render .claim-subject { font-size: 0.8em; color: #79c0ff; font-family: monospace;
            overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 100%; }
        .claim-render .claim-source { font-size: 0.75em; color: #8b949e; }
        .claim-render .claim-stars { color: #d29922; }

        /* Custom renderer: gaming (anisota) */
        .game-render { display: flex; align-items: center; gap: 10px; padding: 2px 0; }
        .game-render .game-icon { font-size: 1.2em; flex-shrink: 0; }
        .game-render .game-name { font-size: 0.9em; font-weight: 600; color: #a371f7; }
        .game-render .game-event { font-size: 0.8em; color: #8b949e; }

        /* Custom renderer: podcast (podping) */
        .pod-render { display: flex; align-items: center; gap: 10px; padding: 2px 0; }
        .pod-render .pod-icon { font-size: 1.2em; flex-shrink: 0; }
        .pod-render .pod-title { font-size: 0.9em; font-weight: 600; color: #c9d1d9; }
        .pod-render .pod-url { font-size: 0.75em; color: #58a6ff; }

        /* Load more arrow */
        .load-more {
            position: fixed; bottom: 20px; left: max(10px, calc(50% - 370px));
            z-index: 90;
        }
        .load-more button {
            width: 40px; height: 40px; border-radius: 50%;
            background: #21262d; border: 1px solid #30363d; color: #8b949e;
            cursor: pointer; font-size: 1.2em; display: flex; align-items: center; justify-content: center;
            transition: all 0.15s;
        }
        .load-more button:hover { border-color: #3fb950; color: #3fb950; background: #161b22; }
        .load-more button.loading { color: #d29922; border-color: #d29922; cursor: wait; }
        @media (max-width: 750px) {
            .load-more { left: 10px; }
        }

        /* Toast notification */
        .toast {
            position: fixed; bottom: 20px; right: 20px;
            background: #238636; color: white; padding: 10px 16px;
            border-radius: 6px; font-size: 0.85em; z-index: 999;
            animation: fadeIn 0.2s ease;
        }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: none; } }
    </style>
</head>
<body>
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px;">
        <div style="display:flex;align-items:center;gap:12px;"><h1><a href="/">LexiStats</a></h1> <span style="font-size:0.85em;color:#8b949e;"><a href="/" style="color:#8b949e">Stats</a> · <b style="color:#c9d1d9">Live Feed</b> · <a href="/chooser" style="color:#8b949e">Chooser</a> · <a href="https://lexicon.garden" style="color:#8b949e" target="_blank">Nick's Garden ↗</a></span></div>
        <span style="font-size:0.8em;color:#8b949e;"><a href="https://linkedtrust.us" style="color:#8b949e;"><img src="https://linkedtrust.us/static/img/logo.523bee24fbc7.svg" alt="" style="height:14px;vertical-align:middle;margin-right:3px;">by LinkedTrust.us</a></span>
    </div>
    <p class="subtitle">ATProto records from beyond Bluesky. <span style="color:#3fb950">Comment</span>, <span style="color:#f85149">like</span>, or <span style="color:#a371f7">claim about</span> any record.</p>

    <div class="status-bar">
        <div class="status-left">
            <span class="status-dot disconnected" id="statusDot"></span>
            <span class="status-text" id="statusText">Not connected</span>
        </div>
        <div class="controls">
            <button class="btn connect-ready" id="connectBtn" onclick="toggleConnection()">Connect</button>
            <button class="btn" onclick="clearFeed()">Clear</button>
            <a href="/chooser" class="btn">Chooser</a>
        </div>
    </div>

    <div class="filter-bar">
        <input type="text" id="filterInput" placeholder="Filter by NSID, domain, content..." oninput="applyFilterToExisting()">
        <span style="color:#30363d;margin:0 2px;">|</span>
        <input type="text" id="uriInput" placeholder="at:// or bsky.app URL" style="max-width:200px;" onkeydown="if(event.key==='Enter'){goToRecord(this.value);this.value='';}">
        <button class="btn" onclick="goToRecord(document.getElementById('uriInput').value);document.getElementById('uriInput').value='';" style="flex-shrink:0;">View</button>
    </div>
    <div class="pills" id="categoryPills"></div>

    <div class="new-pill" id="newPill">
        <button onclick="loadBuffered()">
            <span id="newCount">0</span> new records — click to load
        </button>
    </div>

    <div class="feed" id="feed">
        <div class="empty-state" id="emptyState">
            <p>ATProto feed from beyond Bluesky.</p>
            <p style="margin-top:12px;"><button class="btn connect-ready" onclick="toggleConnection()" style="font-size:1em;padding:6px 16px;">Connect</button></p>
            <p style="margin-top:8px;font-size:0.85em;">Records buffer silently — load them when you're ready.</p>
        </div>
    </div>

    <div class="load-more" id="loadMore">
        <button onclick="loadBatch()" id="loadMoreBtn" title="Load latest records">&#x2191;</button>
    </div>

    <footer>
        Via <a href="https://github.com/bluesky-social/jetstream">Jetstream</a> server-side filtered WebSocket.
        Schemas from <a href="https://lexicon.garden">Lexicon Garden</a>.
        <a href="/chooser">Chooser</a> |
        <a href="/api/v1/feed/collections">API</a> |
        <a href="https://github.com/Cooperation-org/lexistats-api">GitHub</a> |
        <a href="https://linkedtrust.us">LinkedTrust.us</a>
    </footer>

    <script>
    const JETSTREAM_BASE = 'wss://jetstream2.us-east.bsky.network/subscribe';
    const OFFICIAL = new Set(['app.bsky', 'chat.bsky', 'com.atproto', 'tools.ozone']);
    const MAX_VISIBLE = 200;
    const MAX_BUFFER = 500;
    const CATEGORY_COLORS = {
        social: 'cat-social', messaging: 'cat-messaging', gaming: 'cat-gaming',
        streaming: 'cat-streaming', publishing: 'cat-publishing', identity: 'cat-identity',
        iot: 'cat-iot', reputation: 'cat-reputation', music: 'cat-music', podcasts: 'cat-podcasts',
        curation: 'cat-curation', events: 'cat-events', status: 'cat-status', other: 'cat-other',
    };

    let lexiconMeta = {};
    let sockets = [];
    let connected = false;
    let eventTotal = 0;
    let activeCategory = null;
    let buffer = [];  // Buffered events waiting to be shown

    // SVG icons for the three actions
    const ICON_COMMENT = '<svg viewBox="0 0 24 24"><path d="M1.751 10c0-4.42 3.584-8 8.005-8h4.366c4.49 0 8.129 3.58 8.129 8 0 4.42-3.64 8-8.13 8h-.582c-.297 0-.59.078-.848.226l-2.89 1.66c-.39.22-.87-.083-.87-.529v-1.472a.75.75 0 0 0-.75-.75h-.332c-4.42 0-8.005-3.58-8.005-8zM5.5 10.5a1 1 0 1 0 2 0 1 1 0 0 0-2 0zm4 0a1 1 0 1 0 2 0 1 1 0 0 0-2 0zm4 0a1 1 0 1 0 2 0 1 1 0 0 0-2 0z"/></svg>';
    const ICON_LIKE = '<svg viewBox="0 0 24 24"><path d="M16.697 5.5c-1.222-.06-2.679.51-3.89 2.16l-.805 1.09-.806-1.09c-1.21-1.65-2.667-2.22-3.89-2.16C5.177 5.57 3.5 7.24 3.5 9.66c0 2.12 1.56 4.09 3.54 5.9 1.61 1.46 3.45 2.72 4.96 3.68 1.51-.96 3.35-2.22 4.96-3.68 1.98-1.81 3.54-3.78 3.54-5.9 0-2.42-1.677-4.09-3.803-4.16z"/></svg>';
    const ICON_CLAIM = '<svg viewBox="0 0 24 24"><path d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>';
    const ICON_JSON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>';
    const ICON_LINK = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71"/></svg>';

    // --- Init: load metadata + collections ---

    async function init() {
        try {
            // Only fetch metadata if not already loaded
            if (Object.keys(lexiconMeta).length === 0) {
                const metaRes = await fetch('/api/v1/lexicon-meta');
                const metaData = await metaRes.json();
                for (const lex of metaData.lexicons) lexiconMeta[lex.nsid] = lex;
                buildCategoryPills();
            }
            const colRes = await fetch('/api/v1/feed/collections');
            const colData = await colRes.json();
            connectJetstream(colData.collections);
        } catch (e) {
            setStatus('disconnected', 'Failed to load metadata');
            connected = false;
            updateConnectBtn();
        }
    }

    function buildCategoryPills() {
        const cats = {};
        for (const lex of Object.values(lexiconMeta)) {
            if (OFFICIAL.has(lex.authority)) continue;
            const cat = lex.category || 'other';
            cats[cat] = (cats[cat] || 0) + 1;
        }
        const sorted = Object.entries(cats).sort((a, b) => b[1] - a[1]);
        const el = document.getElementById('categoryPills');
        el.innerHTML = '<span class="pill active" data-cat="all" onclick="setCategory(null, this)">All <span class="pill-count"></span></span>' +
            sorted.map(([cat]) =>
                '<span class="pill" data-cat="' + cat + '" onclick="setCategory(\\'' + cat + '\\', this)">' + cat + ' <span class="pill-count"></span></span>'
            ).join('');
    }

    function setCategory(cat, el) {
        activeCategory = cat;
        document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
        el.classList.add('active');
        applyFilterToExisting();
    }

    // --- Jetstream connection ---

    function connectJetstream(collections) {
        const chunkSize = 100;
        for (let i = 0; i < collections.length; i += chunkSize) {
            const chunk = collections.slice(i, i + chunkSize);
            const params = chunk.map(c => 'wantedCollections=' + encodeURIComponent(c)).join('&');
            const url = JETSTREAM_BASE + '?' + params;
            openSocket(url, Math.floor(i / chunkSize));
        }
        connected = true;
        updateConnectBtn();
    }

    function openSocket(url, idx) {
        const ws = new WebSocket(url);
        sockets[idx] = ws;

        ws.onopen = () => {
            const openCount = sockets.filter(s => s && s.readyState === 1).length;
            setStatus('connected', openCount + ' conn');
        };

        ws.onmessage = (e) => {
            try {
                handleEvent(JSON.parse(e.data));
            } catch {}
        };

        ws.onclose = () => {
            if (!connected) return;
            setStatus('connecting', 'Reconnecting...');
            setTimeout(() => { if (connected) openSocket(url, idx); }, 3000);
        };

        ws.onerror = () => {};
    }

    function closeSockets() {
        for (let i = 0; i < sockets.length; i++) {
            if (sockets[i]) { sockets[i].close(); sockets[i] = null; }
        }
        sockets = [];
        setStatus('disconnected', 'Disconnected');
    }

    function setStatus(state, text) {
        document.getElementById('statusDot').className = 'status-dot ' + state;
        document.getElementById('statusText').textContent = text;
    }

    function toggleConnection() {
        if (connected) {
            connected = false;
            closeSockets();
            updateConnectBtn();
        } else {
            setStatus('connecting', 'Connecting...');
            init();
        }
    }

    function updateConnectBtn() {
        const btn = document.getElementById('connectBtn');
        btn.textContent = connected ? 'Disconnect' : 'Connect';
        btn.classList.toggle('active', connected);
        btn.classList.toggle('connect-ready', !connected);
    }

    // --- Event handling: buffer, don't render ---

    function handleEvent(msg) {
        if (msg.kind !== 'commit' || !msg.commit) return;
        const commit = msg.commit;
        if (!commit.collection || !commit.record) return;
        if (commit.operation === 'delete') return;

        eventTotal++;

        // Buffer the event — don't render yet
        buffer.push(msg);
        if (buffer.length > MAX_BUFFER) buffer.shift();

        updateNewPill();
    }

    function updateNewPill() {
        const pill = document.getElementById('newPill');
        const count = document.getElementById('newCount');
        const n = buffer.length;
        if (n > 0) {
            count.textContent = n;
            pill.classList.add('visible');
        } else {
            pill.classList.remove('visible');
        }
        updatePillCounts();
    }

    function updatePillCounts() {
        // Count from buffer if buffering, otherwise count visible rendered records
        const cats = {};
        let total = 0;
        if (buffer.length > 0) {
            for (const msg of buffer) {
                const nsid = msg.commit.collection;
                const meta = lexiconMeta[nsid] || {};
                const cat = meta.category || 'other';
                cats[cat] = (cats[cat] || 0) + 1;
                total++;
            }
        } else {
            document.querySelectorAll('.record[data-category]').forEach(el => {
                const cat = el.dataset.category;
                cats[cat] = (cats[cat] || 0) + 1;
                total++;
            });
        }
        document.querySelectorAll('.pill[data-cat]').forEach(p => {
            const cat = p.dataset.cat;
            const span = p.querySelector('.pill-count');
            if (!span) return;
            const c = cat === 'all' ? total : (cats[cat] || 0);
            span.textContent = c > 0 ? c : '';
        });
    }

    // --- Load buffered records into feed ---

    function loadBuffered() {
        if (buffer.length === 0) return;

        const feed = document.getElementById('feed');
        const empty = document.getElementById('emptyState');
        if (empty) empty.remove();

        // Render ALL buffered events into DOM (newest first)
        const fragment = document.createDocumentFragment();
        for (let i = buffer.length - 1; i >= 0; i--) {
            fragment.appendChild(makeRecord(buffer[i]));
        }
        feed.insertBefore(fragment, feed.firstChild);

        // Cap visible records
        while (feed.children.length > MAX_VISIBLE) {
            feed.removeChild(feed.lastChild);
        }

        // Clear buffer, update counts from rendered records, then apply current filter
        buffer = [];
        updateNewPill();
        updatePillCounts();
        applyFilterToExisting();
        if (connected) {
            connected = false;
            closeSockets();
            updateConnectBtn();
        }
    }

    function getAuthority(nsid) {
        const parts = nsid.split('.');
        return parts.length >= 2 ? parts.slice(0, 2).join('.') : nsid;
    }

    function authorityToDomain(auth) {
        return auth.split('.').reverse().join('.');
    }

    // --- Rendering: light line-between style ---

    function makeRecord(msg) {
        const commit = msg.commit;
        const nsid = commit.collection;
        const meta = lexiconMeta[nsid] || {};
        const authority = getAuthority(nsid);
        const category = meta.category || 'other';
        const domain = meta.domain || authorityToDomain(authority);
        const shortNsid = nsid.substring(authority.length + 1);
        const catClass = CATEGORY_COLORS[category] || 'cat-other';
        const ts = msg.time_us ? new Date(msg.time_us / 1000) : new Date();
        const timeStr = ts.toLocaleTimeString();
        const did = msg.did || '';
        const shortDid = did.length > 24 ? did.substring(0, 20) + '...' : did;
        const rkey = commit.rkey || '';
        const atUri = 'at://' + did + '/' + nsid + '/' + rkey;
        const opClass = commit.operation === 'create' ? 'op-create' : commit.operation === 'update' ? 'op-update' : '';
        const opLabel = commit.operation || 'create';

        const hasCustomRenderer = RENDERERS.some(r => nsid.startsWith(r.match) || nsid === r.match);
        const fieldsHtml = renderContent(commit.record, nsid, did);
        const lexUrl = meta.lexicon_url || ('https://lexicon.garden/search?q=' + encodeURIComponent(nsid));

        // Best-guess human name for this record (for claim prefill)
        const r = commit.record || {};
        const claimName = r.title || r.name || r.trackName || r.podcastName || r.gameName || (shortNsid + ' by ' + domain);

        const el = document.createElement('div');
        el.className = 'record';
        el.dataset.category = category;
        el.dataset.nsid = nsid;
        el.dataset.searchable = (nsid + ' ' + domain + ' ' + (meta.description || '') + ' ' + JSON.stringify(commit.record)).toLowerCase();

        el.innerHTML =
            '<div class="record-top">' +
                '<div class="record-left">' +
                    '<span class="record-domain ' + catClass + '">' + esc(domain) + '</span>' +
                    '<span class="record-nsid">' + (lexUrl ? '<a href="' + esc(lexUrl) + '" target="_blank">' + esc(shortNsid) + '</a>' : esc(shortNsid)) + '</span>' +
                    '<span class="record-time"><span class="' + opClass + '">' + esc(opLabel) + '</span> ' + timeStr + '</span>' +
                '</div>' +
            '</div>' +
            (!hasCustomRenderer && meta.description ? '<div class="record-desc">' + esc(meta.description).substring(0, 120) + '</div>' : '') +
            '<div class="record-fields">' + fieldsHtml + '</div>' +
            '<div class="record-footer">' +
                '<span class="record-did"><a href="https://bsky.app/profile/' + esc(did) + '" target="_blank">' + esc(shortDid) + '</a></span>' +
                '<div class="record-actions">' +
                    '<button class="action-btn" onclick="event.stopPropagation(); doJson(this)" title="Show raw JSON">' + ICON_JSON + '</button>' +
                    '<button class="action-btn" onclick="event.stopPropagation(); doPermalink(\\'' + esc(atUri) + '\\')" title="Copy link to this record">' + ICON_LINK + '</button>' +
                    '<button class="action-btn comment-action" onclick="event.stopPropagation(); doComment(\\'' + esc(atUri) + '\\')" title="Comment — post about this record">' + ICON_COMMENT + '</button>' +
                    '<button class="action-btn like-action" onclick="event.stopPropagation(); doLike(\\'' + esc(atUri) + '\\')" title="Like this record">' + ICON_LIKE + '</button>' +
                    '<button class="action-btn claim-action" onclick="event.stopPropagation(); doClaim(\\'' + esc(atUri) + '\\', \\'' + esc(claimName) + '\\')" title="Make a claim about this record">' + ICON_CLAIM + '</button>' +
                '</div>' +
            '</div>' +
            '<div class="record-detail">' +
                '<div style="display:flex;justify-content:flex-end;gap:6px;margin-bottom:4px;">' +
                    '<button class="btn" style="font-size:0.7em;padding:2px 6px;" onclick="copyJson(this)">Copy</button>' +
                '</div>' +
                '<div class="detail-json">' + esc(JSON.stringify(commit.record, null, 2)) + '</div>' +
                '<div style="margin-top:4px;font-size:0.7em;color:#484f58;">AT URI: <code>' + esc(atUri) + '</code></div>' +
            '</div>';

        // Store record data for comment/action use
        el._recordData = commit.record;

        // Click to expand/collapse detail — but not when clicking inside the JSON detail
        el.addEventListener('click', (e) => {
            if (e.target.closest('.record-detail') || e.target.closest('.record-actions')) return;
            el.classList.toggle('expanded');
        });

        return el;
    }

    // --- Custom renderer registry ---
    // Maps NSID prefix to a render function. First match wins.

    const RENDERERS = [
        { match: 'site.standard.document', fn: renderDocument },
        { match: 'site.standard.publication', fn: renderPublication },
        { match: 'fm.teal.alpha.feed.play', fn: renderMusicPlay },
        { match: 'app.rocksky.music', fn: renderMusicPlay },
        { match: 'place.stream.livestream', fn: renderLivestream },
        { match: 'place.stream.live.viewerCount', fn: renderViewerCount },
        { match: 'place.stream.broadcast.origin', fn: renderBroadcast },
        { match: 'place.stream.chat.message', fn: renderStreamChat },
        { match: 'place.stream.', fn: renderStreamGeneric },
        { match: 'com.linkedclaims.claim', fn: renderClaim },
        { match: 'net.anisota.', fn: renderGame },
        { match: 'at.podping.', fn: renderPodcast },
        { match: 'com.puzzmo.', fn: renderGame },
    ];

    function renderContent(record, nsid, did) {
        for (const r of RENDERERS) {
            if (nsid.startsWith(r.match) || nsid === r.match) {
                const html = r.fn(record, nsid, did);
                if (html) return html;
            }
        }
        return renderFieldsGeneric(record);
    }

    // --- standard.site document: title + excerpt + tags + link ---
    function renderDocument(r) {
        if (!r.title) return null;
        const url = (r.site && r.path) ? esc(r.site + r.path) : '';
        const title = url
            ? '<a href="' + url + '" target="_blank" onclick="event.stopPropagation()">' + esc(r.title) + '</a>'
            : esc(r.title);
        const excerpt = r.textContent ? esc(r.textContent.substring(0, 250)) : '';
        const date = r.publishedAt ? new Date(r.publishedAt).toLocaleDateString() : '';
        const tags = Array.isArray(r.tags) ? r.tags.slice(0, 4).map(t => '<span class="doc-tag">' + esc(t) + '</span>').join(' ') : '';
        const site = r.site ? esc(r.site.replace(/^https?:\\/\\//, '').replace(/\\/$/, '')) : '';
        return '<div class="doc-render">' +
            '<div class="doc-title">' + title + '</div>' +
            (excerpt ? '<div class="doc-excerpt">' + excerpt + '</div>' : '') +
            '<div class="doc-meta">' + (date ? '<span>' + date + '</span>' : '') + (site ? '<span>' + site + '</span>' : '') + (tags ? '<span>' + tags + '</span>' : '') + '</div>' +
            '</div>';
    }

    // --- standard.site publication: name + description ---
    function renderPublication(r) {
        if (!r.name) return null;
        const url = r.url ? esc(r.url) : '';
        const name = url
            ? '<a href="' + url + '" target="_blank" onclick="event.stopPropagation()">' + esc(r.name) + '</a>'
            : esc(r.name);
        return '<div class="doc-render">' +
            '<div class="doc-title">' + name + '</div>' +
            (r.description ? '<div class="doc-excerpt">' + esc(r.description.substring(0, 200)) + '</div>' : '') +
            '</div>';
    }

    // --- teal.fm / rocksky music play ---
    function renderMusicPlay(r) {
        const track = r.trackName || r.musicName || r.name || '';
        const artist = r.artistName || r.artist || '';
        if (!track && !artist) return null;
        const album = r.albumName || r.album || '';
        return '<div class="music-render">' +
            '<span class="music-icon">\\u266B</span>' +
            '<div class="music-info">' +
                (track ? '<div class="music-track">' + esc(track) + '</div>' : '') +
                (artist ? '<div class="music-artist">' + esc(artist) + '</div>' : '') +
                (album ? '<div class="music-album">' + esc(album) + '</div>' : '') +
            '</div></div>';
    }

    // --- place.stream.livestream: title + canonical URL + last seen ---
    function renderLivestream(r) {
        const title = r.title || 'Untitled stream';
        const url = r.canonicalUrl || r.url || '';
        const link = url ? '<a href="' + esc(url) + '" target="_blank" onclick="event.stopPropagation()">' + esc(title) + '</a>' : esc(title);
        const lastSeen = r.lastSeenAt ? new Date(r.lastSeenAt).toLocaleTimeString() : '';
        return '<div class="stream-render">' +
            '<span class="stream-live">LIVE</span>' +
            '<div style="min-width:0"><div style="font-weight:600;color:#c9d1d9">' + link + '</div>' +
            (lastSeen ? '<div style="font-size:0.75em;color:#484f58">last seen ' + lastSeen + '</div>' : '') +
            '</div></div>';
    }

    // --- place.stream.live.viewerCount ---
    function renderViewerCount(r) {
        const count = r.count || r.viewers || r.viewerCount || 0;
        const streamer = r.streamer || '';
        const shortStreamer = streamer.length > 24 ? streamer.substring(0, 20) + '...' : streamer;
        return '<div class="stream-render">' +
            '<span class="stream-live">LIVE</span>' +
            '<span class="stream-count">' + Number(count).toLocaleString() + '</span>' +
            '<span class="stream-label">viewers</span>' +
            (streamer ? '<span style="font-size:0.75em;color:#484f58;margin-left:6px">' + esc(shortStreamer) + '</span>' : '') +
            '</div>';
    }

    // --- place.stream.broadcast.origin: streamer going live on a server ---
    function renderBroadcast(r) {
        const streamer = r.streamer || '';
        const server = r.server || '';
        const shortStreamer = streamer.length > 28 ? streamer.substring(0, 24) + '...' : streamer;
        const shortServer = server.replace('did:web:', '');
        return '<div class="stream-render">' +
            '<span class="stream-live">BROADCAST</span>' +
            '<div style="min-width:0">' +
                '<div style="font-weight:600;color:#c9d1d9">' + esc(shortStreamer) + '</div>' +
                '<div style="font-size:0.75em;color:#484f58">on ' + esc(shortServer) + '</div>' +
            '</div></div>';
    }

    // --- place.stream.chat.message ---
    function renderStreamChat(r) {
        const text = r.text || '';
        return '<div style="padding:2px 0">' +
            '<span style="font-size:0.85em;color:#c9d1d9">' + esc(text.substring(0, 200)) + '</span>' +
            '</div>';
    }

    // --- place.stream generic fallback ---
    function renderStreamGeneric(r) {
        if (r.title) return '<div class="stream-render"><span class="stream-live">STREAM</span> <span class="stream-label" style="color:#c9d1d9">' + esc(r.title) + '</span></div>';
        return null;
    }

    // --- com.linkedclaims.claim ---
    function renderClaim(r) {
        const claimType = r.claimType || r.claim_type || 'claim';
        const subject = r.subject || '';
        const source = r.source || r.sourceURI || '';
        const stars = r.stars ? '\\u2605'.repeat(Math.min(r.stars, 5)) : '';
        return '<div class="claim-render">' +
            '<div class="claim-type">' + esc(claimType) + (stars ? ' <span class="claim-stars">' + stars + '</span>' : '') + '</div>' +
            (subject ? '<div class="claim-subject">' + esc(subject) + '</div>' : '') +
            (source ? '<div class="claim-source">source: ' + renderValue(source) + '</div>' : '') +
            '</div>';
    }

    // --- anisota games / puzzmo ---
    function renderGame(r, nsid) {
        const parts = nsid.split('.');
        const eventType = parts[parts.length - 1] || 'event';
        const name = r.gameName || r.name || r.gameId || '';
        return '<div class="game-render">' +
            '<span class="game-icon">\\u1F3AE</span>' +
            '<div>' +
                (name ? '<div class="game-name">' + esc(name) + '</div>' : '') +
                '<div class="game-event">' + esc(eventType) + '</div>' +
            '</div></div>';
    }

    // --- podping podcast ---
    function renderPodcast(r) {
        const title = r.podcastName || r.medium || '';
        const url = r.feedUrl || r.iri || '';
        return '<div class="pod-render">' +
            '<span class="pod-icon">\\u1F399</span>' +
            '<div>' +
                (title ? '<div class="pod-title">' + esc(title) + '</div>' : '') +
                (url ? '<div class="pod-url">' + renderValue(url) + '</div>' : '') +
            '</div></div>';
    }

    // --- Generic fallback ---
    function renderFieldsGeneric(record) {
        if (!record || typeof record !== 'object') return '';
        let html = '';
        const skip = new Set(['$type', 'createdAt', 'updatedAt']);
        let count = 0;
        for (const [key, val] of Object.entries(record)) {
            if (skip.has(key)) continue;
            if (count >= 5) { html += '<span class="field"><span class="field-name">...</span></span>'; break; }
            html += '<span class="field"><span class="field-name">' + esc(key) + ':</span> <span class="field-val">' + renderValue(val) + '</span></span>';
            count++;
        }
        return html;
    }

    function renderValue(val) {
        if (val === null || val === undefined) return '<span style="color:#484f58">null</span>';
        if (typeof val === 'string') {
            if (val.length > 60) val = val.substring(0, 57) + '...';
            if (val.startsWith('http://') || val.startsWith('https://'))
                return '<a href="' + esc(val) + '" target="_blank" onclick="event.stopPropagation()">' + esc(val.substring(0, 40)) + '</a>';
            if (val.startsWith('did:'))
                return '<a href="https://bsky.app/profile/' + esc(val) + '" target="_blank" onclick="event.stopPropagation()">' + esc(val.substring(0, 20)) + '...</a>';
            if (val.startsWith('at://'))
                return '<span style="color:#a371f7">' + esc(val.substring(0, 40)) + '</span>';
            if (/^\\d{4}-\\d{2}-\\d{2}T/.test(val)) {
                try { return new Date(val).toLocaleString(); } catch { return esc(val); }
            }
            return esc(val);
        }
        if (typeof val === 'number') return '<span style="color:#79c0ff">' + val.toLocaleString() + '</span>';
        if (typeof val === 'boolean') return '<span style="color:#3fb950">' + val + '</span>';
        if (Array.isArray(val)) {
            if (val.length === 0) return '[]';
            if (val.length <= 3 && val.every(v => typeof v === 'string')) return esc(val.join(', '));
            return '[' + val.length + ' items]';
        }
        if (typeof val === 'object') {
            const keys = Object.keys(val);
            if (keys.length <= 2) return esc(keys.join(', '));
            return '{' + keys.length + ' fields}';
        }
        return esc(String(val));
    }

    // --- Three interaction buttons ---

    function doJson(btn) {
        // Toggle JSON detail on the parent record
        const record = btn.closest('.record');
        if (record) record.classList.toggle('expanded');
    }

    function copyJson(btn) {
        const record = btn.closest('.record');
        if (!record || !record._recordData) return;
        const json = JSON.stringify(record._recordData, null, 2);
        navigator.clipboard.writeText(json).then(() => {
            showToast('JSON copied');
        });
    }

    function doPermalink(atUri) {
        const url = window.location.origin + '/feed/view?uri=' + encodeURIComponent(atUri);
        navigator.clipboard.writeText(url).then(() => {
            showToast('Record link copied');
        });
    }

    function doComment(atUri) {
        const el = event.target.closest('.record');
        const record = el ? el._recordData : null;
        const title = record ? (record.title || record.name || record.trackName || '') : '';
        const visibleLink = record ? (record.site && record.path ? record.site + record.path : record.canonicalUrl || record.url || '') : '';
        const text = (title ? 'About ' + title + ':\\n\\n' : '') + (visibleLink ? visibleLink + '\\n\\n' : '') + atUri;
        window.open('https://bsky.app/intent/compose?text=' + encodeURIComponent(text), '_blank');
    }

    function doLike(atUri) {
        // Needs OAuth — for now, copy URI and show info
        navigator.clipboard.writeText(atUri).then(() => {
            showToast('Like requires Bluesky OAuth (coming soon)');
        });
    }

    function doClaim(atUri, name) {
        // Open LinkedTrust claim form with subject prefilled
        const url = 'https://live.linkedtrust.us/claim?subject=' + encodeURIComponent(atUri) + (name ? '&name=' + encodeURIComponent(name) : '');
        window.open(url, '_blank');
    }

    function showToast(msg) {
        const el = document.createElement('div');
        el.className = 'toast';
        el.textContent = msg;
        document.body.appendChild(el);
        setTimeout(() => el.remove(), 3000);
    }

    function esc(s) {
        if (!s) return '';
        return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    }

    // --- Filter existing visible records ---

    function applyFilterToExisting() {
        const filter = document.getElementById('filterInput').value.toLowerCase();
        document.querySelectorAll('.record').forEach(el => {
            let show = true;
            if (activeCategory && el.dataset.category !== activeCategory) show = false;
            if (show && filter && !el.dataset.searchable.includes(filter)) show = false;
            el.style.display = show ? '' : 'none';
        });
    }

    // --- Controls ---

    async function goToRecord(input) {
        if (!input) return;
        let atUri = input.trim();

        // Convert bsky.app URL to AT URI
        const bskyMatch = atUri.match(/bsky\\.app\\/profile\\/([^/]+)\\/post\\/([^/?#]+)/);
        if (bskyMatch) {
            let [, handleOrDid, rkey] = bskyMatch;
            if (!handleOrDid.startsWith('did:')) {
                try {
                    const res = await fetch('https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle=' + encodeURIComponent(handleOrDid));
                    const data = await res.json();
                    handleOrDid = data.did;
                } catch { showToast('Could not resolve handle'); return; }
            }
            atUri = 'at://' + handleOrDid + '/app.bsky.feed.post/' + rkey;
        }

        if (!atUri.startsWith('at://')) {
            showToast('Enter an at:// URI or bsky.app URL');
            return;
        }
        window.open('/feed/view?uri=' + encodeURIComponent(atUri), '_blank');
    }

    function clearFeed() {
        document.getElementById('feed').innerHTML = '<div class="empty-state" id="emptyState">Feed cleared.</div>';
        buffer = [];
        updateNewPill();
        updatePillCounts();
        if (connected) {
            connected = false;
            closeSockets();
            updateConnectBtn();
        }
    }

    // --- Load batch: connect, grab ~20 records, disconnect ---
    let batchCollections = null;
    async function loadBatch() {
        const btn = document.getElementById('loadMoreBtn');
        if (btn.classList.contains('loading')) return;
        btn.classList.add('loading');

        // Ensure we have metadata loaded
        if (Object.keys(lexiconMeta).length === 0) {
            try {
                const metaRes = await fetch('/api/v1/lexicon-meta');
                const metaData = await metaRes.json();
                for (const lex of metaData.lexicons) lexiconMeta[lex.nsid] = lex;
                buildCategoryPills();
            } catch { btn.classList.remove('loading'); return; }
        }

        if (!batchCollections) {
            try {
                const colRes = await fetch('/api/v1/feed/collections');
                const colData = await colRes.json();
                batchCollections = colData.collections;
            } catch { btn.classList.remove('loading'); return; }
        }

        // Open sockets, collect 20 events, then close
        const TARGET = 20;
        const batchBuffer = [];
        const batchSockets = [];

        await new Promise((resolve) => {
            const chunkSize = 100;
            let resolved = false;
            const timeout = setTimeout(() => { if (!resolved) { resolved = true; resolve(); } }, 8000);

            for (let i = 0; i < batchCollections.length; i += chunkSize) {
                const chunk = batchCollections.slice(i, i + chunkSize);
                const params = chunk.map(c => 'wantedCollections=' + encodeURIComponent(c)).join('&');
                const ws = new WebSocket(JETSTREAM_BASE + '?' + params);
                batchSockets.push(ws);

                ws.onmessage = (e) => {
                    try {
                        const msg = JSON.parse(e.data);
                        if (msg.kind !== 'commit' || !msg.commit || !msg.commit.collection || !msg.commit.record) return;
                        if (msg.commit.operation === 'delete') return;
                        batchBuffer.push(msg);
                        if (batchBuffer.length >= TARGET && !resolved) {
                            resolved = true;
                            clearTimeout(timeout);
                            resolve();
                        }
                    } catch {}
                };
            }
        });

        // Close all batch sockets
        batchSockets.forEach(ws => { try { ws.close(); } catch {} });

        // Render collected records
        if (batchBuffer.length > 0) {
            const feed = document.getElementById('feed');
            const empty = document.getElementById('emptyState');
            if (empty) empty.remove();

            const filter = document.getElementById('filterInput').value.toLowerCase();
            const fragment = document.createDocumentFragment();
            for (let i = batchBuffer.length - 1; i >= 0; i--) {
                const msg = batchBuffer[i];
                const nsid = msg.commit.collection;
                const meta = lexiconMeta[nsid] || {};
                const category = meta.category || 'other';
                if (activeCategory && category !== activeCategory) continue;
                if (filter) {
                    const searchable = nsid + ' ' + JSON.stringify(msg.commit.record);
                    if (!searchable.toLowerCase().includes(filter)) continue;
                }
                fragment.appendChild(makeRecord(msg));
            }
            feed.insertBefore(fragment, feed.firstChild);
            while (feed.children.length > MAX_VISIBLE) feed.removeChild(feed.lastChild);
        }

        btn.classList.remove('loading');
    }

    // Pre-load metadata so pills show immediately
    fetch('/api/v1/lexicon-meta').then(r => r.json()).then(data => {
        for (const lex of data.lexicons) lexiconMeta[lex.nsid] = lex;
        buildCategoryPills();
    }).catch(() => {});
    </script>
</body>
</html>
"""
