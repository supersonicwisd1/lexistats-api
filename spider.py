#!/usr/bin/env python3
"""
Spider lexicon schemas from the ATProto network and categorize them.

Data sources (in priority order):
1. Lexicon Garden XRPC — formal schema definitions with descriptions
2. DID → PDS → listRecords — example records for schema shape inference
3. NSID pattern matching — fallback categorization

Usage:
    python spider.py                # spider all unknown lexicons
    python spider.py --refresh      # re-spider everything
    python spider.py --categorize   # just re-run categorization on existing data
    python spider.py --stats        # just print stats, no changes
"""

import argparse
import json
import os
import re
import sys
import time
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import psycopg2
import psycopg2.extras

DB_DSN = os.environ.get("DATABASE_URL", "")

LEXICON_GARDEN_XRPC = "https://lexicon.garden/xrpc/com.atproto.lexicon.resolveLexicon"
LEXICON_GARDEN_PAGE = "https://lexicon.garden/lexicon"  # {did}/{nsid}

OFFICIAL_AUTHORITIES = {"app.bsky", "chat.bsky", "com.atproto", "tools.ozone"}

# --- NSID-based category rules ---
# Order matters: first match wins

CATEGORY_RULES = [
    # Official Bluesky
    (r"^app\.bsky\.feed\.", "social", ["feed", "content", "bluesky"]),
    (r"^app\.bsky\.actor\.", "identity", ["profile", "bluesky"]),
    (r"^app\.bsky\.graph\.", "social-graph", ["follow", "block", "bluesky"]),
    (r"^app\.bsky\.notification\.", "notification", ["bluesky"]),
    (r"^app\.bsky\.embed\.", "media", ["embed", "bluesky"]),
    (r"^app\.bsky\.richtext\.", "content", ["richtext", "bluesky"]),
    (r"^app\.bsky\.labeler\.", "moderation", ["labels", "bluesky"]),
    (r"^chat\.bsky\.", "messaging", ["chat", "bluesky"]),
    (r"^com\.atproto\.repo\.", "protocol", ["repo", "data"]),
    (r"^com\.atproto\.sync\.", "protocol", ["sync"]),
    (r"^com\.atproto\.server\.", "protocol", ["server"]),
    (r"^com\.atproto\.identity\.", "identity", ["did", "handle"]),
    (r"^com\.atproto\.label\.", "moderation", ["labels"]),
    (r"^com\.atproto\.admin\.", "admin", ["moderation"]),
    (r"^tools\.ozone\.", "moderation", ["ozone", "tooling"]),

    # Third-party patterns
    (r"\.feed\.", "social", ["feed"]),
    (r"\.post$", "social", ["content"]),
    (r"\.like$", "social", ["engagement"]),
    (r"\.follow$", "social-graph", ["follow"]),
    (r"\.profile$", "identity", ["profile"]),
    (r"\.status$", "status", ["presence"]),
    (r"\.chat\.", "messaging", ["chat"]),
    (r"\.message$", "messaging", []),
    (r"\.game\.", "gaming", ["game"]),
    (r"\.blog\.", "publishing", ["blog"]),
    (r"\.article$", "publishing", ["long-form"]),
    (r"\.document$", "publishing", ["document"]),
    (r"\.comment$", "social", ["comment"]),
    (r"\.vote$", "social", ["engagement"]),
    (r"\.rating$", "reputation", ["rating"]),
    (r"\.review$", "reputation", ["review"]),
    (r"\.claim", "reputation", ["claim", "attestation", "verification", "endorsement"]),
    (r"\.credential$", "identity", ["credential"]),
    (r"\.bookmark$", "curation", ["bookmark"]),
    (r"\.list$", "curation", ["list"]),
    (r"\.tag$", "curation", ["tag"]),
    (r"\.media\.", "media", []),
    (r"\.image$", "media", ["image"]),
    (r"\.video$", "media", ["video"]),
    (r"\.audio$", "media", ["audio"]),
    (r"\.event$", "events", ["event"]),
    (r"\.calendar\.", "events", ["calendar"]),
    (r"\.sensor\.", "iot", ["sensor", "data"]),
    (r"\.observation", "iot", ["sensor", "data"]),
    (r"\.broadcast\.", "streaming", ["broadcast"]),
    (r"\.stream\.", "streaming", []),
    (r"\.tip$", "commerce", ["tipping"]),
    (r"\.payment$", "commerce", ["payment"]),
    (r"\.poll$", "social", ["poll", "engagement"]),
    (r"\.scrobble$", "social", ["music", "scrobble"]),
    (r"\.play$", "social", ["music", "play"]),
    (r"\.streak$", "gaming", ["streak"]),
    (r"\.inventory$", "gaming", ["inventory"]),
    (r"\.session$", "gaming", ["session"]),
    (r"\.log\.", "gaming", ["log"]),
    (r"\.podping$", "podcasts", ["podcast"]),
    (r"\.publication$", "publishing", ["document"]),
]

# Description-based category overrides (applied after schema fetch)
DESCRIPTION_CATEGORIES = [
    (r"music|track|album|artist|scrobbl", "music", ["music"]),
    (r"podcast|podping|feed.*rss", "podcasts", ["podcast"]),
    (r"stream|broadcast|live|viewer", "streaming", ["live"]),
    (r"game|inventory|session|quest|expedition", "gaming", ["game"]),
    (r"claim|attestation|trust|verification|attest", "reputation", ["claim", "attestation"]),
    (r"blog|article|document|publish", "publishing", ["content"]),
    (r"chat|message|conversation", "messaging", ["chat"]),
    (r"sensor|observation|iot|temperature|humidity", "iot", ["sensor"]),
]


def get_authority(nsid):
    parts = nsid.split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else nsid


def authority_to_domain(authority):
    parts = authority.split(".")
    return ".".join(reversed(parts))


def categorize_nsid(nsid, description=None, schema_type=None):
    """Auto-categorize an NSID based on patterns and description."""
    # Try NSID pattern first
    for pattern, category, tags in CATEGORY_RULES:
        if re.search(pattern, nsid):
            result_tags = list(tags)
            if schema_type and schema_type not in result_tags:
                result_tags.append(schema_type)
            return category, result_tags

    # Try description-based categorization
    if description:
        desc_lower = description.lower()
        for pattern, category, tags in DESCRIPTION_CATEGORIES:
            if re.search(pattern, desc_lower):
                result_tags = list(tags)
                if schema_type and schema_type not in result_tags:
                    result_tags.append(schema_type)
                return category, result_tags

    tags = []
    if schema_type:
        tags.append(schema_type)
    return "other", tags


def http_get_json(url, timeout=10):
    """GET a URL and parse JSON response."""
    req = Request(url, headers={"User-Agent": "lexistats-spider/1.0 (linkedtrust.us)"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


# --- Lexicon Garden ---

def fetch_from_lexicon_garden(nsid):
    """Fetch formal schema from Lexicon Garden XRPC endpoint."""
    try:
        url = f"{LEXICON_GARDEN_XRPC}?nsid={nsid}"
        data = http_get_json(url)
        schema = data.get("schema", {})
        uri = data.get("uri", "")
        # Extract DID from uri for page link: at://did:xxx/collection/rkey
        did = uri.split("/")[2] if uri.startswith("at://") and len(uri.split("/")) > 2 else None
        page_url = f"{LEXICON_GARDEN_PAGE}/{did}/{nsid}" if did else None

        main_def = schema.get("defs", {}).get("main", {})
        description = main_def.get("description") or schema.get("description", "")
        schema_type = main_def.get("type", "")

        return {
            "schema": schema,
            "description": description,
            "schema_type": schema_type,
            "lexicon_url": page_url,
            "source": "lexicon_garden",
        }
    except (HTTPError, URLError):
        return None
    except Exception as e:
        print(f"    warning: {e}")
        return None


# --- DID → PDS → example record ---

def resolve_did_doc(did):
    """Resolve a DID document."""
    try:
        if did.startswith("did:plc:"):
            url = f"https://plc.directory/{did}"
        elif did.startswith("did:web:"):
            domain = did.replace("did:web:", "")
            url = f"https://{domain}/.well-known/did.json"
        else:
            return None
        return http_get_json(url)
    except Exception:
        return None


def get_pds_url(did_doc):
    """Extract PDS endpoint from a DID document."""
    for svc in did_doc.get("service", []):
        if svc.get("type") == "AtprotoPersonalDataServer":
            return svc.get("serviceEndpoint")
    return None


def fetch_example_record(did, nsid, pds_url):
    """Fetch one example record from a PDS."""
    try:
        url = f"{pds_url}/xrpc/com.atproto.repo.listRecords?repo={did}&collection={nsid}&limit=1"
        data = http_get_json(url)
        records = data.get("records", [])
        if records:
            return records[0].get("value")
    except Exception:
        pass
    return None


def infer_schema_from_record(record):
    """Infer field types from an example record value."""
    if not isinstance(record, dict):
        return None
    fields = {}
    for key, val in record.items():
        if key == "$type":
            continue
        if isinstance(val, str):
            if re.match(r"^\d{4}-\d{2}-\d{2}T", val):
                fields[key] = "datetime"
            elif val.startswith("did:"):
                fields[key] = "did"
            elif val.startswith("at://"):
                fields[key] = "at-uri"
            elif val.startswith("http"):
                fields[key] = "uri"
            else:
                fields[key] = "string"
        elif isinstance(val, bool):
            fields[key] = "boolean"
        elif isinstance(val, int):
            fields[key] = "integer"
        elif isinstance(val, float):
            fields[key] = "number"
        elif isinstance(val, list):
            if val and isinstance(val[0], str):
                fields[key] = "string[]"
            elif val and isinstance(val[0], dict):
                fields[key] = "object[]"
            else:
                fields[key] = "array"
        elif isinstance(val, dict):
            fields[key] = "object"
    return fields


def fetch_example_for_nsid(nsid, conn):
    """Try to get an example record using DIDs we've collected."""
    cur = conn.cursor()
    # Prefer did:plc: DIDs (they resolve via plc.directory)
    cur.execute(
        "SELECT did FROM lexicon_unique_dids WHERE nsid = %s AND did LIKE 'did:plc:%%' LIMIT 3",
        (nsid,)
    )
    dids = [r[0] for r in cur.fetchall()]

    # Also try did:web: if no plc DIDs
    if not dids:
        cur.execute(
            "SELECT did FROM lexicon_unique_dids WHERE nsid = %s LIMIT 3",
            (nsid,)
        )
        dids = [r[0] for r in cur.fetchall()]

    for did in dids:
        did_doc = resolve_did_doc(did)
        if not did_doc:
            continue
        pds_url = get_pds_url(did_doc)
        if not pds_url:
            continue
        record = fetch_example_record(did, nsid, pds_url)
        if record:
            return record
        time.sleep(0.3)

    return None


# --- GitHub link generation ---

def get_github_url(nsid):
    """Generate GitHub URL for official Bluesky lexicons."""
    authority = get_authority(nsid)
    if authority in OFFICIAL_AUTHORITIES:
        path = nsid.replace(".", "/")
        return f"https://github.com/bluesky-social/atproto/blob/main/lexicons/{path}.json"
    return None


# --- Main spider logic ---

def populate_from_samples(conn):
    """Populate lexicon_meta from existing sample data."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT c.nsid,
               SUM(c.event_count) as total_events,
               MIN(s.ts) as first_seen,
               MAX(s.ts) as last_seen
        FROM lexicon_counts c
        JOIN lexicon_samples s ON s.id = c.sample_id
        GROUP BY c.nsid
    """)
    nsids = cur.fetchall()

    cur.execute("""
        SELECT nsid, COUNT(*) as unique_users
        FROM lexicon_unique_dids
        WHERE last_seen > now() - interval '7 days'
        GROUP BY nsid
    """)
    uniques = {r["nsid"]: r["unique_users"] for r in cur.fetchall()}

    inserted = 0
    for row in nsids:
        nsid = row["nsid"]
        authority = get_authority(nsid)
        domain = authority_to_domain(authority)
        category, tags = categorize_nsid(nsid)

        cur.execute("""
            INSERT INTO lexicon_meta (nsid, authority, domain, category, tags,
                                      first_seen, last_seen, total_events, unique_users_7d)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (nsid) DO UPDATE SET
                total_events = EXCLUDED.total_events,
                first_seen = LEAST(lexicon_meta.first_seen, EXCLUDED.first_seen),
                last_seen = GREATEST(lexicon_meta.last_seen, EXCLUDED.last_seen),
                unique_users_7d = EXCLUDED.unique_users_7d,
                updated_at = now()
        """, (nsid, authority, domain, category, tags,
              row["first_seen"], row["last_seen"],
              int(row["total_events"]), uniques.get(nsid, 0)))
        inserted += 1

    conn.commit()
    print(f"Populated {inserted} lexicon entries from sample data")


def spider_schemas(conn, refresh=False):
    """Spider lexicon schemas using Lexicon Garden + DID→PDS fallback."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if refresh:
        cur.execute("SELECT nsid, authority, domain FROM lexicon_meta ORDER BY total_events DESC NULLS LAST")
    else:
        cur.execute("""
            SELECT nsid, authority, domain FROM lexicon_meta
            WHERE spidered_at IS NULL
            ORDER BY total_events DESC NULLS LAST
        """)

    rows = cur.fetchall()
    print(f"Spidering {len(rows)} lexicons...")

    garden_hits = 0
    pds_hits = 0
    misses = 0

    for i, row in enumerate(rows):
        nsid = row["nsid"]
        authority = row["authority"]
        print(f"  [{i+1}/{len(rows)}] {nsid}...", end=" ", flush=True)

        schema_data = None
        example = None
        lexicon_url = None
        description = None
        schema_type = None
        schema_json = None

        # Source 1: Lexicon Garden XRPC
        garden = fetch_from_lexicon_garden(nsid)
        if garden:
            schema_json = garden["schema"]
            description = garden["description"]
            schema_type = garden["schema_type"]
            lexicon_url = garden["lexicon_url"]
            garden_hits += 1
            print(f"garden ✓", end="", flush=True)
        else:
            # Source 2: Official GitHub
            github_url = get_github_url(nsid)
            if github_url:
                lexicon_url = github_url
                print(f"github", end="", flush=True)

        # Source 3: DID→PDS for example record (if no schema from garden, or always for third-party)
        if not schema_json or authority not in OFFICIAL_AUTHORITIES:
            example = fetch_example_for_nsid(nsid, conn)
            if example:
                pds_hits += 1
                print(f" + example ✓", end="", flush=True)

                # If no schema from garden, infer from example
                if not schema_json:
                    inferred = infer_schema_from_record(example)
                    if inferred:
                        schema_json = {"inferred_fields": inferred, "_source": "example_record"}
                    if not schema_type:
                        schema_type = "record"

        if not schema_json and not example:
            misses += 1
            print(f"miss", end="", flush=True)

        # If no lexicon_url yet, try Lexicon Garden page via autocomplete
        if not lexicon_url and authority not in OFFICIAL_AUTHORITIES:
            lexicon_url = f"https://lexidex.bsky.dev/lexicon/{nsid}"

        # Re-categorize with description info
        category, tags = categorize_nsid(nsid, description, schema_type)

        cur.execute("""
            UPDATE lexicon_meta SET
                schema_json = COALESCE(%s, schema_json),
                record_example = COALESCE(%s, record_example),
                description = COALESCE(%s, description),
                schema_type = COALESCE(%s, schema_type),
                lexicon_url = COALESCE(%s, lexicon_url),
                category = %s,
                tags = %s,
                spidered_at = now(),
                updated_at = now()
            WHERE nsid = %s
        """, (
            json.dumps(schema_json) if schema_json else None,
            json.dumps(example) if example else None,
            description or None,
            schema_type or None,
            lexicon_url,
            category,
            tags,
            nsid
        ))
        conn.commit()
        print()
        time.sleep(1.0)  # Rate limit (Lexicon Garden has aggressive 429s)

    print(f"\nDone: {garden_hits} from Lexicon Garden, {pds_hits} example records, {misses} misses")


def recategorize(conn):
    """Re-run categorization on all entries using stored descriptions."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT nsid, schema_json, description, schema_type FROM lexicon_meta")
    rows = cur.fetchall()

    for row in rows:
        nsid = row["nsid"]
        description = row.get("description")
        schema_type = row.get("schema_type")

        # Get schema_type from schema if not stored
        schema = row.get("schema_json")
        if not schema_type and schema and isinstance(schema, dict):
            main_def = schema.get("defs", {}).get("main", schema)
            schema_type = main_def.get("type", "")

        category, tags = categorize_nsid(nsid, description, schema_type)

        cur.execute("""
            UPDATE lexicon_meta SET category = %s, tags = %s, schema_type = %s, updated_at = now()
            WHERE nsid = %s
        """, (category, tags, schema_type or None, nsid))

    conn.commit()
    print(f"Re-categorized {len(rows)} entries")


def print_stats(conn):
    """Print summary statistics."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT COUNT(*) as total FROM lexicon_meta")
    print(f"Total lexicons: {cur.fetchone()['total']}")

    cur.execute("SELECT COUNT(*) as ct FROM lexicon_meta WHERE schema_json IS NOT NULL")
    print(f"With schema: {cur.fetchone()['ct']}")

    cur.execute("SELECT COUNT(*) as ct FROM lexicon_meta WHERE record_example IS NOT NULL")
    print(f"With example: {cur.fetchone()['ct']}")

    cur.execute("SELECT COUNT(*) as ct FROM lexicon_meta WHERE description IS NOT NULL AND description != ''")
    print(f"With description: {cur.fetchone()['ct']}")

    cur.execute("SELECT COUNT(*) as ct FROM lexicon_meta WHERE lexicon_url IS NOT NULL")
    print(f"With URL: {cur.fetchone()['ct']}")

    cur.execute("""
        SELECT category, COUNT(*) as count
        FROM lexicon_meta
        GROUP BY category
        ORDER BY count DESC
    """)
    print("\nCategories:")
    for row in cur.fetchall():
        print(f"  {row['category']}: {row['count']}")

    # Show third-party with descriptions
    cur.execute("""
        SELECT nsid, description, category, schema_type
        FROM lexicon_meta
        WHERE authority NOT IN ('app.bsky', 'chat.bsky', 'com.atproto', 'tools.ozone')
          AND description IS NOT NULL AND description != ''
        ORDER BY unique_users_7d DESC NULLS LAST
        LIMIT 20
    """)
    rows = cur.fetchall()
    if rows:
        print("\nThird-party with descriptions:")
        for r in rows:
            desc = (r['description'] or '')[:60]
            print(f"  {r['nsid']}: [{r['category']}] {desc}")


def main():
    parser = argparse.ArgumentParser(description="Spider and categorize ATProto lexicons")
    parser.add_argument("--refresh", action="store_true", help="Re-spider all lexicons")
    parser.add_argument("--categorize", action="store_true", help="Just re-run categorization")
    parser.add_argument("--stats", action="store_true", help="Just print stats")
    parser.add_argument("--no-spider", action="store_true", help="Skip schema resolution")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_DSN)

    if args.stats:
        print_stats(conn)
        conn.close()
        return

    # Always populate/update from sample data first
    populate_from_samples(conn)

    if args.categorize:
        recategorize(conn)
    elif not args.no_spider:
        spider_schemas(conn, refresh=args.refresh)
    else:
        recategorize(conn)

    print_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
