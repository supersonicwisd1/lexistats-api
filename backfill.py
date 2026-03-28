"""Backfill existing lexistats JSON samples into the database."""
import json
import os
import glob
import psycopg2
import psycopg2.extras

DB_DSN = os.environ.get("DATABASE_URL", "")

SAMPLES_DIR = "/opt/shared/repos/lexistats/data/samples"

def backfill():
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    files = sorted(glob.glob(os.path.join(SAMPLES_DIR, "sample_*.json")))
    print(f"Found {len(files)} sample files")

    # Check what's already loaded
    cur.execute("SELECT COUNT(*) FROM lexicon_samples")
    existing = cur.fetchone()[0]
    if existing > 0:
        print(f"DB already has {existing} samples, skipping backfill")
        conn.close()
        return

    for i, f in enumerate(files):
        with open(f) as fh:
            data = json.load(fh)

        ts = data["ts"]
        duration = data.get("duration_sec", 60)
        total = data["total"]
        eps = round(total / max(duration, 1), 2)

        cur.execute(
            "INSERT INTO lexicon_samples (ts, duration_sec, total_events, events_per_sec) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (ts, duration, total, eps)
        )
        sample_id = cur.fetchone()[0]

        rows = []
        for nsid, count in data["counts"].items():
            nsid_eps = round(count / max(duration, 1), 2)
            rows.append((sample_id, nsid, count, nsid_eps))

        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO lexicon_counts (sample_id, nsid, event_count, events_per_sec) VALUES %s",
            rows
        )

        if (i + 1) % 50 == 0:
            conn.commit()
            print(f"  {i+1}/{len(files)} loaded")

    conn.commit()
    print(f"Done. Loaded {len(files)} samples.")
    conn.close()

if __name__ == "__main__":
    backfill()
