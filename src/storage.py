"""DuckDB storage layer for the keyword data lake.

Handles schema creation, atomic upserts, Parquet export, and validation.
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from pathlib import Path

from .config import DB_PATH, LAKE_DIR
from .schema import init_all_tables, UNIFIED_KEYWORDS

SCHEMA_SQL = UNIFIED_KEYWORDS

ARROW_SCHEMA = pa.schema([
    ("id", pa.string()),
    ("label", pa.string()),
    ("definition", pa.string()),
    ("parent_id", pa.string()),
    ("source", pa.string()),
    ("type", pa.string()),
    ("uri", pa.string()),
    ("full_path", pa.string()),
    ("aliases", pa.list_(pa.string())),
    ("level", pa.int32()),
    ("cross_refs", pa.list_(pa.string())),
    ("last_updated", pa.timestamp("us", tz="UTC")),
    ("version", pa.string()),
])


class KeywordStore:
    def __init__(self, db_path: Path = DB_PATH):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self):
        init_all_tables(self.conn)

    def upsert(self, records: list[dict], source: str) -> dict:
        """Atomic upsert — delete all records for source, insert new batch.

        Returns stats dict with counts.
        """
        if not records:
            return {"inserted": 0, "source": source}

        now = datetime.now(timezone.utc)
        for r in records:
            r.setdefault("last_updated", now)
            r.setdefault("aliases", [])
            r.setdefault("cross_refs", [])
            r.setdefault("definition", None)
            r.setdefault("parent_id", None)
            r.setdefault("type", None)
            r.setdefault("uri", None)
            r.setdefault("full_path", None)
            r.setdefault("level", None)
            r.setdefault("version", None)
            r["source"] = source

        self.conn.execute("BEGIN TRANSACTION")
        try:
            old_count = self.conn.execute(
                "SELECT COUNT(*) FROM keywords WHERE source = ?", [source]
            ).fetchone()[0]

            self.conn.execute("DELETE FROM keywords WHERE source = ?", [source])

            self.conn.executemany(
                """INSERT INTO keywords
                   (id, label, definition, parent_id, source, type, uri,
                    full_path, aliases, level, cross_refs, last_updated, version)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        r["id"], r["label"], r["definition"], r["parent_id"],
                        r["source"], r["type"], r["uri"], r["full_path"],
                        r["aliases"], r["level"], r["cross_refs"],
                        r["last_updated"], r["version"],
                    )
                    for r in records
                ],
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

        return {
            "source": source,
            "inserted": len(records),
            "previous": old_count,
            "delta": len(records) - old_count,
        }

    def upsert_raw(self, table: str, records: list[dict], key_col: str) -> dict:
        """Atomic upsert into a raw source table — delete all then insert batch."""
        if not records:
            return {"table": table, "inserted": 0}

        self.conn.execute("BEGIN TRANSACTION")
        try:
            old_count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.conn.execute(f"DELETE FROM {table}")

            cols = self.conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position"
            ).fetchall()
            col_names = [c[0] for c in cols]

            for r in records:
                r.setdefault("ingested_at", datetime.now(timezone.utc))

            present_cols = [c for c in col_names if c in records[0]]
            placeholders = ", ".join(["?"] * len(present_cols))
            col_list = ", ".join(present_cols)

            self.conn.executemany(
                f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
                [tuple(r.get(c) for c in present_cols) for r in records],
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

        return {
            "table": table,
            "inserted": len(records),
            "previous": old_count,
        }

    def count(self, source: str = None) -> int:
        if source:
            return self.conn.execute(
                "SELECT COUNT(*) FROM keywords WHERE source = ?", [source]
            ).fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]

    def get_by_source(self, source: str) -> list[dict]:
        result = self.conn.execute(
            "SELECT * FROM keywords WHERE source = ? ORDER BY full_path, label",
            [source],
        ).fetchdf()
        return result.to_dict("records")

    def search(self, label_pattern: str, source: str = None) -> list[dict]:
        if source:
            result = self.conn.execute(
                "SELECT * FROM keywords WHERE label ILIKE ? AND source = ?",
                [f"%{label_pattern}%", source],
            ).fetchdf()
        else:
            result = self.conn.execute(
                "SELECT * FROM keywords WHERE label ILIKE ?",
                [f"%{label_pattern}%"],
            ).fetchdf()
        return result.to_dict("records")

    def export_parquet(self, source: str = None, output_dir: Path = LAKE_DIR):
        output_dir.mkdir(parents=True, exist_ok=True)
        if source:
            filename = output_dir / f"{source}.parquet"
            self.conn.execute(
                f"COPY (SELECT * FROM keywords WHERE source = ?) TO '{filename}' (FORMAT PARQUET, COMPRESSION SNAPPY)",
                [source],
            )
            return filename
        else:
            filename = output_dir / "all_keywords.parquet"
            self.conn.execute(
                f"COPY (SELECT * FROM keywords) TO '{filename}' (FORMAT PARQUET, COMPRESSION SNAPPY)",
            )
            return filename

    def validate(self, source: str) -> dict:
        """Run validation checks on ingested data for a source."""
        issues = []
        count = self.count(source)
        if count == 0:
            issues.append(f"No records for source '{source}'")
            return {"source": source, "count": count, "valid": False, "issues": issues}

        orphans = self.conn.execute(
            """SELECT COUNT(*) FROM keywords k
               WHERE k.source = ? AND k.parent_id IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM keywords p
                   WHERE p.id = k.parent_id AND p.source = k.source
               )""",
            [source],
        ).fetchone()[0]
        if orphans > 0:
            issues.append(f"{orphans} orphan records (parent_id references missing parent)")

        empty_labels = self.conn.execute(
            "SELECT COUNT(*) FROM keywords WHERE source = ? AND (label IS NULL OR label = '')",
            [source],
        ).fetchone()[0]
        if empty_labels > 0:
            issues.append(f"{empty_labels} records with empty labels")

        dupes = self.conn.execute(
            "SELECT COUNT(*) - COUNT(DISTINCT id) FROM keywords WHERE source = ?",
            [source],
        ).fetchone()[0]
        if dupes > 0:
            issues.append(f"{dupes} duplicate IDs")

        return {
            "source": source,
            "count": count,
            "orphans": orphans,
            "empty_labels": empty_labels,
            "duplicates": dupes,
            "valid": len(issues) == 0,
            "issues": issues,
        }

    def stats(self) -> dict:
        sources = self.conn.execute(
            "SELECT source, COUNT(*) as cnt FROM keywords GROUP BY source ORDER BY cnt DESC"
        ).fetchdf()
        return {
            "total": self.count(),
            "by_source": dict(zip(sources["source"], sources["cnt"])),
        }

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
