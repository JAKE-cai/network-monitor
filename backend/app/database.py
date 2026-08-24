import logging
import os
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "/data/monitor.db")

INIT_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-32000;
PRAGMA temp_store=MEMORY;
PRAGMA mmap_size=268435456;

CREATE TABLE IF NOT EXISTS targets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    address     TEXT    NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    interval_ms INTEGER NOT NULL DEFAULT 1000,
    created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS ping_results (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id  INTEGER NOT NULL,
    ts         INTEGER NOT NULL,
    latency_ms REAL,
    is_loss    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pr_target_ts ON ping_results(target_id, ts);
CREATE INDEX IF NOT EXISTS idx_pr_ts        ON ping_results(ts);

CREATE TABLE IF NOT EXISTS ping_summary (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id     INTEGER NOT NULL,
    bucket_ts     INTEGER NOT NULL,
    cnt_1_30      INTEGER NOT NULL DEFAULT 0,
    cnt_31_60     INTEGER NOT NULL DEFAULT 0,
    cnt_61_100    INTEGER NOT NULL DEFAULT 0,
    cnt_101_200   INTEGER NOT NULL DEFAULT 0,
    cnt_201_500   INTEGER NOT NULL DEFAULT 0,
    cnt_501_1000  INTEGER NOT NULL DEFAULT 0,
    cnt_over_1000 INTEGER NOT NULL DEFAULT 0,
    cnt_loss      INTEGER NOT NULL DEFAULT 0,
    cnt_total     INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_target_bucket ON ping_summary(target_id, bucket_ts);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);

CREATE TABLE IF NOT EXISTS target_schedules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    scope_type  TEXT    NOT NULL,
    scope_value TEXT    NOT NULL DEFAULT '',
    action      TEXT    NOT NULL,
    time_hhmm   TEXT    NOT NULL,
    weekdays    TEXT    NOT NULL DEFAULT '0,1,2,3,4,5,6',
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
"""


_MIGRATIONS = [
    "ALTER TABLE targets ADD COLUMN group_name TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE targets ADD COLUMN tags       TEXT NOT NULL DEFAULT ''",
    """CREATE TABLE IF NOT EXISTS target_schedules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    scope_type  TEXT    NOT NULL,
    scope_value TEXT    NOT NULL DEFAULT '',
    action      TEXT    NOT NULL,
    time_hhmm   TEXT    NOT NULL,
    weekdays    TEXT    NOT NULL DEFAULT '0,1,2,3,4,5,6',
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
)""",
    """CREATE TABLE IF NOT EXISTS sessions (
    token      TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    expires_at INTEGER NOT NULL
)""",
    "CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)",
    # Last disable/enable timestamps (Unix seconds) – used to mark "paused"
    # periods on the latency chart instead of showing them as packet loss.
    "ALTER TABLE targets ADD COLUMN disabled_at INTEGER",     # NULL or last disable time
    "ALTER TABLE targets ADD COLUMN enabled_at  INTEGER",     # NULL or last enable time
    # Full enable/disable event history so the chart can grey out EVERY paused
    # period (not just the most recent one).
    """CREATE TABLE IF NOT EXISTS target_events (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        target_id  INTEGER NOT NULL,
        action     TEXT    NOT NULL,     -- 'enable' | 'disable'
        ts         INTEGER NOT NULL,
        created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
    )""",
    "CREATE INDEX IF NOT EXISTS idx_te_target_ts ON target_events(target_id, ts)",
]


async def init_db() -> None:
    Path("/data").mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(INIT_SQL)
        for sql in _MIGRATIONS:
            try:
                await db.execute(sql)
            except Exception as exc:
                # "duplicate column name" is expected when re-running migrations
                msg = str(exc).lower()
                if "duplicate column" in msg or "already exists" in msg:
                    logger.debug("Migration already applied, skipping: %s", sql[:60])
                else:
                    logger.warning("Migration warning (%s): %s", exc, sql[:80])
        await db.commit()
    logger.info("Database initialised at %s", DB_PATH)
