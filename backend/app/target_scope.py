"""
Resolve target IDs by scope and apply batch enable/disable with PingManager sync.
"""

from __future__ import annotations

import logging
import time
from typing import List, Optional

import aiosqlite

from .state import ping_manager

logger = logging.getLogger(__name__)

SCOPE_ALL = "all"
SCOPE_GROUP = "group"
SCOPE_TAG = "tag"
SCOPE_IDS = "ids"
SCOPE_FILTERED = "filtered"


async def record_target_event(db_path: str, target_id: int, action: str, ts: int | None = None) -> None:
    """Append an enable/disable event to target_events (the chart uses this to
    grey out every paused period)."""
    if ts is None:
        ts = int(time.time())
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            "INSERT INTO target_events (target_id, action, ts) VALUES (?,?,?)",
            (target_id, action, ts),
        )
        await db.commit()


async def fetch_all_targets(db: aiosqlite.Connection) -> List[dict]:
    db.row_factory = aiosqlite.Row
    async with db.execute("SELECT * FROM targets ORDER BY id") as cur:
        return [dict(r) for r in await cur.fetchall()]


def _csv_set(value: str) -> set[str]:
    return {x.strip() for x in (value or "").split(",") if x.strip()}


def _tag_matches(tags_str: str, tag: str) -> bool:
    if not tag:
        return False
    parts = [x.strip() for x in (tags_str or "").split(",")]
    return tag in parts


def _tags_match_any(tags_str: str, wanted: set[str]) -> bool:
    if not wanted:
        return False
    return any(_tag_matches(tags_str, t) for t in wanted)


async def resolve_target_ids(
    db_path: str,
    scope_type: str,
    scope_value: str = "",
    *,
    filter_group: str = "",
    filter_tag: str = "",
    filter_search: str = "",
) -> List[int]:
    async with aiosqlite.connect(db_path) as db:
        rows = await fetch_all_targets(db)

    scope_type = (scope_type or SCOPE_ALL).lower()
    scope_value = (scope_value or "").strip()
    filter_search = (filter_search or "").strip().lower()

    if scope_type == SCOPE_ALL:
        return [r["id"] for r in rows]

    if scope_type == SCOPE_GROUP:
        groups = _csv_set(scope_value)
        if not groups:
            return []
        return [r["id"] for r in rows if r.get("group_name") in groups]

    if scope_type == SCOPE_TAG:
        tags = _csv_set(scope_value)
        if not tags:
            return []
        return [r["id"] for r in rows if _tags_match_any(r.get("tags", ""), tags)]

    if scope_type == SCOPE_IDS:
        ids = []
        for part in scope_value.replace(" ", "").split(","):
            if part.isdigit():
                ids.append(int(part))
        valid = {r["id"] for r in rows}
        return [i for i in ids if i in valid]

    if scope_type == SCOPE_FILTERED:
        # Multi-select: filter_group / filter_tag may be comma-separated lists
        # (OR semantics, matching the frontend multi-select filter bar).
        groups = _csv_set(filter_group)
        tags   = _csv_set(filter_tag)
        out = []
        for r in rows:
            if groups and r.get("group_name") not in groups:
                continue
            if tags and not _tags_match_any(r.get("tags", ""), tags):
                continue
            if filter_search:
                name = (r.get("name") or "").lower()
                addr = (r.get("address") or "").lower()
                if filter_search not in name and filter_search not in addr:
                    continue
            out.append(r["id"])
        return out

    return []


async def batch_set_enabled(
    db_path: str,
    target_ids: List[int],
    enabled: bool,
) -> dict:
    """Update enabled flag for target_ids and sync ping tasks."""
    if not target_ids:
        return {"updated": 0, "enabled": enabled}

    val = 1 if enabled else 0
    changed: list[dict] = []
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        placeholders = ",".join("?" * len(target_ids))
        now = int(time.time())
        # Only targets whose state actually flips get a timestamp + event entry,
        # so re-applying the same state does not create spurious events.
        async with db.execute(
            f"SELECT id, enabled FROM targets WHERE id IN ({placeholders})",
            target_ids,
        ) as cur:
            before = {r["id"]: bool(r["enabled"]) for r in await cur.fetchall()}
        for tid in target_ids:
            if before.get(tid) != enabled:
                changed.append(tid)

        ts_col = "enabled_at" if enabled else "disabled_at"
        await db.execute(
            f"UPDATE targets SET enabled=?, {ts_col}=? WHERE id IN ({placeholders})",
            (val, now, *target_ids),
        )
        await db.commit()
        async with db.execute(
            f"SELECT id, address, interval_ms, probe_type, port, enabled FROM targets WHERE id IN ({placeholders})",
            target_ids,
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]

    # Record events only for targets whose state actually changed
    for tid in changed:
        await record_target_event(db_path, tid, "enable" if enabled else "disable", int(time.time()))

    for r in rows:
        if r["enabled"]:
            ping_manager.add_target(
                r["id"], r["address"], r["interval_ms"],
                r.get("probe_type") or "icmp", r.get("port"),
            )
        else:
            ping_manager.remove_target(r["id"])

    logger.info("Batch %s: %d targets", "enable" if enabled else "disable", len(rows))
    return {"updated": len(rows), "enabled": enabled, "target_ids": target_ids}
