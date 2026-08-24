import asyncio
import json
import time
from typing import Optional

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from ..auth import make_sse_auth_dependency
from ..compressor import RETENTION_SECONDS, compress_old_data
from ..database import DB_PATH
from ..pubsub import sub_manager

_sse_auth = make_sse_auth_dependency(lambda: DB_PATH)

router = APIRouter(prefix="/api/metrics", tags=["metrics"])

SEVEN_DAYS = RETENTION_SECONDS   # alias for clarity


def _auto_bucket(duration: int) -> int:
    """Choose a bucket size (seconds) that keeps roughly ≤720 data points."""
    for threshold, bucket in [
        (7_200,   10),     # ≤ 2 h  → 10 s buckets
        (43_200,  60),     # ≤ 12 h → 1 min
        (259_200, 600),    # ≤ 3 d  → 10 min
        (604_800, 1_800),  # ≤ 7 d  → 30 min
    ]:
        if duration <= threshold:
            return bucket
    return 3_600


# ------------------------------------------------------------------ #
# Batch status (all targets in one query – for dashboard polling)
# ------------------------------------------------------------------ #

@router.get("/all-status")
async def get_all_status():
    now = int(time.time())
    since = now - 60
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT
                target_id,
                COUNT(*)  AS total,
                SUM(is_loss) AS loss,
                AVG(CASE WHEN is_loss=0 THEN latency_ms END) AS avg_ms,
                MIN(CASE WHEN is_loss=0 THEN latency_ms END) AS min_ms,
                MAX(CASE WHEN is_loss=0 THEN latency_ms END) AS max_ms
            FROM ping_results
            WHERE ts >= ?
            GROUP BY target_id
            """,
            (since,),
        ) as cur:
            rows = await cur.fetchall()

    result: dict = {}
    for r in rows:
        total = r["total"] or 0
        loss  = r["loss"]  or 0
        result[str(r["target_id"])] = {
            "total":       total,
            "loss":        loss,
            "loss_rate":   round(loss / total * 100, 2) if total else 0,
            "avg_latency": round(r["avg_ms"], 2) if r["avg_ms"] is not None else None,
            "min_latency": round(r["min_ms"], 2) if r["min_ms"] is not None else None,
            "max_latency": round(r["max_ms"], 2) if r["max_ms"] is not None else None,
        }
    return result


# ------------------------------------------------------------------ #
# Single-target current status
# ------------------------------------------------------------------ #

@router.get("/{target_id}/status")
async def get_status(target_id: int):
    now = int(time.time())
    since = now - 60
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT COUNT(*) AS total, SUM(is_loss) AS loss,
                   AVG(CASE WHEN is_loss=0 THEN latency_ms END) AS avg_ms,
                   MIN(CASE WHEN is_loss=0 THEN latency_ms END) AS min_ms,
                   MAX(CASE WHEN is_loss=0 THEN latency_ms END) AS max_ms
            FROM ping_results
            WHERE target_id=? AND ts>=?
            """,
            (target_id, since),
        ) as cur:
            r = dict(await cur.fetchone())

    total = r["total"] or 0
    loss  = r["loss"]  or 0
    return {
        "total":       total,
        "loss":        loss,
        "loss_rate":   round(loss / total * 100, 2) if total else 0,
        "avg_latency": round(r["avg_ms"], 2) if r["avg_ms"] is not None else None,
        "min_latency": round(r["min_ms"], 2) if r["min_ms"] is not None else None,
        "max_latency": round(r["max_ms"], 2) if r["max_ms"] is not None else None,
    }


# ------------------------------------------------------------------ #
# Chart data (time-series, last 7 days only)
# ------------------------------------------------------------------ #

@router.get("/{target_id}/chart")
async def get_chart(
    target_id: int,
    start: int = Query(..., description="Unix timestamp"),
    end:   int = Query(..., description="Unix timestamp"),
):
    now = int(time.time())
    cutoff = now - SEVEN_DAYS

    # Clamp to the 7-day window; callers should use /summary for older data
    raw_start = max(start, cutoff)
    raw_end   = min(end, now)

    if raw_end <= raw_start:
        return []

    bucket = _auto_bucket(raw_end - raw_start)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT
                (ts / ?) * ?  AS ts,
                AVG(CASE WHEN is_loss=0 THEN latency_ms END) AS avg_ms,
                MIN(CASE WHEN is_loss=0 THEN latency_ms END) AS min_ms,
                MAX(CASE WHEN is_loss=0 THEN latency_ms END) AS max_ms,
                COUNT(*)                                      AS total,
                COALESCE(SUM(is_loss), 0)                     AS loss
            FROM ping_results
            WHERE target_id=? AND ts BETWEEN ? AND ?
            GROUP BY ts / ?
            ORDER BY ts
            """,
            (bucket, bucket, target_id, raw_start, raw_end, bucket),
        ) as cur:
            rows = await cur.fetchall()

    return [
        {
            "ts":    r["ts"],
            "avg":   round(r["avg_ms"], 2) if r["avg_ms"] is not None else None,
            "min":   round(r["min_ms"], 2) if r["min_ms"] is not None else None,
            "max":   round(r["max_ms"], 2) if r["max_ms"] is not None else None,
            "total": r["total"],
            "loss":  r["loss"],
        }
        for r in rows
    ]


@router.get("/{target_id}/paused-at")
async def get_paused_at(target_id: int):
    """Return the target's enable/disable status plus EVERY paused interval
    (derived from target_events), so the chart can grey out all paused periods
    instead of showing them as red packet loss."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT enabled, enabled_at, disabled_at FROM targets WHERE id=?",
            (target_id,),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Target not found")

        async with db.execute(
            "SELECT action, ts FROM target_events WHERE target_id=? ORDER BY ts ASC, id ASC",
            (target_id,),
        ) as cur:
            events = [(r["action"], r["ts"]) for r in await cur.fetchall()]

    enabled_now = bool(row["enabled"])

    # Reconstruct paused intervals from the event timeline.
    # A 'disable' starts a paused run; the next 'enable' ends it. If the target
    # is currently disabled and the last event was a disable, extend to now.
    paused: list[dict] = []
    pause_start: int | None = None
    last_action: str | None = None
    for action, ts in events:
        if action == "disable" and pause_start is None:
            pause_start = ts
        elif action == "enable" and pause_start is not None:
            paused.append({"start": pause_start, "end": ts})
            pause_start = None
        last_action = action

    if pause_start is not None:
        # still paused up to now
        paused.append({"start": pause_start, "end": int(time.time())})
    elif not enabled_now and pause_start is None and last_action != "disable":
        # enabled=0 but no disable event recorded (e.g. legacy data before
        # events were tracked): fall back to disabled_at if present.
        if row["disabled_at"]:
            paused.append({"start": row["disabled_at"], "end": int(time.time())})

    return {
        "enabled": enabled_now,
        "enabled_at": row["enabled_at"],
        "disabled_at": row["disabled_at"],
        "paused": paused,
    }


# ------------------------------------------------------------------ #
# Loss events list (paginated, consecutive losses merged into one entry)
# ------------------------------------------------------------------ #

@router.get("/{target_id}/loss-events")
async def get_loss_events(
    target_id: int,
    page:  int           = Query(1,   ge=1),
    limit: int           = Query(50,  ge=1, le=200),
    start: Optional[int] = Query(None),
    end:   Optional[int] = Query(None),
):
    offset = (page - 1) * limit
    conditions = ["target_id=?", "is_loss=1"]
    params: list = [target_id]

    if start is not None:
        conditions.append("ts>=?")
        params.append(start)
    if end is not None:
        conditions.append("ts<=?")
        params.append(end)

    where = " AND ".join(conditions)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Probe interval of the target: consecutive loss events are merged
        # when the gap between them is <= one interval.
        interval_ms = 1000
        async with db.execute(
            "SELECT interval_ms FROM targets WHERE id=?", (target_id,)
        ) as cur:
            trow = await cur.fetchone()
        if trow and trow["interval_ms"]:
            interval_ms = trow["interval_ms"]

        async with db.execute(
            f"SELECT COUNT(*) AS cnt FROM ping_results WHERE {where}", params
        ) as cur:
            total = (await cur.fetchone())["cnt"]

        # Fetch ALL loss timestamps in the window (chronological), merge
        # consecutive ones into runs, then paginate over the runs so that
        # merged_total / page / items stay consistent.
        async with db.execute(
            f"SELECT ts FROM ping_results WHERE {where} ORDER BY ts ASC",
            params,
        ) as cur:
            all_ts = [r["ts"] for r in await cur.fetchall()]

    # gap tolerance: one interval (+ small slack for scheduling jitter)
    gap_tol = interval_ms / 1000.0 + 1

    # Merge consecutive loss timestamps into [start, end, count] runs.
    merged: list[dict] = []
    for ts in all_ts:
        if merged and ts - merged[-1]["end_ts"] <= gap_tol:
            merged[-1]["end_ts"] = ts
            merged[-1]["count"] += 1
        else:
            merged.append({"start_ts": ts, "end_ts": ts, "count": 1})

    # Newest first (matches previous page ordering), then paginate over runs.
    merged.reverse()
    merged_total = len(merged)
    items = merged[offset:offset + limit]

    return {
        "total": total,           # raw loss count (kept for the "共 N 条" label)
        "merged_total": merged_total,  # loss runs after merging consecutive
        "page":  page,
        "limit": limit,
        "items": items,
    }


# ------------------------------------------------------------------ #
# Historical compressed summary (data > 7 days old)
# ------------------------------------------------------------------ #

@router.get("/{target_id}/summary")
async def get_summary(
    target_id: int,
    start: Optional[int] = Query(None, description="Unix timestamp, bucket_ts >= start"),
    end:   Optional[int] = Query(None, description="Unix timestamp, bucket_ts <= end"),
):
    conditions = ["target_id=?"]
    params: list = [target_id]
    if start is not None:
        conditions.append("bucket_ts>=?")
        params.append(start)
    if end is not None:
        conditions.append("bucket_ts<=?")
        params.append(end)
    where = " AND ".join(conditions)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"""
            SELECT
                SUM(cnt_1_30)      AS c1, SUM(cnt_31_60)     AS c2,
                SUM(cnt_61_100)    AS c3, SUM(cnt_101_200)   AS c4,
                SUM(cnt_201_500)   AS c5, SUM(cnt_501_1000)  AS c6,
                SUM(cnt_over_1000) AS c7, SUM(cnt_loss)      AS loss,
                SUM(cnt_total)     AS total,
                MIN(bucket_ts)     AS oldest, MAX(bucket_ts) AS newest
            FROM ping_summary
            WHERE {where}
            """,
            params,
        ) as cur:
            r = dict(await cur.fetchone())

    total = r["total"] or 0
    if not total:
        return None

    def pct(n):
        return round((n or 0) / total * 100, 1)

    return {
        "oldest": r["oldest"],
        "newest": r["newest"],
        "total":  total,
        "buckets": {
            "<30ms":     {"count": r["c1"] or 0, "pct": pct(r["c1"])},
            "30-60ms":   {"count": r["c2"] or 0, "pct": pct(r["c2"])},
            "60-100ms":  {"count": r["c3"] or 0, "pct": pct(r["c3"])},
            "100-200ms": {"count": r["c4"] or 0, "pct": pct(r["c4"])},
            "200-500ms": {"count": r["c5"] or 0, "pct": pct(r["c5"])},
            "500-1000ms":{"count": r["c6"] or 0, "pct": pct(r["c6"])},
            ">1000ms":   {"count": r["c7"] or 0, "pct": pct(r["c7"])},
            "丢包":       {"count": r["loss"] or 0, "pct": pct(r["loss"])},
        },
    }


# ------------------------------------------------------------------ #
# Recent raw data (for live 5-min chart initial load)
# ------------------------------------------------------------------ #

@router.get("/{target_id}/recent")
async def get_recent(
    target_id: int,
    seconds: int = Query(300, ge=10, le=300),
):
    """Return raw ping results for the last N seconds (max 300 = 5 min)."""
    since = int(time.time()) - seconds
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT ts, latency_ms, is_loss
            FROM ping_results
            WHERE target_id=? AND ts>=?
            ORDER BY ts
            """,
            (target_id, since),
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"ts": r["ts"], "latency_ms": r["latency_ms"], "is_loss": r["is_loss"]}
        for r in rows
    ]


# ------------------------------------------------------------------ #
# SSE live stream
# ------------------------------------------------------------------ #

@router.get("/{target_id}/stream", dependencies=[Depends(_sse_auth)])
async def stream_ping(target_id: int, request: Request, token: Optional[str] = Query(None)):
    """
    Server-Sent Events stream – pushes one JSON object per ping probe.
    Payload: {"ts": int, "latency_ms": float|null, "is_loss": 0|1}
    A comment ":keepalive" is sent every 20 s to prevent proxy timeouts.
    """
    async def generator():
        q = sub_manager.subscribe(target_id)
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(q.get(), timeout=20.0)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sub_manager.unsubscribe(target_id, q)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "Connection":       "keep-alive",
            "X-Accel-Buffering": "no",   # disable nginx / proxy buffering
        },
    )


# ------------------------------------------------------------------ #
# Manual compression trigger
# ------------------------------------------------------------------ #

@router.get("/all-recent")
async def get_all_recent(seconds: int = Query(5, ge=1, le=60)):
    """Return last N seconds of raw ping results for ALL targets (for card live bars)."""
    server_ts = int(time.time())
    since = server_ts - seconds
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT target_id, ts, latency_ms, is_loss
            FROM ping_results
            WHERE ts >= ?
            ORDER BY target_id, ts
            """,
            (since,),
        ) as cur:
            rows = await cur.fetchall()
        async with db.execute(
            "SELECT id FROM targets WHERE enabled=1"
        ) as cur:
            enabled_ids = [r[0] for r in await cur.fetchall()]

    result: dict = {tid: [] for tid in enabled_ids}
    for r in rows:
        tid = r["target_id"]
        if tid not in result:
            result[tid] = []
        result[tid].append({
            "ts": int(r["ts"]),
            "latency_ms": r["latency_ms"],
            "is_loss": int(r["is_loss"]),
        })
    return {"server_ts": server_ts, "data": result}


import asyncio

_compress_lock = asyncio.Lock()


@router.post("/compress")
async def manual_compress(
    keep_hours: int = Query(
        1, ge=1, le=168,
        description="Keep the most recent N hours of raw data; compress everything older",
    ),
):
    """
    Compress raw data older than the retention window into hourly summary
    buckets, then return the result (compressed row count and bucket count).
    """
    cutoff = int(time.time()) - keep_hours * 3600
    async with _compress_lock:
        result = await compress_old_data(DB_PATH, cutoff=cutoff)
    return {"ok": True, "keep_hours": keep_hours, **result}
