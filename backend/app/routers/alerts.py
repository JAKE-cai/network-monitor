"""Alert rules / history / suppressions / SMTP settings API."""

import time
from typing import Literal, Optional

import aiosqlite
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from ..database import DB_PATH

router = APIRouter(prefix="/api/alerts", tags=["alerts"])

ScopeType = Literal["all", "group", "tag", "ids"]


class AlertCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    condition: Literal["loss", "latency"]
    scope_type: ScopeType = "all"
    scope_value: str = Field("", max_length=512)
    loss_pct: float = Field(0, ge=0, le=100)
    loss_consecutive: int = Field(0, ge=0, le=100000)
    latency_ms: float = Field(0, ge=0)
    latency_count: int = Field(0, ge=0, le=100000)
    window_count: int = Field(60, ge=1, le=100000)
    repeat_min: int = Field(30, ge=0, le=100000)
    enabled: bool = True
    # Effective-time windows (all selected ones must be satisfied)
    enable_date_range: bool = Field(False)
    date_start: str = Field("", max_length=20)
    date_end: str = Field("", max_length=20)
    enable_weekdays: bool = Field(False)
    weekdays: str = Field("", max_length=32)
    enable_time_range: bool = Field(False)
    time_start: str = Field("", max_length=8)
    time_end: str = Field("", max_length=8)


class AlertUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=64)
    condition: Optional[Literal["loss", "latency"]] = None
    scope_type: Optional[ScopeType] = None
    scope_value: Optional[str] = Field(None, max_length=512)
    loss_pct: Optional[float] = Field(None, ge=0, le=100)
    loss_consecutive: Optional[int] = Field(None, ge=0)
    latency_ms: Optional[float] = Field(None, ge=0)
    latency_count: Optional[int] = Field(None, ge=0)
    window_count: Optional[int] = Field(None, ge=1)
    repeat_min: Optional[int] = Field(None, ge=0)
    enabled: Optional[bool] = None
    enable_date_range: Optional[bool] = None
    date_start: Optional[str] = Field(None, max_length=20)
    date_end: Optional[str] = Field(None, max_length=20)
    enable_weekdays: Optional[bool] = None
    weekdays: Optional[str] = Field(None, max_length=32)
    enable_time_range: Optional[bool] = None
    time_start: Optional[str] = Field(None, max_length=8)
    time_end: Optional[str] = Field(None, max_length=8)


class SuppressionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    scope_type: ScopeType = "all"
    scope_value: str = Field("", max_length=512)
    start_ts: int = Field(..., ge=0)
    end_ts: int = Field(..., ge=0)
    enabled: bool = True

    @field_validator("end_ts")
    @classmethod
    def _end_after_start(cls, v, info):
        s = info.data.get("start_ts")
        if s is not None and v <= s:
            raise ValueError("结束时间必须晚于开始时间")
        return v


class SuppressionUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=64)
    scope_type: Optional[ScopeType] = None
    scope_value: Optional[str] = Field(None, max_length=512)
    start_ts: Optional[int] = Field(None, ge=0)
    end_ts: Optional[int] = Field(None, ge=0)
    enabled: Optional[bool] = None


# ─────────────────────────── Alert rules ───────────────────────────

@router.get("/rules")
async def list_rules():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM alerts ORDER BY id DESC") as cur:
            rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        r["enabled"] = bool(r["enabled"])
        r["enable_date_range"] = bool(r["enable_date_range"])
        r["enable_weekdays"] = bool(r["enable_weekdays"])
        r["enable_time_range"] = bool(r["enable_time_range"])
    return rows


def _validate_windows(data: dict) -> None:
    """Validate the effective-time window fields. Raises ValueError on error."""
    if data.get("enable_date_range"):
        start = data.get("date_start") or ""
        end   = data.get("date_end")   or ""
        if not start or not end:
            raise ValueError("已勾选生效时间，请填写起止日期时间")
        try:
            from datetime import datetime
            # Accept both "YYYY-MM-DD HH:MM" and the <input type=datetime-local>
            # value "YYYY-MM-DDTHH:MM".
            s = datetime.strptime(start.replace("T", " "), "%Y-%m-%d %H:%M")
            e = datetime.strptime(end.replace("T", " "),   "%Y-%m-%d %H:%M")
        except ValueError:
            raise ValueError("生效时间格式无效，应为 YYYY-MM-DD HH:MM")
        if s >= e:
            raise ValueError("生效时间的开始时间须早于结束时间")
    if data.get("enable_weekdays"):
        wds = [x.strip() for x in (data.get("weekdays") or "").split(",") if x.strip()]
        if not wds or not all(x.isdigit() and 0 <= int(x) <= 6 for x in wds):
            raise ValueError("已勾选按天，请至少选择一天")
    if data.get("enable_time_range"):
        ts = data.get("time_start") or ""
        te = data.get("time_end")   or ""
        if not ts or not te:
            raise ValueError("已勾选按时段，请填写起止时间")
        try:
            a = tuple(int(x) for x in ts.split(":"))
            b = tuple(int(x) for x in te.split(":"))
        except ValueError:
            raise ValueError("时段格式无效，应为 HH:MM")
        if len(a) < 2 or len(b) < 2 or not (0 <= a[0] <= 23 and 0 <= a[1] <= 59 and 0 <= b[0] <= 23 and 0 <= b[1] <= 59):
            raise ValueError("时段格式无效，应为 HH:MM")
        if a == b:
            raise ValueError("时段的开始与结束时间不能相同")


@router.post("/rules", status_code=201)
async def create_rule(body: AlertCreate):
    try:
        _validate_windows(body.model_dump())
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO alerts
               (name, condition, scope_type, scope_value, loss_pct, loss_consecutive,
                latency_ms, latency_count, window_count, repeat_min, enabled,
                enable_date_range, date_start, date_end,
                enable_weekdays, weekdays,
                enable_time_range, time_start, time_end)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (body.name, body.condition, body.scope_type, body.scope_value,
             body.loss_pct, body.loss_consecutive, body.latency_ms, body.latency_count,
             body.window_count, body.repeat_min, 1 if body.enabled else 0,
             1 if body.enable_date_range else 0, body.date_start, body.date_end,
             1 if body.enable_weekdays else 0, body.weekdays,
             1 if body.enable_time_range else 0, body.time_start, body.time_end),
        )
        await db.commit()
        rid = cur.lastrowid
    return {**body.model_dump(), "id": rid}


@router.put("/rules/{rule_id}")
async def update_rule(rule_id: int, body: AlertUpdate):
    updates: dict = body.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(400, "无更新内容")
    # Load current values so window validation sees the merged result
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM alerts WHERE id=?", (rule_id,)) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "规则不存在")
        merged = dict(row)
    merged.update(updates)
    try:
        _validate_windows(merged)
    except ValueError as exc:
        raise HTTPException(400, str(exc))

    # Boolean columns are stored as 0/1
    for bool_col in ("enabled", "enable_date_range", "enable_weekdays", "enable_time_range"):
        if bool_col in updates:
            updates[bool_col] = 1 if updates[bool_col] else 0
    set_clause = ", ".join(f"{k}=?" for k in updates)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE alerts SET {set_clause} WHERE id=?",
            (*updates.values(), rule_id),
        )
        await db.commit()
    return {"ok": True, "id": rule_id}


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM alerts WHERE id=?", (rule_id,))
        await db.execute("UPDATE alert_history SET status='recovered', recovered_at=strftime('%s','now') WHERE alert_id=? AND status='firing'", (rule_id,))
        await db.commit()
    return {"ok": True}


# ─────────────────────── Alert history ───────────────────────

@router.get("/history")
async def list_history(
    status: Optional[str] = None,
    alert_id: Optional[int] = None,
    target_id: Optional[int] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    conds, params = [], []
    if status:
        conds.append("status=?")
        params.append(status)
    if alert_id is not None:
        conds.append("alert_id=?")
        params.append(alert_id)
    if target_id is not None:
        conds.append("target_id=?")
        params.append(target_id)
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"SELECT COUNT(*) AS c FROM alert_history{where}", params
        ) as cur:
            total = (await cur.fetchone())["c"]
        async with db.execute(
            f"SELECT h.*, a.name AS alert_name FROM alert_history h "
            f"LEFT JOIN alerts a ON a.id=h.alert_id{where} "
            f"ORDER BY h.id DESC LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]
    return {"total": total, "items": rows}


# Confirm / un-confirm a recovered alert (moves it to "history" as acknowledged)
@router.post("/history/{hid}/confirm")
async def confirm_history(hid: int):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status FROM alert_history WHERE id=?", (hid,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "记录不存在")
        if row[0] == "firing":
            raise HTTPException(400, "告警仍在触发中，无法确认")
        await db.execute(
            "UPDATE alert_history SET status='confirmed', confirmed_at=? WHERE id=?",
            (now, hid),
        )
        await db.commit()
    return {"ok": True, "id": hid, "status": "confirmed"}


@router.post("/history/{hid}/unconfirm")
async def unconfirm_history(hid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status FROM alert_history WHERE id=?", (hid,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "记录不存在")
        if row[0] != "confirmed":
            raise HTTPException(400, "仅已确认的记录可反确认")
        await db.execute(
            "UPDATE alert_history SET status='recovered', confirmed_at=NULL WHERE id=?",
            (hid,),
        )
        await db.commit()
    return {"ok": True, "id": hid, "status": "recovered"}


@router.delete("/history/{hid}")
async def delete_history(hid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM alert_history WHERE id=?", (hid,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "记录不存在")
        await db.execute("DELETE FROM alert_history WHERE id=?", (hid,))
        await db.commit()
    return {"ok": True, "id": hid}


# ─────────────────────── Batch history operations ───────────────────────

class BatchHistoryBody(BaseModel):
    ids: list[int] = Field(..., min_length=1)


@router.post("/history/batch-confirm")
async def batch_confirm_history(body: BatchHistoryBody):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        ph = ",".join("?" * len(body.ids))
        # Only recovered (not firing) records can be confirmed
        await db.execute(
            f"UPDATE alert_history SET status='confirmed', confirmed_at=? "
            f"WHERE id IN ({ph}) AND status!='firing'",
            [now, *body.ids],
        )
        await db.commit()
    return {"ok": True, "count": len(body.ids)}


@router.post("/history/batch-unconfirm")
async def batch_unconfirm_history(body: BatchHistoryBody):
    async with aiosqlite.connect(DB_PATH) as db:
        ph = ",".join("?" * len(body.ids))
        # Only confirmed records can be un-confirmed
        await db.execute(
            f"UPDATE alert_history SET status='recovered', confirmed_at=NULL "
            f"WHERE id IN ({ph}) AND status='confirmed'",
            body.ids,
        )
        await db.commit()
    return {"ok": True, "count": len(body.ids)}


@router.post("/history/batch-delete")
async def batch_delete_history(body: BatchHistoryBody):
    async with aiosqlite.connect(DB_PATH) as db:
        ph = ",".join("?" * len(body.ids))
        await db.execute(f"DELETE FROM alert_history WHERE id IN ({ph})", body.ids)
        await db.commit()
    return {"ok": True, "count": len(body.ids)}


# ─────────────────────── Suppressions ───────────────────────

@router.get("/suppressions")
async def list_suppressions():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM alert_suppressions ORDER BY start_ts DESC") as cur:
            rows = [dict(r) for r in await cur.fetchall()]
    for r in rows:
        r["enabled"] = bool(r["enabled"])
    return rows


@router.post("/suppressions", status_code=201)
async def create_suppression(body: SuppressionCreate):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO alert_suppressions (name, scope_type, scope_value, start_ts, end_ts, enabled)
               VALUES (?,?,?,?,?,?)""",
            (body.name, body.scope_type, body.scope_value, body.start_ts, body.end_ts,
             1 if body.enabled else 0),
        )
        await db.commit()
        sid = cur.lastrowid
    return {**body.model_dump(), "id": sid}


@router.put("/suppressions/{sid}")
async def update_suppression(sid: int, body: SuppressionUpdate):
    updates: dict = body.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(400, "无更新内容")
    if "enabled" in updates:
        updates["enabled"] = 1 if updates["enabled"] else 0
    set_clause = ", ".join(f"{k}=?" for k in updates)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE alert_suppressions SET {set_clause} WHERE id=?",
            (*updates.values(), sid),
        )
        await db.commit()
    return {"ok": True, "id": sid}


@router.delete("/suppressions/{sid}")
async def delete_suppression(sid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM alert_suppressions WHERE id=?", (sid,))
        await db.commit()
    return {"ok": True}


# ─────────────────────── SMTP settings ───────────────────────

@router.get("/settings/smtp")
async def get_smtp_settings():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT key, value FROM settings WHERE key LIKE 'smtp_%' OR key='notify_email'"
        ) as cur:
            rows = await cur.fetchall()
    cfg = {k: v for k, v in rows}
    return {
        "smtp_host": cfg.get("smtp_host", ""),
        "smtp_port": cfg.get("smtp_port", "587"),
        "smtp_user": cfg.get("smtp_user", ""),
        "smtp_password": cfg.get("smtp_password", ""),
        "smtp_from": cfg.get("smtp_from", ""),
        "smtp_tls": cfg.get("smtp_tls", "1") == "1",
        "notify_email": cfg.get("notify_email", ""),
    }


@router.put("/settings/smtp")
async def save_smtp_settings(body: dict):
    allowed = {"smtp_host", "smtp_port", "smtp_user", "smtp_password", "smtp_from", "notify_email"}
    async with aiosqlite.connect(DB_PATH) as db:
        for k, v in body.items():
            if k in allowed:
                await db.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)",
                    (k, str(v)),
                )
        # tls is a boolean field
        if "smtp_tls" in body:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('smtp_tls', ?)",
                ("1" if body["smtp_tls"] else "0",),
            )
        await db.commit()
    return {"ok": True}
