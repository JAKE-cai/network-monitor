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
    return rows


@router.post("/rules", status_code=201)
async def create_rule(body: AlertCreate):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO alerts
               (name, condition, scope_type, scope_value, loss_pct, loss_consecutive,
                latency_ms, latency_count, window_count, repeat_min, enabled)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (body.name, body.condition, body.scope_type, body.scope_value,
             body.loss_pct, body.loss_consecutive, body.latency_ms, body.latency_count,
             body.window_count, body.repeat_min, 1 if body.enabled else 0),
        )
        await db.commit()
        rid = cur.lastrowid
    return {**body.model_dump(), "id": rid}


@router.put("/rules/{rule_id}")
async def update_rule(rule_id: int, body: AlertUpdate):
    updates: dict = body.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(400, "无更新内容")
    if "enabled" in updates:
        updates["enabled"] = 1 if updates["enabled"] else 0
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
