import re
import time
from typing import Literal, Optional

import aiosqlite
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from ..database import DB_PATH
from ..state import ping_manager
from ..target_scope import batch_set_enabled, record_target_event, resolve_target_ids

router = APIRouter(prefix="/api/targets", tags=["targets"])

# Strict allow-list: hostname labels, IPv4, IPv6 (no leading dash, no shell meta)
_ADDR_RE = re.compile(
    r'^('
    r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?'  # hostname
    r'|(?:\d{1,3}\.){3}\d{1,3}'          # IPv4
    r'|[0-9a-fA-F:]{2,39}'               # IPv6 (simplified)
    r')$'
)

# host:port  (IPv4 / hostname / [IPv6])
_HOSTPORT_RE = re.compile(
    r'^'
    r'('
    r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?'  # hostname
    r'|(?:\d{1,3}\.){3}\d{1,3}'          # IPv4
    r'|\[[0-9a-fA-F:]+\]'                # [IPv6]
    r')'
    r':(\d{1,5})$'
)

# http://host[:port][/path]
_HTTP_RE = re.compile(
    r'^https?://'
    r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?'
    r'(?::\d{1,5})?(?:/.*)?$',
    re.IGNORECASE,
)


def _validate_address(v: str, probe_type: str = "icmp") -> str:
    v = v.strip()
    if not v:
        raise ValueError("地址不能为空")
    if probe_type == "http":
        if not _HTTP_RE.match(v):
            raise ValueError("HTTP 地址格式无效，示例: http://example.com 或 http://1.2.3.4:8080")
        return v
    if probe_type in ("tcp", "udp"):
        m = _HOSTPORT_RE.match(v)
        if not m:
            raise ValueError("TCP/UDP 地址格式无效，示例: 1.2.3.4:8080 或 example.com:53")
        port = int(m.group(2))
        if not (1 <= port <= 65535):
            raise ValueError("端口号须在 1-65535 之间")
        return v
    # icmp: hostname / IPv4 / IPv6
    if not _ADDR_RE.match(v):
        raise ValueError("ICMP 地址格式无效，仅支持域名、IPv4 或 IPv6")
    return v


def _probe_type(v: str) -> str:
    v = (v or "icmp").strip().lower()
    if v not in ("icmp", "tcp", "udp", "http"):
        raise ValueError("探测类型仅支持 icmp / tcp / udp / http")
    return v


# ------------------------------------------------------------------ #
# Schemas
# ------------------------------------------------------------------ #

class TargetCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=64)
    probe_type: str = Field("icmp", max_length=16)   # icmp | tcp | udp | http
    address: str
    interval_ms: int = Field(1000, ge=100, le=60000)
    enabled: bool = True
    group_name: str = Field("", max_length=64)
    tags: str = Field("", max_length=256)   # comma-separated
    port: Optional[int] = Field(None, ge=1, le=65535)

    @field_validator("address")
    @classmethod
    def _check_address(cls, v: str, info) -> str:
        pt = (info.data.get("probe_type") or "icmp").strip().lower()
        return _validate_address(v, pt)

    @field_validator("probe_type")
    @classmethod
    def _check_type(cls, v: str) -> str:
        return _probe_type(v)

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        return v.strip()


class BatchEnabledBody(BaseModel):
    enabled: bool
    scope_type: Literal["all", "group", "tag", "ids", "filtered"] = "all"
    scope_value: str = Field("", max_length=256)
    filter_group: str = Field("", max_length=64)
    filter_tag: str = Field("", max_length=64)
    filter_search: str = Field("", max_length=128)


class TargetUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=64)
    probe_type: Optional[str] = Field(None, max_length=16)
    address: Optional[str] = None
    interval_ms: Optional[int] = Field(None, ge=100, le=60000)
    enabled: Optional[bool] = None
    group_name: Optional[str] = Field(None, max_length=64)
    tags: Optional[str] = Field(None, max_length=256)
    port: Optional[int] = Field(None, ge=1, le=65535)

    @field_validator("address")
    @classmethod
    def _check_address(cls, v: Optional[str], info) -> Optional[str]:
        if v is None:
            return v
        pt = (info.data.get("probe_type") or "icmp").strip().lower()
        return _validate_address(v, pt)

    @field_validator("probe_type")
    @classmethod
    def _check_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return _probe_type(v)


# ------------------------------------------------------------------ #
# Endpoints
# ------------------------------------------------------------------ #

@router.get("")
async def list_targets():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM targets ORDER BY group_name, name"
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


@router.post("/batch-enabled")
async def batch_enabled(body: BatchEnabledBody):
    """Enable or disable many targets by scope (all / group / tag / ids / current filters)."""
    ids = await resolve_target_ids(
        DB_PATH,
        body.scope_type,
        body.scope_value,
        filter_group=body.filter_group,
        filter_tag=body.filter_tag,
        filter_search=body.filter_search,
    )
    if not ids:
        raise HTTPException(400, "没有匹配的目标")
    return await batch_set_enabled(DB_PATH, ids, body.enabled)


@router.get("/groups")
async def list_groups():
    """Return distinct non-empty group names."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT DISTINCT group_name FROM targets WHERE group_name != '' ORDER BY group_name"
        ) as cur:
            rows = await cur.fetchall()
    return [r["group_name"] for r in rows]


async def _unique_clone_name(db: aiosqlite.Connection, base_name: str) -> str:
    """Pick a non-colliding name: 'xxx (副本)', 'xxx (副本2)', ..."""
    candidate = f"{base_name} (副本)"
    n = 2
    while True:
        async with db.execute(
            "SELECT 1 FROM targets WHERE name=?", (candidate,)
        ) as cur:
            if not await cur.fetchone():
                return candidate
        candidate = f"{base_name} (副本{n})"
        n += 1


def _resolve_port(probe_type: str, address: str, port) -> Optional[int]:
    """If the port isn't explicitly provided but the address is host:port,
    extract it. ICMP never has a port."""
    if probe_type == "icmp":
        return None
    if port is not None:
        return port
    m = _HOSTPORT_RE.match(address.strip())
    if m:
        p = int(m.group(2))
        if 1 <= p <= 65535:
            return p
    return port


@router.post("", status_code=201)
async def create_target(body: TargetCreate):
    async with aiosqlite.connect(DB_PATH) as db:
        now = int(time.time())
        port = _resolve_port(body.probe_type, body.address, body.port)
        cur = await db.execute(
            """INSERT INTO targets (name, address, interval_ms, enabled, group_name, tags,
                                    enabled_at, disabled_at, probe_type, port)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (body.name, body.address, body.interval_ms,
             1 if body.enabled else 0, body.group_name, body.tags,
             now if body.enabled else None, None if body.enabled else now,
             body.probe_type, port),
        )
        await db.commit()
        target_id = cur.lastrowid

    if body.enabled:
        ping_manager.add_target(target_id, body.address, body.interval_ms, body.probe_type, port)
    await record_target_event(DB_PATH, target_id, "enable" if body.enabled else "disable", int(time.time()))

    return {"id": target_id, **body.model_dump()}


@router.post("/{target_id}/clone", status_code=201)
async def clone_target(target_id: int):
    """Duplicate a target (same address/settings); new row gets a unique name."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM targets WHERE id=?", (target_id,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "Target not found")
        src = dict(row)
        new_name = await _unique_clone_name(db, src["name"])
        now = int(time.time())
        probe_type = src.get("probe_type") or "icmp"
        port = src.get("port") if probe_type != "icmp" else None
        cur = await db.execute(
            """INSERT INTO targets (name, address, interval_ms, enabled, group_name, tags,
                                    enabled_at, disabled_at, probe_type, port)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                new_name,
                src["address"],
                src["interval_ms"],
                src["enabled"],
                src["group_name"],
                src["tags"],
                now if src["enabled"] else None,
                None if src["enabled"] else now,
                probe_type,
                port,
            ),
        )
        await db.commit()
        new_id = cur.lastrowid

    if src["enabled"]:
        ping_manager.add_target(new_id, src["address"], src["interval_ms"], probe_type, port)
    await record_target_event(DB_PATH, new_id, "enable" if src["enabled"] else "disable", int(time.time()))

    return {
        "id": new_id,
        "name": new_name,
        "address": src["address"],
        "interval_ms": src["interval_ms"],
        "enabled": bool(src["enabled"]),
        "group_name": src["group_name"],
        "tags": src["tags"],
        "probe_type": probe_type,
        "port": port,
    }


@router.put("/{target_id}")
async def update_target(target_id: int, body: TargetUpdate):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM targets WHERE id=?", (target_id,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(404, "Target not found")

        updates: dict = {}
        if body.name is not None:       updates["name"]       = body.name
        if body.address is not None:    updates["address"]    = body.address
        if body.interval_ms is not None:updates["interval_ms"]= body.interval_ms
        if body.enabled is not None:    updates["enabled"]    = 1 if body.enabled else 0
        if body.group_name is not None: updates["group_name"] = body.group_name
        if body.tags is not None:       updates["tags"]       = body.tags
        if body.probe_type is not None: updates["probe_type"] = body.probe_type
        if body.port is not None:       updates["port"]       = body.port

        # When the enabled state actually changes, record the timestamp so the
        # chart can mark "paused" periods instead of showing red packet loss,
        # and append an event to target_events for full history greying.
        state_changed = (body.enabled is not None and bool(body.enabled) != bool(row["enabled"]))
        if state_changed:
            updates["enabled_at" if body.enabled else "disabled_at"] = int(time.time())

        # Probe type changed to icmp -> port not needed
        if body.probe_type == "icmp":
            updates["port"] = None
        # For tcp/udp, if the address changed and no explicit port was given,
        # derive the port from the host:port address.
        elif body.address is not None and body.port is None:
            new_pt = body.probe_type or row["probe_type"] or "icmp"
            p = _resolve_port(new_pt, body.address, None)
            if p is not None:
                updates["port"] = p

        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            await db.execute(
                f"UPDATE targets SET {set_clause} WHERE id=?",
                (*updates.values(), target_id),
            )
            await db.commit()

        if state_changed:
            await record_target_event(DB_PATH, target_id, "enable" if body.enabled else "disable")

        async with db.execute(
            "SELECT * FROM targets WHERE id=?", (target_id,)
        ) as cur:
            updated = dict(await cur.fetchone())

    if updated["enabled"]:
        ping_manager.add_target(
            target_id,
            updated["address"],
            updated["interval_ms"],
            updated.get("probe_type") or "icmp",
            updated.get("port"),
        )
    else:
        ping_manager.remove_target(target_id)

    return updated


@router.delete("/{target_id}")
async def delete_target(target_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM targets WHERE id=?", (target_id,)
        ) as cur:
            if not await cur.fetchone():
                raise HTTPException(404, "Target not found")
        await db.execute("DELETE FROM targets        WHERE id=?",        (target_id,))
        await db.execute("DELETE FROM ping_results   WHERE target_id=?", (target_id,))
        await db.execute("DELETE FROM ping_summary   WHERE target_id=?", (target_id,))
        await db.commit()

    ping_manager.remove_target(target_id)
    return {"ok": True}
