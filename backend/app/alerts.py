"""
Alerting engine: evaluates target health against alert rules, manages
suppressions, sends email notifications and records alert history.

Conditions (rule.condition):
  - loss:     triggered when loss rate over the last `window_count` samples
              is >= `loss_pct` AND/OR consecutive losses >= `loss_consecutive`
  - latency:  triggered when at least `latency_count` samples in the last
              `window_count` exceed `latency_ms`
"""

import asyncio
import logging
import re
import smtplib
import time
from datetime import datetime
from email.mime.text import MIMEText
from typing import List, Optional

import aiosqlite

from .database import DB_PATH
from .target_scope import resolve_target_ids

logger = logging.getLogger(__name__)

POLL_INTERVAL = 5          # evaluate every N seconds
WINDOW_SECONDS = 3600      # how far back raw samples are fetched for evaluation


# ------------------------------------------------------------------ #
# DB helpers
# ------------------------------------------------------------------ #

async def _fetch_rules(db_path: str) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alerts WHERE enabled=1"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def _fetch_suppressions(db_path: str, now: int) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alert_suppressions WHERE enabled=1 AND start_ts<=? AND end_ts>=?",
            (now, now),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def _fetch_targets(db_path: str) -> list[dict]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, address, group_name, tags, enabled FROM targets"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def _fetch_recent_samples(db_path: str, target_id: int, n: int, now: int) -> list[tuple]:
    """Return last n (latency_ms, is_loss) samples for target, oldest→newest."""
    since = now - WINDOW_SECONDS
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            """SELECT latency_ms, is_loss FROM ping_results
               WHERE target_id=? AND ts>=? ORDER BY ts DESC LIMIT ?""",
            (target_id, since, n),
        ) as cur:
            rows = await cur.fetchall()
    return [(r[0], r[1]) for r in reversed(rows)]


# ------------------------------------------------------------------ #
# Condition evaluation
# ------------------------------------------------------------------ #

def _evaluate(rule: dict, samples: list[tuple]) -> tuple[bool, str, Optional[float]]:
    """Return (triggered, detail, value)."""
    if not samples:
        return False, "无样本数据", None
    total = len(samples)
    losses = sum(1 for _, is_loss in samples if is_loss)
    loss_pct = losses / total * 100 if total else 0

    if rule["condition"] == "loss":
        reasons = []
        if rule.get("loss_pct") and loss_pct >= rule["loss_pct"]:
            reasons.append(f"丢包率{loss_pct:.1f}%≥{rule['loss_pct']}%")
        if rule.get("loss_consecutive"):
            # count trailing consecutive losses
            consec = 0
            for _, is_loss in reversed(samples):
                if is_loss:
                    consec += 1
                else:
                    break
            if consec >= rule["loss_consecutive"]:
                reasons.append(f"连续丢包{consec}次≥{rule['loss_consecutive']}次")
        if reasons:
            return True, "；".join(reasons), loss_pct
        return False, f"丢包率{loss_pct:.1f}%（{losses}/{total}）", loss_pct

    # latency
    thr = rule.get("latency_ms") or 0
    over = [lat for lat, _ in samples if lat is not None and lat >= thr]
    if rule.get("latency_count") and len(over) >= rule["latency_count"]:
        mx = max(over)
        return True, f"{len(over)}/{total}次延迟≥{thr:.0f}ms（最大{mx:.1f}ms）", mx
    return False, f"{len(over)}/{total}次≥{thr:.0f}ms", None


# ------------------------------------------------------------------ #
# Rule effective-time windows
# ------------------------------------------------------------------ #

def _rule_in_window(rule: dict, now: int) -> bool:
    """Return True if the rule's effective-time windows are satisfied at `now`.
    Each enabled window must be satisfied (AND across windows); a window with no
    configured value is ignored. A rule with no enabled windows always matches."""
    if not (rule.get("enable_date_range") or rule.get("enable_weekdays") or rule.get("enable_time_range")):
        return True

    dt = datetime.fromtimestamp(now)

    # 1) Date range: YYYY-MM-DD HH:MM .. YYYY-MM-DD HH:MM
    #    (also accepts the <input type=datetime-local> "T" separator)
    if rule.get("enable_date_range"):
        try:
            start = datetime.strptime((rule.get("date_start") or "").replace("T", " "), "%Y-%m-%d %H:%M")
            end   = datetime.strptime((rule.get("date_end")   or "").replace("T", " "), "%Y-%m-%d %H:%M")
            if not (start <= dt <= end):
                return False
        except ValueError:
            return False

    # 2) Weekdays: 0=Monday .. 6=Sunday
    if rule.get("enable_weekdays"):
        wanted = set()
        for part in (rule.get("weekdays") or "").split(","):
            part = part.strip()
            if part.isdigit() and 0 <= int(part) <= 6:
                wanted.add(int(part))
        if not wanted:
            return False
        if dt.weekday() not in wanted:
            return False

    # 3) Daily time range: HH:MM .. HH:MM
    if rule.get("enable_time_range"):
        try:
            ts = (dt.hour, dt.minute)
            t0 = tuple(int(x) for x in (rule.get("time_start") or "").split(":"))
            t1 = tuple(int(x) for x in (rule.get("time_end")   or "").split(":"))
        except ValueError:
            return False
        if len(t0) < 2 or len(t1) < 2:
            return False
        # Handle a range that crosses midnight (e.g. 22:00-02:00)
        if t0 <= t1:
            if not (t0 <= ts <= t1):
                return False
        else:
            if not (ts >= t0 or ts <= t1):
                return False

    return True


# ------------------------------------------------------------------ #
# Suppression matching
# ------------------------------------------------------------------ #

def _csv_set(v: str) -> set:
    return {x.strip() for x in (v or "").split(",") if x.strip()}


def _tag_matches(tags_str: str, wanted: set) -> bool:
    if not wanted:
        return False
    parts = {x.strip() for x in (tags_str or "").split(",") if x.strip()}
    return bool(parts & wanted)


def _is_suppressed(target: dict, suppressions: list[dict]) -> bool:
    for s in suppressions:
        st = s["scope_type"]
        sv = s.get("scope_value") or ""
        if st == "all":
            return True
        if st == "group":
            if target.get("group_name") in _csv_set(sv):
                return True
        elif st == "tag":
            if _tag_matches(target.get("tags", ""), _csv_set(sv)):
                return True
        elif st == "ids":
            ids = {int(x) for x in sv.replace(" ", "").split(",") if x.isdigit()}
            if target["id"] in ids:
                return True
    return False


# ------------------------------------------------------------------ #
# Email notification
# ------------------------------------------------------------------ #

async def _get_smtp_config(db_path: str) -> Optional[dict]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT key, value FROM settings WHERE key LIKE 'smtp_%' OR key='notify_email'"
        ) as cur:
            rows = await cur.fetchall()
    cfg = {k: v for k, v in rows}
    if not cfg.get("smtp_host") or not cfg.get("notify_email"):
        return None
    return {
        "host": cfg["smtp_host"],
        "port": int(cfg.get("smtp_port") or 587),
        "user": cfg.get("smtp_user") or "",
        "password": cfg.get("smtp_password") or "",
        "use_tls": (cfg.get("smtp_tls") or "1") == "1",
        "from_email": cfg.get("smtp_from") or cfg.get("smtp_user") or "",
        "to_email": cfg["notify_email"],
    }


def _split_recipients(raw: str) -> list[str]:
    """Split the notify_email setting into a list of addresses.
    Supports ';' (and Chinese '；'), ',' and whitespace as separators,
    dropping empty entries."""
    parts = re.split(r"[;,，；\s]+", raw or "")
    return [p.strip() for p in parts if p.strip()]


def _send_email_sync(cfg: dict, subject: str, body: str) -> bool:
    try:
        to_list = _split_recipients(cfg["to_email"])
        if not to_list:
            return False
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = cfg["from_email"] or cfg["user"]
        msg["To"] = ", ".join(to_list)
        if cfg["use_tls"]:
            server = smtplib.SMTP(cfg["host"], cfg["port"], timeout=15)
            server.starttls()
        else:
            server = smtplib.SMTP(cfg["host"], cfg["port"], timeout=15)
        if cfg["user"]:
            server.login(cfg["user"], cfg["password"])
        server.sendmail(msg["From"], to_list, msg.as_string())
        server.quit()
        return True
    except Exception as exc:
        logger.error("Email send failed: %s", exc)
        return False


async def _send_email(cfg: dict, subject: str, body: str) -> bool:
    return await asyncio.to_thread(_send_email_sync, cfg, subject, body)


# ------------------------------------------------------------------ #
# History helpers
# ------------------------------------------------------------------ #

async def _get_open_history(db_path: str, alert_id: int, target_id: int) -> Optional[int]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT id FROM alert_history WHERE alert_id=? AND target_id=? AND status='firing' ORDER BY id DESC LIMIT 1",
            (alert_id, target_id),
        ) as cur:
            r = await cur.fetchone()
    return r[0] if r else None


async def _open_history(db_path: str, alert_id: int, target: dict, detail: str, value) -> None:
    now = int(time.time())
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            """INSERT INTO alert_history
               (alert_id, target_id, target_name, status, detail, value, started_at)
               VALUES (?,?,?,?,?,?,?)""",
            (alert_id, target["id"], target.get("name") or "", "firing", detail, value, now),
        )
        await db.commit()


async def _bump_notify(db_path: str, hid: int, now: int) -> None:
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            "UPDATE alert_history SET notify_count=notify_count+1, last_notify=? WHERE id=?",
            (now, hid),
        )
        await db.commit()


async def _recover_history(db_path: str, hid: int, now: int) -> None:
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            "UPDATE alert_history SET status='recovered', recovered_at=? WHERE id=?",
            (now, hid),
        )
        await db.commit()


async def _last_notify(db_path: str, hid: int) -> Optional[int]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT last_notify, notify_count FROM alert_history WHERE id=?", (hid,)
        ) as cur:
            r = await cur.fetchone()
    return r[0] if r else None


# ------------------------------------------------------------------ #
# Main evaluation loop
# ------------------------------------------------------------------ #

async def run_alerter(db_path: str) -> None:
    """Poll rules every POLL_INTERVAL seconds and evaluate each rule+target."""
    while True:
        try:
            await _tick(db_path)
        except Exception as exc:
            logger.error("Alerter tick error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)


async def _tick(db_path: str) -> None:
    now = int(time.time())
    rules = await _fetch_rules(db_path)
    if not rules:
        return
    suppressions = await _fetch_suppressions(db_path, now)
    targets = await _fetch_targets(db_path)
    cfg = await _get_smtp_config(db_path)
    target_by_id = {t["id"]: t for t in targets}

    for rule in rules:
        # Effective-time window check: outside the window the rule is inactive.
        if not _rule_in_window(rule, now):
            # If any target is currently firing under this rule, recover it so
            # history doesn't show a stale "告警中" when the rule goes inactive.
            tids_all = await resolve_target_ids(db_path, rule["scope_type"], rule.get("scope_value") or "")
            for tid in tids_all:
                hid = await _get_open_history(db_path, rule["id"], tid)
                if hid:
                    await _recover_history(db_path, hid, now)
            continue

        tids = await resolve_target_ids(db_path, rule["scope_type"], rule.get("scope_value") or "")
        for tid in tids:
            target = target_by_id.get(tid)
            if not target or not target.get("enabled"):
                continue

            # Suppression only silences email notifications: the alert is still
            # evaluated and recorded in history, just without notifying.
            suppressed = _is_suppressed(target, suppressions)

            samples = await _fetch_recent_samples(db_path, tid, rule.get("window_count") or 60, now)
            triggered, detail, value = _evaluate(rule, samples)

            if triggered:
                hid = await _get_open_history(db_path, rule["id"], tid)
                if hid is None:
                    await _open_history(db_path, rule["id"], target, detail, value)
                    hid = await _get_open_history(db_path, rule["id"], tid)
                    # send immediate notification on new firing
                    await _notify(db_path, cfg, rule, target, detail, value, hid,
                                  is_repeat=False, now=now, suppressed=suppressed)
                else:
                    # already firing -> maybe repeat reminder
                    repeat_min = rule.get("repeat_min") or 0
                    if repeat_min and repeat_min > 0:
                        last = await _last_notify(db_path, hid)
                        if last is None or (now - last) >= repeat_min * 60:
                            await _notify(db_path, cfg, rule, target, detail, value, hid,
                                          is_repeat=True, now=now, suppressed=suppressed)
            else:
                hid = await _get_open_history(db_path, rule["id"], tid)
                if hid:
                    await _recover_history(db_path, hid, now)
                    await _notify(db_path, cfg, rule, target, f"已恢复：{detail}", value, hid,
                                  is_repeat=False, now=now, recover=True, suppressed=suppressed)


async def _notify(db_path: str, cfg, rule, target, detail, value, hid, *, is_repeat, now, recover=False, suppressed=False) -> None:
    if not cfg or suppressed:
        return
    subject = f"[YTPing告警] {rule['name']} - {target.get('name')}"
    if recover:
        subject = f"[YTPing恢复] {rule['name']} - {target.get('name')}"
    body = (
        f"告警规则：{rule['name']}\n"
        f"监控目标：{target.get('name')} ({target.get('address')})\n"
        f"状态：{'已恢复' if recover else ('持续告警' if is_repeat else '触发告警')}\n"
        f"详情：{detail}\n"
        f"时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))}\n"
    )
    ok = await _send_email(cfg, subject, body)
    if ok:
        await _bump_notify(db_path, hid, now)
