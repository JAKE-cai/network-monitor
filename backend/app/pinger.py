"""
Ping manager: launches one asyncio task per enabled target, collects results
and batch-inserts them into the database for high throughput.

Supports four probe types:
  - icmp : system `ping` (raw ICMP)
  - tcp  : TCP connect handshake latency
  - udp  : UDP datagram + wait for a reply (timeout => loss)
  - http : HTTP(S) GET response latency
"""

import asyncio
import logging
import re
import time
import urllib.parse
from typing import Dict, List, Optional, Tuple

import aiosqlite

logger = logging.getLogger(__name__)

BATCH_SIZE = 500       # max rows per INSERT
BATCH_INTERVAL = 1.0   # seconds between flushes


class PingManager:
    def __init__(self, db_path: str, on_result=None) -> None:
        self.db_path = db_path
        self._on_result = on_result   # callable(target_id, ts, latency_ms, is_loss)
        self._tasks: Dict[int, asyncio.Task] = {}
        self._queue: asyncio.Queue[Tuple] = asyncio.Queue(maxsize=20000)
        self._writer_task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        self._running = True
        self._writer_task = asyncio.create_task(
            self._batch_writer(), name="ping-batch-writer"
        )
        await self._reload_targets()
        logger.info("PingManager started")

    async def stop(self) -> None:
        self._running = False
        for task in list(self._tasks.values()):
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        if self._writer_task:
            self._writer_task.cancel()
        logger.info("PingManager stopped")

    # ------------------------------------------------------------------ #
    # Target management (called from API routers)
    # ------------------------------------------------------------------ #

    async def _reload_targets(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT id, address, interval_ms, probe_type, port FROM targets WHERE enabled=1"
            ) as cur:
                rows = [dict(r) for r in await cur.fetchall()]

        active_ids = {r["id"] for r in rows}
        for tid in list(self._tasks):
            if tid not in active_ids:
                self._tasks[tid].cancel()
                del self._tasks[tid]

        for r in rows:
            if r["id"] not in self._tasks:
                self._launch(
                    r["id"], r["address"], r["interval_ms"],
                    r.get("probe_type") or "icmp", r.get("port"),
                )

    def add_target(
        self,
        target_id: int,
        address: str,
        interval_ms: int = 1000,
        probe_type: str = "icmp",
        port: Optional[int] = None,
    ) -> None:
        if target_id in self._tasks:
            self._tasks[target_id].cancel()
        self._launch(target_id, address, interval_ms, probe_type, port)

    def remove_target(self, target_id: int) -> None:
        if target_id in self._tasks:
            self._tasks[target_id].cancel()
            del self._tasks[target_id]

    def _launch(
        self,
        target_id: int,
        address: str,
        interval_ms: int,
        probe_type: str = "icmp",
        port: Optional[int] = None,
    ) -> None:
        task = asyncio.create_task(
            self._ping_loop(target_id, address, interval_ms, probe_type, port),
            name=f"ping-{target_id}",
        )
        self._tasks[target_id] = task

    # ------------------------------------------------------------------ #
    # Ping loop per target
    # ------------------------------------------------------------------ #

    async def _ping_loop(
        self,
        target_id: int,
        address: str,
        interval_ms: int,
        probe_type: str = "icmp",
        port: Optional[int] = None,
    ) -> None:
        interval = interval_ms / 1000.0
        while True:
            t0 = time.monotonic()
            try:
                latency = await self._probe_once(
                    probe_type, address, port, timeout=min(interval, 2.0)
                )
                ts = int(time.time())
                is_loss = 1 if latency is None else 0

                # Notify SSE subscribers immediately (lowest possible latency)
                if self._on_result is not None:
                    try:
                        self._on_result(target_id, ts, latency, is_loss)
                    except Exception as cb_exc:
                        logger.debug("on_result callback error: %s", cb_exc)

                try:
                    self._queue.put_nowait((target_id, ts, latency, is_loss))
                except asyncio.QueueFull:
                    logger.warning("Ping queue full – dropping result for target %d", target_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Ping loop error target=%d: %s", target_id, exc)

            elapsed = time.monotonic() - t0
            await asyncio.sleep(max(0.0, interval - elapsed))

    @classmethod
    async def _probe_once(
        cls,
        probe_type: str,
        address: str,
        port: Optional[int] = None,
        timeout: float = 1.0,
    ) -> Optional[float]:
        """Dispatch to the right probe. Returns latency in ms or None on loss."""
        try:
            pt = (probe_type or "icmp").lower()
            if pt == "tcp":
                return await cls._tcp_once(address, port, timeout)
            if pt == "udp":
                return await cls._udp_once(address, port, timeout)
            if pt == "http":
                return await cls._http_once(address, timeout)
            return await cls._icmp_once(address, timeout)
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    @staticmethod
    async def _icmp_once(address: str, timeout: float = 1.0) -> Optional[float]:
        """ICMP ping via the system `ping` binary."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "ping", "-c", "1", "-W", str(max(1, int(timeout))),
                address,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                stdout, _ = await asyncio.wait_for(
                    proc.communicate(), timeout=timeout + 1.5
                )
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except Exception:
                    pass
                return None

            if proc.returncode == 0:
                m = re.search(r"time[<=](\d+\.?\d*)\s*ms", stdout.decode())
                if m:
                    return float(m.group(1))
            return None
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    @staticmethod
    def _split_host(host: str, port: Optional[int]) -> Tuple[str, Optional[int]]:
        """Normalise a host string that may be 'host', 'host:port' or '[v6]:port'.
        Returns (hostname, port)."""
        h = (host or "").strip()
        if not h:
            return h, port
        # [IPv6]:port
        if h.startswith("["):
            if "]" in h:
                rest = h[h.index("]") + 1:]
                hostname = h[1:h.index("]")]
                if rest.startswith(":") and port is None:
                    try:
                        port = int(rest[1:])
                    except ValueError:
                        pass
                return hostname, port
            return h, port
        # host:port
        if ":" in h and not h.startswith(":"):
            # Only split on the last colon (hostnames may contain dots but IPv6
            # bare form isn't used here since we require [v6])
            head, _, tail = h.rpartition(":")
            if tail.isdigit():
                if port is None:
                    port = int(tail)
                return head, port
        return h, port

    @classmethod
    async def _tcp_once(cls, host: str, port: int, timeout: float = 1.0) -> Optional[float]:
        """TCP connect handshake latency (ms). None on failure/timeout."""
        hostname, port = cls._split_host(host, port)
        if port is None:
            return None
        try:
            t0 = time.monotonic()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(hostname, port), timeout=timeout
            )
            elapsed = (time.monotonic() - t0) * 1000.0
            try:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
            except Exception:
                pass
            return elapsed
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    @classmethod
    async def _udp_once(cls, host: str, port: int, timeout: float = 1.0) -> Optional[float]:
        """UDP probe: send a datagram, wait briefly for any reply. Timeout => loss."""
        hostname, port = cls._split_host(host, port)
        if port is None:
            return None

        loop = asyncio.get_running_loop()
        # A queue to receive the first datagram
        recvq: asyncio.Queue = asyncio.Queue(maxsize=1)

        class _Proto(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                try:
                    recvq.put_nowait(data)
                except Exception:
                    pass
            def error_received(self, exc):
                try:
                    recvq.put_nowait(None)
                except Exception:
                    pass

        transport, _proto = await loop.create_datagram_endpoint(
            lambda: _Proto(), remote_addr=(hostname, port)
        )
        try:
            t0 = time.monotonic()
            transport.sendto(b"ytping-probe")
            try:
                await asyncio.wait_for(recvq.get(), timeout=timeout)
                return (time.monotonic() - t0) * 1000.0
            except asyncio.TimeoutError:
                return None
        finally:
            transport.close()

    @staticmethod
    async def _http_once(url: str, timeout: float = 1.0) -> Optional[float]:
        """HTTP(S) GET latency (ms). None on error/timeout/4xx/5xx."""
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in ("http", "https"):
            return None
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        host = parsed.hostname or ""
        ssl = parsed.scheme == "https"
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        try:
            t0 = time.monotonic()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=ssl), timeout=timeout
            )
            req = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                "User-Agent: ytping\r\n"
                "Connection: close\r\n\r\n"
            )
            writer.write(req.encode("latin-1"))
            await writer.drain()

            # Read response status line + headers
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=timeout)
                status = int(line.split()[1])
                # Read remaining headers (up to a cap) to get the real status
                header_bytes = 0
                while header_bytes < 64 * 1024:
                    h = await asyncio.wait_for(reader.readline(), timeout=timeout)
                    if not h or h in (b"\r\n", b"\n"):
                        break
                    header_bytes += len(h)
            except Exception:
                status = 0

            try:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
            except Exception:
                pass

            elapsed = (time.monotonic() - t0) * 1000.0
            if 200 <= status < 400:
                return elapsed
            return None
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    # Batch writer
    # ------------------------------------------------------------------ #

    async def _batch_writer(self) -> None:
        while self._running or not self._queue.empty():
            batch: List[Tuple] = []
            deadline = time.monotonic() + BATCH_INTERVAL

            while time.monotonic() < deadline and len(batch) < BATCH_SIZE:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                    batch.append(item)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    break

            if batch:
                try:
                    async with aiosqlite.connect(self.db_path) as db:
                        await db.executemany(
                            "INSERT INTO ping_results (target_id, ts, latency_ms, is_loss) "
                            "VALUES (?, ?, ?, ?)",
                            batch,
                        )
                        await db.commit()
                except Exception as exc:
                    logger.error("Batch DB write failed: %s", exc)
            else:
                await asyncio.sleep(0.05)
