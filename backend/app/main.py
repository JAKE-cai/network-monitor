import asyncio
import logging
import os
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from .alerts import run_alerter
from .auth import ensure_admin, make_auth_dependency
from .compressor import run_compressor
from .database import DB_PATH, init_db
from .routers import metrics, schedules, targets
from .routers import alerts as alerts_router
from .routers import auth as auth_router
from .scheduler import run_schedule_checker
from .state import ping_manager

_require_auth = make_auth_dependency(lambda: DB_PATH)

_PROD = os.environ.get("ENV", "production").lower() == "production"


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        return response

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

_ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "").split(",")
_ALLOWED_ORIGINS = [o.strip() for o in _ALLOWED_ORIGINS if o.strip()] or ["*"]

app = FastAPI(
    title="YTPing-网络质量监控",
    docs_url=None if _PROD else "/api/docs",
    redoc_url=None,
)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(auth_router.router)
app.include_router(targets.router, dependencies=[Depends(_require_auth)])
app.include_router(schedules.router, dependencies=[Depends(_require_auth)])
app.include_router(metrics.router, dependencies=[Depends(_require_auth)])
app.include_router(alerts_router.router, dependencies=[Depends(_require_auth)])


@app.get("/health", include_in_schema=False)
async def health():
    return JSONResponse({"ok": True})


@app.on_event("startup")
async def on_startup() -> None:
    await init_db()
    await ensure_admin(DB_PATH)
    await ping_manager.start()
    asyncio.create_task(run_compressor(DB_PATH), name="compressor")
    asyncio.create_task(run_schedule_checker(DB_PATH), name="schedule-checker")
    asyncio.create_task(run_alerter(DB_PATH), name="alerter")
    logger.info("Application started")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await ping_manager.stop()
    logger.info("Application stopped")


# ------------------------------------------------------------------ #
# Serve the single-page frontend
# ------------------------------------------------------------------ #

# Container layout: /app/app/main.py  →  /app/frontend
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

if FRONTEND_DIR.exists():
    # Serve /static/** assets (JS, CSS, etc.)
    static_dir = FRONTEND_DIR / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    async def root():
        return FileResponse(str(FRONTEND_DIR / "index.html"))

    _FRONTEND_ROOT = FRONTEND_DIR.resolve()

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_fallback(full_path: str):
        candidate = (FRONTEND_DIR / full_path).resolve()
        # Prevent directory traversal outside the frontend root
        if candidate.is_relative_to(_FRONTEND_ROOT) and candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(FRONTEND_DIR / "index.html"))
