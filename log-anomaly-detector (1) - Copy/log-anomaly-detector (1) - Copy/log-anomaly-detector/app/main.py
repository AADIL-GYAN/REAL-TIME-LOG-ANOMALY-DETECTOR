"""Real-Time Log Anomaly Detector - FastAPI backend.

Run:  python run.py     (then open http://localhost:8000)
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Deque, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .aws_integration import AwsNotifier
from .detector import AnomalyDetector, DetectorConfig
from .simulator import LogSimulator

BASE = Path(__file__).resolve().parent.parent
LOG_FILE = Path(os.getenv("LOG_FILE", BASE / "logs" / "app.log"))
SIMULATE = os.getenv("SIMULATE", "1") != "0"

detector = AnomalyDetector(DetectorConfig(
    window_seconds=float(os.getenv("WINDOW_SECONDS", 30)),
    baseline_seconds=float(os.getenv("BASELINE_SECONDS", 60)),
    threshold=float(os.getenv("ZSCORE_THRESHOLD", 3.0)),
))
aws = AwsNotifier()
simulator = LogSimulator(LOG_FILE)
recent_logs: Deque[dict] = deque(maxlen=150)
history: Deque[dict] = deque(maxlen=600)  # last 10 minutes of metrics
started_at = time.time()


class Hub:
    """Tracks WebSocket clients and broadcasts JSON messages."""

    def __init__(self) -> None:
        self.clients: Set[WebSocket] = set()

    async def broadcast(self, msg: dict) -> None:
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(msg)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


hub = Hub()


# ------------------------------------------------------------------ workers
async def tail_log() -> None:
    """'tail -f' style follower: only reads newly appended bytes."""
    pos: Optional[int] = None
    buf = ""
    while True:
        try:
            if not LOG_FILE.exists():
                pos = 0
            else:
                size = LOG_FILE.stat().st_size
                if pos is None:
                    pos = size                      # start at end, like tail -f
                if size < pos:                      # truncated / rotated
                    pos, buf = 0, ""
                if size > pos:
                    with LOG_FILE.open("r", encoding="utf-8", errors="replace") as fh:
                        fh.seek(pos)
                        chunk = fh.read()
                        pos = fh.tell()
                    buf += chunk
                    *lines, buf = buf.split("\n")
                    batch = [detector.ingest(l) for l in lines if l.strip()]
                    if batch:
                        recent_logs.extend(batch)
                        await hub.broadcast({"type": "logs", "lines": batch[-60:]})
        except Exception as exc:  # noqa: BLE001
            print("tailer error:", exc)
        await asyncio.sleep(0.2)


async def ticker() -> None:
    while True:
        await asyncio.sleep(1.0)
        metric, alert = detector.tick()
        history.append(metric)
        await hub.broadcast(metric)
        if alert:
            await hub.broadcast(alert)
            asyncio.create_task(aws.notify(alert))
            await hub.broadcast({"type": "aws", **aws.status()})


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    tasks = [asyncio.create_task(tail_log()), asyncio.create_task(ticker())]
    if SIMULATE:
        LOG_FILE.write_text("")  # fresh file for the demo
        tasks.append(asyncio.create_task(simulator.run()))
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="Real-Time Log Anomaly Detector", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------- API
def state() -> dict:
    return {
        "config": detector.config_dict(),
        "phase": detector.phase,
        "aws": aws.status(),
        "simulator": {"available": SIMULATE, "enabled": simulator.enabled, "bursting": simulator.bursting},
        "log_file": str(LOG_FILE),
        "uptime": round(time.time() - started_at),
        "total_lines": detector.total_lines,
    }


class ConfigIn(BaseModel):
    threshold: Optional[float] = None
    window_seconds: Optional[float] = None
    baseline_seconds: Optional[float] = None
    min_events: Optional[int] = None
    cooldown_seconds: Optional[float] = None


class BurstIn(BaseModel):
    seconds: float = 20
    intensity: float = 0.55


class LineIn(BaseModel):
    line: str


@app.get("/api/status")
async def api_status():
    return state()


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Return empty 204 to stop browsers spamming 404 for favicon."""
    from fastapi.responses import Response
    return Response(status_code=204)


@app.get("/api/alerts")
async def api_alerts(limit: int = 50):
    return list(detector.alerts)[:limit]


@app.get("/api/metrics")
async def api_metrics(limit: int = 120):
    return list(history)[-limit:]


@app.post("/api/config")
async def api_config(body: ConfigIn):
    data = (body.model_dump(exclude_none=True) if hasattr(body, 'model_dump') else body.dict(exclude_none=True))
    if "threshold" in data:
        data["threshold"] = min(max(data["threshold"], 1.0), 10.0)
    if "window_seconds" in data:
        data["window_seconds"] = min(max(data["window_seconds"], 5), 300)
    if "baseline_seconds" in data:
        data["baseline_seconds"] = min(max(data["baseline_seconds"], 10), 900)
    detector.cfg.update(**data)
    await hub.broadcast({"type": "state", **state()})
    return state()


@app.post("/api/relearn")
async def api_relearn():
    detector.reset_baseline()
    await hub.broadcast({"type": "state", **state()})
    return state()


@app.post("/api/alerts/clear")
async def api_clear():
    detector.clear_alerts()
    await hub.broadcast({"type": "cleared"})
    return {"ok": True}


@app.post("/api/simulate/burst")
async def api_burst(body: BurstIn = BurstIn()):
    if not SIMULATE:
        return {"ok": False, "error": "Simulator disabled (SIMULATE=0)"}
    simulator.burst(body.seconds, body.intensity)
    await hub.broadcast({"type": "state", **state()})
    return {"ok": True}


@app.post("/api/simulate/toggle")
async def api_toggle():
    simulator.enabled = not simulator.enabled
    await hub.broadcast({"type": "state", **state()})
    return state()["simulator"]


@app.post("/api/inject")
async def api_inject(body: LineIn):
    """Append a custom line to the monitored log file (handy for testing)."""
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(body.line.strip() + "\n")
    return {"ok": True}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    hub.clients.add(ws)
    try:
        await ws.send_json({
            "type": "snapshot", **state(),
            "history": list(history)[-300:],
            "alerts": list(detector.alerts)[:50],
            "logs": list(recent_logs)[-60:],
        })
        while True:
            await ws.receive_text()  # keep-alive; client pings
    except WebSocketDisconnect:
        pass
    finally:
        hub.clients.discard(ws)


app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")


@app.get("/")
async def index():
    return FileResponse(BASE / "static" / "index.html")
