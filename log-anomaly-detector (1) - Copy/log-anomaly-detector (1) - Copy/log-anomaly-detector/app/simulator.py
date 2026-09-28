"""Generates realistic application log traffic so the demo works out of the box.
Point LOG_FILE at a real log and set SIMULATE=0 to monitor production logs instead."""
from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime
from pathlib import Path

SERVICES = ["auth-service", "payment-api", "order-worker", "inventory", "gateway", "notifier"]
INFO = ["Request completed in {ms}ms", "User {u} logged in", "Cache hit for key sess:{u}",
        "Order #{o} created", "Health check OK", "Job {o} dequeued", "Webhook delivered to partner-{u}"]
DEBUG = ["Connection pool size={u}", "Serialized payload ({ms} bytes)", "GC pause {ms}ms"]
WARN = ["Slow query detected ({ms}ms)", "Retrying upstream call (attempt 2/3)", "Rate limit at 85% for client {u}",
        "Deprecated endpoint /v1/orders hit"]
ERROR = ["Database connection timeout after {ms}ms", "Payment gateway returned 502", "NullPointerException in OrderHandler",
         "Failed to publish message to queue: broker unavailable", "Upstream service unreachable: inventory",
         "TLS handshake failed with partner-{u}", "OOMKilled: worker-{u} exceeded memory limit"]
CRIT = ["Primary DB unreachable - failover initiated", "Disk usage at 99% on /var/data"]


class LogSimulator:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.enabled = True
        self.base_error_p = 0.04
        self.burst_until = 0.0
        self.burst_p = 0.55
        self._n = 0

    @property
    def bursting(self) -> bool:
        return time.time() < self.burst_until

    def burst(self, seconds: float = 20, intensity: float = 0.55) -> None:
        self.burst_until = time.time() + seconds
        self.burst_p = min(max(intensity, 0.1), 0.95)

    def _line(self) -> str:
        p = self.burst_p if self.bursting else self.base_error_p
        r = random.random()
        if r < p:
            lvl, pool = ("CRITICAL", CRIT) if random.random() < 0.06 else ("ERROR", ERROR)
        else:
            r2 = random.random()
            lvl, pool = (("DEBUG", DEBUG) if r2 < 0.2 else ("WARN", WARN) if r2 < 0.32 else ("INFO", INFO))
        msg = random.choice(pool).format(ms=random.randint(3, 4000), u=random.randint(100, 999),
                                         o=random.randint(10000, 99999))
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        return f"{ts} {lvl:<8} [{random.choice(SERVICES)}] {msg}\n"

    async def run(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        while True:
            if self.enabled:
                n = random.randint(4, 9) if not self.bursting else random.randint(8, 16)
                with self.path.open("a", encoding="utf-8") as fh:
                    fh.writelines(self._line() for _ in range(n))
            await asyncio.sleep(1.0)
