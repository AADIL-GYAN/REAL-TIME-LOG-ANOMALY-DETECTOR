"""Core anomaly detection engine.

Pipeline (matches the architecture slides):
  1. Parse a log line (INFO / WARN / ERROR ...).
  2. Append it to a sliding window (collections.deque of timestamped events).
  3. Compute the current error rate  = errors / total  inside the window.
  4. During the learning phase, record rate samples -> baseline (mean, std dev).
  5. In monitoring phase: z = (current_rate - mean) / std_dev.
  6. If z > threshold -> assign severity and raise an alert.
"""
from __future__ import annotations

import math
import re
import statistics
import time
import uuid
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

LEVEL_RE = re.compile(r"\b(DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|FATAL)\b", re.IGNORECASE)
ERROR_LEVELS = {"ERROR", "CRITICAL", "FATAL"}
SEVERITY_RANK = {"low": 1, "medium": 2, "high": 3, "critical": 4}


@dataclass
class DetectorConfig:
    window_seconds: float = 30.0      # sliding window length
    baseline_seconds: float = 60.0    # how long to learn "normal"
    threshold: float = 3.0            # z-score that triggers an alert
    min_events: int = 10              # ignore windows with too little data
    cooldown_seconds: float = 15.0    # min gap between alerts of equal severity
    std_floor: float = 0.01           # avoids divide-by-~zero on very quiet baselines

    def update(self, **kw) -> None:
        for key, val in kw.items():
            if val is None or not hasattr(self, key):
                continue
            setattr(self, key, type(getattr(self, key))(val))


def parse_level(line: str) -> str:
    m = LEVEL_RE.search(line)
    if not m:
        return "INFO"
    lvl = m.group(1).upper()
    return "WARN" if lvl.startswith("WARN") else lvl


def severity_for(z: float) -> str:
    if z >= 8:
        return "critical"
    if z >= 5:
        return "high"
    if z >= 4:
        return "medium"
    return "low"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


class AnomalyDetector:
    def __init__(self, config: Optional[DetectorConfig] = None) -> None:
        self.cfg = config or DetectorConfig()
        self.window: Deque[Tuple[float, str]] = deque()   # (timestamp, level)
        self.level_counts: Counter = Counter()
        self.recent_errors: Deque[str] = deque(maxlen=5)
        self.total_lines = 0
        self.alerts: Deque[dict] = deque(maxlen=200)
        self.reset_baseline()

    # ------------------------------------------------------------------ state
    def reset_baseline(self) -> None:
        self.phase = "learning"
        self.learn_started = time.time()
        self.samples: List[float] = []
        self.mean = 0.0
        self.std = 0.0
        self.in_anomaly = False
        self.last_alert_ts = 0.0
        self.last_severity = None

    # ----------------------------------------------------------------- ingest
    def ingest(self, line: str, now: Optional[float] = None) -> dict:
        now = now or time.time()
        level = parse_level(line)
        self.window.append((now, level))
        self.level_counts[level] += 1
        self.total_lines += 1
        if level in ERROR_LEVELS:
            self.recent_errors.append(line.strip()[:240])
        self._expire(now)
        return {"ts": now, "level": level, "line": line.rstrip("\n")}

    def _expire(self, now: float) -> None:
        """Drop events that fell out of the window (no full re-scan needed)."""
        cutoff = now - self.cfg.window_seconds
        while self.window and self.window[0][0] < cutoff:
            _, lvl = self.window.popleft()
            self.level_counts[lvl] -= 1
            if self.level_counts[lvl] <= 0:
                del self.level_counts[lvl]

    # ------------------------------------------------------------------- tick
    def tick(self, now: Optional[float] = None) -> Tuple[dict, Optional[dict]]:
        """Call ~1x/second. Returns (metric snapshot, alert-or-None)."""
        now = now or time.time()
        self._expire(now)
        total = len(self.window)
        errors = sum(self.level_counts.get(l, 0) for l in ERROR_LEVELS)
        rate = errors / total if total else 0.0
        enough = total >= self.cfg.min_events

        z = 0.0
        alert = None
        progress = 1.0

        if self.phase == "learning":
            elapsed = now - self.learn_started
            progress = min(elapsed / self.cfg.baseline_seconds, 1.0)
            if enough:
                self.samples.append(rate)
            if elapsed >= self.cfg.baseline_seconds:
                if len(self.samples) >= 5:
                    self.mean = statistics.fmean(self.samples)
                    self.std = max(statistics.pstdev(self.samples), self.cfg.std_floor)
                    self.phase = "monitoring"
                else:  # not enough traffic yet -> keep learning
                    self.learn_started = now - self.cfg.baseline_seconds + 5
        elif enough:
            z = (rate - self.mean) / self.std
            alert = self._evaluate(now, z, rate, total, errors)

        metric = {
            "type": "metric",
            "ts": now,
            "rate": round(rate, 4),
            "z": round(z, 3),
            "mean": round(self.mean, 4),
            "std": round(self.std, 4),
            "upper": round(self.mean + self.cfg.threshold * self.std, 4) if self.phase == "monitoring" else None,
            "total": total,
            "errors": errors,
            "phase": self.phase,
            "progress": round(progress, 3),
            "levels": dict(self.level_counts),
            "in_anomaly": self.in_anomaly,
            "total_lines": self.total_lines,
            "lps": round(total / self.cfg.window_seconds, 2),
        }
        return metric, alert

    def _evaluate(self, now, z, rate, total, errors) -> Optional[dict]:
        if z > self.cfg.threshold:
            sev = severity_for(z)
            escalated = self.last_severity is None or SEVERITY_RANK[sev] > SEVERITY_RANK[self.last_severity]
            cooled = now - self.last_alert_ts >= self.cfg.cooldown_seconds
            first = not self.in_anomaly
            self.in_anomaly = True
            if first or escalated or cooled:
                self.last_alert_ts = now
                self.last_severity = sev
                return self._make_alert(now, sev, z, rate, total, errors)
        elif self.in_anomaly and z < self.cfg.threshold * 0.6:
            self.in_anomaly = False
            self.last_severity = None
            return self._make_alert(now, "resolved", z, rate, total, errors)
        return None

    def _make_alert(self, now, sev, z, rate, total, errors) -> dict:
        if sev == "resolved":
            msg = f"Error rate back to normal ({rate:.1%}, z={z:.1f})"
        else:
            msg = (f"Error rate {rate:.1%} is {z:.1f}σ above baseline "
                   f"({self.mean:.1%} ± {self.std:.1%})")
        alert = {
            "type": "alert",
            "id": uuid.uuid4().hex[:8],
            "ts": now,
            "time": _iso(now),
            "severity": sev,
            "z": round(z, 2),
            "rate": round(rate, 4),
            "mean": round(self.mean, 4),
            "std": round(self.std, 4),
            "threshold": self.cfg.threshold,
            "window_total": total,
            "window_errors": errors,
            "message": msg,
            "samples": list(self.recent_errors) if sev != "resolved" else [],
        }
        self.alerts.appendleft(alert)
        return alert

    def clear_alerts(self) -> None:
        self.alerts.clear()

    def config_dict(self) -> dict:
        return asdict(self.cfg)
