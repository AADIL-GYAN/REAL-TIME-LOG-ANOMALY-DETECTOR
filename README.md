# Real-Time Log Anomaly Detector

FastAPI + WebSockets backend with a live dashboard. Tails a log file, learns what "normal" looks like,
and raises alerts (with severity) when the error rate deviates statistically.

## Quick start
```bash
pip install -r requirements.txt
python run.py
# open http://localhost:8000
```
The built-in simulator writes realistic traffic to `logs/app.log`. Wait ~60 s for the baseline to learn,
then click **⚡ Simulate error spike**.

## How it works (matches the deck)
1. **Log monitoring** – `tail -f` style follower reads only newly appended bytes (handles rotation).
2. **Sliding window** – `collections.deque` of `(timestamp, level)`; old events expire, no full re-scan.
3. **Baseline learning** – error-rate samples for `BASELINE_SECONDS` -> mean & std dev.
4. **Z-score** – `z = (current_rate - mean) / std_dev`. Alert when `z > threshold` (default 3σ).
5. **Severity** – low (>threshold), medium (≥4), high (≥5), critical (≥8). A "resolved" event fires on recovery.
6. **Real-time feed** – alerts + metrics + log lines pushed over `/ws` to the dashboard.
7. **AWS (optional)** – alerts to CloudWatch Logs; high/critical also to SNS. Falls back to local-only if unset.

## Monitor a real log
```bash
SIMULATE=0 LOG_FILE=/var/log/myapp.log python run.py
```
Any line containing DEBUG/INFO/WARN/ERROR/CRITICAL/FATAL is understood.

## AWS (optional)
```bash
export AWS_REGION=ap-south-1
export CLOUDWATCH_LOG_GROUP=/anomaly-detector/alerts
export SNS_TOPIC_ARN=arn:aws:sns:ap-south-1:123456789012:log-alerts
python run.py      # uses your normal AWS credentials (env vars, ~/.aws, IAM role)
```
IAM needs: `logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents, sns:Publish`.

## API
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/status` | config, phase, AWS + simulator state |
| GET | `/api/alerts?limit=` | recent alerts |
| GET | `/api/metrics?limit=` | recent per-second metrics |
| POST | `/api/config` | `{threshold, window_seconds, baseline_seconds, min_events}` |
| POST | `/api/relearn` | reset baseline |
| POST | `/api/simulate/burst` | `{seconds, intensity}` |
| POST | `/api/simulate/toggle` | pause/resume simulator |
| POST | `/api/inject` | `{line}` append a line to the log |
| POST | `/api/alerts/clear` | clear feed |
| WS | `/ws` | snapshot, metric, logs, alert, state, aws |

Interactive docs: http://localhost:8000/docs
