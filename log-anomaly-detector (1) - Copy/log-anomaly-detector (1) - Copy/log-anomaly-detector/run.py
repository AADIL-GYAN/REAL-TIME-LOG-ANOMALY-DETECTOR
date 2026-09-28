import os
import sys
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    print(f"\n  Log Anomaly Detector -> open http://localhost:{port}\n", flush=True)
    try:
        uvicorn.run("app.main:app", host=os.getenv("HOST", "0.0.0.0"), port=port, log_level="info")
    except OSError as exc:
        sys.exit(f"Could not start on port {port}: {exc}\nTry: PORT=8010 python run.py")
