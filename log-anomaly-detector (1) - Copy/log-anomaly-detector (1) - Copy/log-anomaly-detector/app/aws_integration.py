"""Optional AWS CloudWatch Logs + SNS alerting via boto3.

If boto3, credentials or config are missing the notifier simply disables itself
and the system keeps working locally (graceful fallback).

Environment variables:
  AWS_REGION            e.g. ap-south-1  (required to enable)
  CLOUDWATCH_LOG_GROUP  e.g. /anomaly-detector/alerts
  CLOUDWATCH_LOG_STREAM e.g. alerts (default: "alerts")
  SNS_TOPIC_ARN         topic to publish high/critical alerts to
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

log = logging.getLogger("aws")

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:  # boto3 not installed
    boto3 = None
    BotoCoreError = ClientError = Exception


class AwsNotifier:
    SNS_SEVERITIES = {"high", "critical"}

    def __init__(self) -> None:
        self.region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        self.group = os.getenv("CLOUDWATCH_LOG_GROUP")
        self.stream = os.getenv("CLOUDWATCH_LOG_STREAM", "alerts")
        self.topic = os.getenv("SNS_TOPIC_ARN")
        self.logs = None
        self.sns = None
        self.last_error = None
        self.sent = {"cloudwatch": 0, "sns": 0}
        self.reason = ""
        self._setup()

    def _setup(self) -> None:
        if boto3 is None:
            self.reason = "boto3 not installed - running local-only"
            return
        if not self.region or not (self.group or self.topic):
            self.reason = "AWS not configured - running local-only"
            return
        try:
            session = boto3.Session(region_name=self.region)
            if session.get_credentials() is None:
                self.reason = "No AWS credentials found - running local-only"
                return
            if self.group:
                self.logs = session.client("logs")
                self._ensure_stream()
            if self.topic:
                self.sns = session.client("sns")
            self.reason = "Connected"
        except Exception as exc:  # noqa: BLE001
            self.logs = self.sns = None
            self.last_error = str(exc)
            self.reason = f"AWS setup failed - running local-only ({exc.__class__.__name__})"

    def _ensure_stream(self) -> None:
        try:
            self.logs.create_log_group(logGroupName=self.group)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise
        try:
            self.logs.create_log_stream(logGroupName=self.group, logStreamName=self.stream)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise

    @property
    def enabled(self) -> bool:
        return bool(self.logs or self.sns)

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "cloudwatch": bool(self.logs),
            "sns": bool(self.sns),
            "region": self.region,
            "reason": self.reason,
            "last_error": self.last_error,
            "sent": self.sent,
        }

    async def notify(self, alert: dict) -> None:
        if not self.enabled or alert["severity"] == "resolved":
            return
        await asyncio.to_thread(self._send, alert)

    def _send(self, alert: dict) -> None:
        payload = json.dumps({k: alert[k] for k in
                              ("id", "time", "severity", "z", "rate", "mean", "std", "message")})
        try:
            if self.logs:
                self.logs.put_log_events(
                    logGroupName=self.group, logStreamName=self.stream,
                    logEvents=[{"timestamp": int(time.time() * 1000), "message": payload}])
                self.sent["cloudwatch"] += 1
            if self.sns and alert["severity"] in self.SNS_SEVERITIES:
                self.sns.publish(TopicArn=self.topic,
                                 Subject=f"[{alert['severity'].upper()}] Log anomaly detected"[:100],
                                 Message=alert["message"] + "\n\n" + payload)
                self.sent["sns"] += 1
            self.last_error = None
        except (BotoCoreError, ClientError, Exception) as exc:  # noqa: BLE001
            self.last_error = str(exc)
            log.warning("AWS notify failed: %s", exc)
