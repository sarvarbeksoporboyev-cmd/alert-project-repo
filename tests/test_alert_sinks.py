from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.error import URLError

from alert_project.alerts import CompositeAlertSink, ListAlertSink, NullAlertSink, WebhookAlertSink
from alert_project.models import Alert


class _DummyResponse:
    status = 200

    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        _ = (exc_type, exc, tb)
        return False

    def getcode(self) -> int:
        return 200


def _sample_alert() -> Alert:
    return Alert(
        rule_name="test_rule",
        triggered_at=datetime.now(timezone.utc),
        message="Test alert",
        metadata={"count": 11},
    )


def test_composite_sink_fans_out_to_all_sinks() -> None:
    sink_one = ListAlertSink()
    sink_two = ListAlertSink()
    composite = CompositeAlertSink([sink_one, sink_two])

    alerts = [_sample_alert()]
    composite.send(alerts)

    assert len(sink_one.alerts) == 1
    assert len(sink_two.alerts) == 1


def test_webhook_sink_posts_alerts(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return _DummyResponse()

    monkeypatch.setattr("alert_project.alerts.urlopen", fake_urlopen)

    sink = WebhookAlertSink("https://example.com/hooks/alerts", timeout_seconds=3.0, max_retries=0)
    sink.send([_sample_alert()])

    assert captured["url"] == "https://example.com/hooks/alerts"
    assert captured["timeout"] == 3.0
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert len(payload["alerts"]) == 1
    assert payload["alerts"][0]["rule_name"] == "test_rule"


def test_webhook_sink_retries_until_success(monkeypatch) -> None:
    attempts = {"count": 0}

    def flaky_urlopen(request, timeout):
        _ = (request, timeout)
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise URLError("temporary failure")
        return _DummyResponse()

    monkeypatch.setattr("alert_project.alerts.urlopen", flaky_urlopen)

    sink = WebhookAlertSink(
        url="https://example.com/hooks/alerts",
        timeout_seconds=2.0,
        max_retries=2,
        backoff_seconds=0.0,
    )
    sink.send([_sample_alert()])

    assert attempts["count"] == 3


def test_null_sink_accepts_alerts() -> None:
    sink = NullAlertSink()
    sink.send([_sample_alert()])