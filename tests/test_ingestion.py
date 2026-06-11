"""
Unit tests for the ingestion webhook handler.

These tests run the FastAPI app entirely in-memory using TestClient —
no real Kafka broker or network is involved. The Kafka producer is
replaced with a MagicMock so we can assert "the app TRIED to publish"
without anything actually being published.
"""

import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import main as ingestion_main
from main import app, verify_signature


@pytest.fixture()
def client():
    """
    A test client with the Kafka producer mocked out.

    We patch the module-level get_producer function so the endpoint
    receives our MagicMock instead of opening a real broker connection.
    """
    mock_producer = MagicMock()
    with patch.object(ingestion_main, "get_producer", return_value=mock_producer):
        yield TestClient(app), mock_producer


@pytest.fixture()
def signed_client():
    """Like `client`, but with signature verification enabled."""
    mock_producer = MagicMock()
    with patch.object(ingestion_main, "get_producer", return_value=mock_producer), \
         patch.object(ingestion_main.settings, "webhook_secret", "supersecret"):
        yield TestClient(app), mock_producer


def _sign(secret: str, body: bytes) -> str:
    """Compute the signature header the provider would send."""
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def test_health(client):
    tc, _ = client
    resp = tc.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_webhook_accepted_when_no_secret_configured(client):
    """With no secret configured (dev mode), any payload is accepted."""
    tc, mock_producer = client
    payload = {"eventType": "VehicleLocation", "eventId": "evt-001"}
    resp = tc.post("/webhooks/telemetry", content=json.dumps(payload))
    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"
    mock_producer.produce.assert_called_once()


def test_webhook_invalid_json_rejected(client):
    tc, _ = client
    resp = tc.post("/webhooks/telemetry", content=b"this is not json {")
    assert resp.status_code == 400


def test_webhook_valid_signature_accepted(signed_client):
    tc, mock_producer = signed_client
    body = json.dumps({"eventId": "evt-002"}).encode()
    resp = tc.post(
        "/webhooks/telemetry",
        content=body,
        headers={"X-Webhook-Signature": _sign("supersecret", body)},
    )
    assert resp.status_code == 200
    mock_producer.produce.assert_called_once()


def test_webhook_invalid_signature_rejected(signed_client):
    tc, _ = signed_client
    body = json.dumps({"eventId": "evt-003"}).encode()
    resp = tc.post(
        "/webhooks/telemetry",
        content=body,
        headers={"X-Webhook-Signature": "sha256=definitely-wrong"},
    )
    assert resp.status_code == 401


def test_webhook_missing_signature_rejected(signed_client):
    tc, _ = signed_client
    resp = tc.post("/webhooks/telemetry", content=b"{}")
    assert resp.status_code == 401


def test_verify_signature_roundtrip():
    """The pure function should accept its own output and reject others."""
    body = b'{"hello": "world"}'
    good = _sign("s3cret", body)
    assert verify_signature("s3cret", body, good) is True
    assert verify_signature("wrong-secret", body, good) is False
    assert verify_signature("s3cret", b"tampered body", good) is False
