"""
Unit tests for the event transformers.

These are pure-function tests: build a fake envelope (the structure the
ingestion service wraps every payload in), pass it to transform(), and
check the flattened output. No external services needed.
"""

from transformer import transform


def _envelope(event_type: str, data: dict) -> dict:
    """Build a minimal envelope like the ingestion service produces."""
    return {
        "ingestion_id": "test-uuid",
        "ingested_at": 1_700_000_000_000,
        "payload": {
            "eventId": "evt-test-001",
            "eventType": event_type,
            "eventMs": 1_700_000_000_000,
            "orgId": "org-123",
            "data": data,
        },
    }


def test_vehicle_location():
    env = _envelope("VehicleLocation", {
        "id": "veh-1",
        "name": "Truck Alpha",
        "location": {"latitude": 37.77, "longitude": -122.41, "speedMilesPerHour": 65.0},
    })
    rec = transform(env)
    assert rec["vehicle_id"] == "veh-1"
    assert rec["speed_mph"] == 65.0
    assert rec["event_type"] == "VehicleLocation"


def test_vehicle_location_missing_optional_fields():
    """Sparse device data (e.g. no GPS fix) should produce Nones, not crash."""
    env = _envelope("VehicleLocation", {"id": "veh-1"})
    rec = transform(env)
    assert rec["latitude"] is None
    assert rec["speed_mph"] is None


def test_safety_event():
    env = _envelope("SafetyEvent", {
        "vehicleId": "veh-2",
        "driverId": "drv-99",
        "behaviorLabel": "harshBraking",
        "severity": "high",
        "maxAccelerationG": 0.82,
    })
    rec = transform(env)
    assert rec["behavior_label"] == "harshBraking"
    assert rec["severity"] == "high"


def test_geofence_entry_and_exit():
    for event_type, expected_direction in [("GeofenceEntry", "entry"), ("GeofenceExit", "exit")]:
        env = _envelope(event_type, {"vehicleId": "veh-3", "geofenceId": "geo-1", "geofenceName": "Depot"})
        rec = transform(env)
        assert rec["direction"] == expected_direction


def test_unknown_event_type_returns_none():
    """Unknown types must return None so the caller can dead-letter them."""
    env = _envelope("SomeFutureEventType", {})
    assert transform(env) is None


def test_driver_hos():
    env = _envelope("DriverHOS", {
        "driverId": "drv-5",
        "driverName": "Jane Doe",
        "currentDutyStatus": "driving",
        "shiftDriveRemainingMs": 7_200_000,
        "shiftRemainingMs": 36_000_000,
    })
    rec = transform(env)
    assert rec["duty_status"] == "driving"
    assert rec["driver_name"] == "Jane Doe"


def test_vehicle_diagnostic():
    env = _envelope("VehicleDiagnostic", {
        "vehicleId": "veh-9",
        "dtcShortCode": "P0301",
        "dtcDescription": "Cylinder 1 misfire detected",
        "isActive": True,
    })
    rec = transform(env)
    assert rec["dtc_short_code"] == "P0301"
    assert rec["is_active"] is True
