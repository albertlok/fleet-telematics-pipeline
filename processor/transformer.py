"""
Transformers: convert raw webhook payloads into flat dicts that match
our database schema.

Telematics providers send deeply nested JSON. Relational databases want
flat rows. This module is the mapping layer between the two.

Event types handled:
  - VehicleLocation          GPS position update from a vehicle gateway
  - DriverHOS                Hours-of-Service (drive-time compliance) change
  - SafetyEvent              AI dashcam alert (harsh braking, tailgating, ...)
  - VehicleDiagnostic        Engine fault code from the OBD-II port
  - GeofenceEntry / Exit     Vehicle crossed a virtual map boundary

Design: a registry pattern. Each transformer function is registered
against the event type it handles via the @register decorator. To support
a new event type, write one function and decorate it — nothing else in
the pipeline needs to change.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

log = logging.getLogger(__name__)

# Maps eventType string → the function that knows how to flatten it.
_REGISTRY: dict[str, Callable[[dict, dict], dict]] = {}


def register(event_type: str):
    """Decorator that adds a transformer function to the registry."""
    def decorator(fn):
        _REGISTRY[event_type] = fn
        return fn
    return decorator


def transform(envelope: dict) -> dict | None:
    """
    Turn one raw envelope into a flat record ready for the database.

    Returns None when the event type is unknown — the caller (the
    processor main loop) routes those to the Dead Letter Queue so a
    human can inspect them, instead of crashing the pipeline.

    Raises if a known event type has a malformed body; the caller
    catches that and dead-letters the message too.
    """
    payload = envelope.get("payload", {})
    event_type = payload.get("eventType")
    fn = _REGISTRY.get(event_type)
    if fn is None:
        log.warning("Unknown eventType=%s — skipping", event_type)
        return None
    try:
        return fn(envelope, payload)
    except (KeyError, TypeError, ValueError) as exc:
        log.error("Transform failed for eventType=%s: %s", event_type, exc)
        raise


def _base_fields(envelope: dict, payload: dict) -> dict:
    """Fields common to every event type, extracted once."""
    return {
        # Our own tracing metadata (added by the ingestion service):
        "ingestion_id": envelope["ingestion_id"],
        "ingested_at_ms": envelope["ingested_at"],
        # The provider's metadata:
        "event_id": payload.get("eventId"),       # unique per event — used for dedup
        "event_type": payload.get("eventType"),
        "event_time_ms": payload.get("eventMs"),  # when the event actually happened
        "org_id": payload.get("orgId"),
    }


# Note the pattern in every transformer below: we use .get() instead of
# ["key"] for optional fields so a missing field becomes None (a NULL in
# the database) instead of a crash. Real-world device data is messy —
# GPS units lose fixes, firmware versions differ, fields come and go.

@register("VehicleLocation")
def _vehicle_location(envelope: dict, payload: dict) -> dict:
    data = payload.get("data", {})
    loc = data.get("location", {})
    return {
        **_base_fields(envelope, payload),
        "vehicle_id": data.get("id"),
        "vehicle_name": data.get("name"),
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "heading_degrees": loc.get("headingDegrees"),
        "speed_mph": loc.get("speedMilesPerHour"),
        # Human-readable address, e.g. "350 5th Ave, New York" — providers
        # compute this from the coordinates ("reverse geocoding").
        "reverse_geo": loc.get("reverseGeo", {}).get("formattedLocation"),
    }


@register("DriverHOS")
def _driver_hos(envelope: dict, payload: dict) -> dict:
    # HOS = Hours of Service: legal limits on how long a commercial
    # driver may drive before resting. Fleets must track this for
    # compliance, which is why losing these events is a big deal.
    data = payload.get("data", {})
    return {
        **_base_fields(envelope, payload),
        "driver_id": data.get("driverId"),
        "driver_name": data.get("driverName"),
        "duty_status": data.get("currentDutyStatus"),  # e.g. "driving", "offDuty"
        "eld_log_id": data.get("eldLogId"),
        "shift_drive_remaining_ms": data.get("shiftDriveRemainingMs"),
        "shift_remaining_ms": data.get("shiftRemainingMs"),
    }


@register("SafetyEvent")
def _safety_event(envelope: dict, payload: dict) -> dict:
    data = payload.get("data", {})
    return {
        **_base_fields(envelope, payload),
        "vehicle_id": data.get("vehicleId"),
        "driver_id": data.get("driverId"),
        "behavior_label": data.get("behaviorLabel"),  # e.g. "harshBraking"
        "severity": data.get("severity"),
        # Peak g-force during the event — how hard the braking/swerve was.
        "max_acceleration_g": data.get("maxAccelerationG"),
        "media_url": data.get("url"),  # link to the dashcam clip
    }


@register("VehicleDiagnostic")
def _vehicle_diagnostic(envelope: dict, payload: dict) -> dict:
    data = payload.get("data", {})
    return {
        **_base_fields(envelope, payload),
        "vehicle_id": data.get("vehicleId"),
        # DTC = Diagnostic Trouble Code, the standard engine fault codes
        # read from the vehicle's OBD-II port (e.g. "P0301").
        "dtc_short_code": data.get("dtcShortCode"),
        "dtc_description": data.get("dtcDescription"),
        "is_active": data.get("isActive"),
    }


# One function handles both directions — stacked decorators register it
# under both event type names.
@register("GeofenceEntry")
@register("GeofenceExit")
def _geofence(envelope: dict, payload: dict) -> dict:
    data = payload.get("data", {})
    return {
        **_base_fields(envelope, payload),
        "vehicle_id": data.get("vehicleId"),
        "driver_id": data.get("driverId"),
        "geofence_id": data.get("geofenceId"),
        "geofence_name": data.get("geofenceName"),
        "direction": "entry" if payload.get("eventType") == "GeofenceEntry" else "exit",
    }
