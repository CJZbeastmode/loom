"""Jira response parser — example transform for Loom."""

from pyloom.transform import register

@register("jira_parser:extract")
def extract(record: dict) -> dict:
    fields = record.get("fields", {})
    return {
        "id": record.get("id"),
        "key": record.get("key"),
        "status": fields.get("status", {}).get("name"),
        "assignee": fields.get("assignee", {}).get("displayName", "Unassigned"),
        "resolution_date": fields.get("resolutiondate"),
        "created_date": fields.get("created"),
        "cycle_time_days": _compute_cycle_time(fields),
    }

def _compute_cycle_time(fields: dict) -> float | None:
    import datetime
    created = fields.get("created")
    resolved = fields.get("resolutiondate")
    if not created or not resolved:
        return None
    try:
        created_dt = datetime.datetime.fromisoformat(created.replace("Z", "+00:00"))
        resolved_dt = datetime.datetime.fromisoformat(resolved.replace("Z", "+00:00"))
        return (resolved_dt - created_dt).total_seconds() / 86400.0
    except (ValueError, TypeError):
        return None
