# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

RunID="" # EAEF3B1C-3898-464D-BE93-BC8FA50FE201
WorkspaceID = "" # 9b8a6500-5ccb-49a9-885b-b5b081efed75
LOOKBACK_HOURS = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
import requests
from datetime import datetime, timedelta, timezone

def _get_root_causes(run_id, after, before, WorkspaceID, depth=0, lineage_key=None, pipeline_map=None):
    if pipeline_map is None:
        pipeline_map = {}

    if depth > 10:
        return [], pipeline_map

    token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "filters": [],
        "orderBy": [{"orderBy": "ActivityRunStart", "order": "DESC"}],
        "lastUpdatedAfter": after,
        "lastUpdatedBefore": before,
    }

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WorkspaceID}/datapipelines/pipelineruns/{run_id.lower()}/queryactivityruns"

    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()

    errors = []

    for activity in response.json().get("value", []):
        if activity.get("status") != "Failed":
            continue

        activity_type = activity.get("activityType", "")
        activity_name = activity.get("activityName", "")
        error_obj = activity.get("error") or {}
        error_msg = error_obj.get("message", "")
        error_code = error_obj.get("errorCode", "")

        failure_reason = (
            (activity.get("output") or {})
            .get("failureReason", {})
            .get("message", "")
        ) or error_msg or ""

        current_lineage_key = lineage_key
        params = (activity.get("input") or {}).get("parameters", {})
        if params.get("LineageKey") is not None:
            current_lineage_key = params.get("LineageKey")
            if isinstance(current_lineage_key, dict):
                current_lineage_key = current_lineage_key.get("value")

        if activity_type == "InvokePipeline":
            nested_run_id = (activity.get("output") or {}).get("pipelineRunId")
            if nested_run_id:
                nested_errors, pipeline_map = _get_root_causes(
                    nested_run_id,
                    after,
                    before,
                    WorkspaceID,
                    depth + 1,
                    current_lineage_key,
                    pipeline_map
                )
                errors.extend(nested_errors)
            continue

        leaf_error = {
            "ErrorCode": error_code or "",
            "ErrorMessage": failure_reason or "",
            "Artefact": activity_name or "",
            "ArtefactType": activity_type or "",
            "Activity": activity_name or ""
        }

        errors.append(leaf_error)

        if current_lineage_key:
            pipeline_map.setdefault(str(current_lineage_key), [])
            pipeline_map[str(current_lineage_key)].append(leaf_error)

    for key, lineage_errors in pipeline_map.items():
        seen = set()
        deduped = []

        for error in lineage_errors:
            dedupe_key = (
                error.get("ErrorCode", ""),
                error.get("ErrorMessage", ""),
                error.get("Artefact", ""),
                error.get("ArtefactType", ""),
                error.get("Activity", "")
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            deduped.append(error)

        pipeline_map[key] = deduped

    return errors, pipeline_map


def RootErrorSurfacer(run_id: str) -> list:
    if not WorkspaceID:
        raise ValueError("WorkspaceID is empty.")
    if not run_id:
        raise ValueError("RunID is empty.")

    now = datetime.now(timezone.utc)
    after = (now - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    before = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    _, pipeline_map = _get_root_causes(run_id, after, before, WorkspaceID)

    result = [
        {
            "LineageKey": lineage_key,
            "Errors": lineage_errors
        }
        for lineage_key, lineage_errors in pipeline_map.items()
    ]

    if not result:
        return [{
            "LineageKey": "",
            "Errors": [{
                "ErrorCode": "",
                "ErrorMessage": "No root cause error found within the lookback window.",
                "Artefact": "",
                "ArtefactType": "",
                "Activity": ""
            }]
        }]

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

result = RootErrorSurfacer(RunID)
notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
