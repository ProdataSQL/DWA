# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c",
# META       "default_lakehouse_name": "FabricLH",
# META       "default_lakehouse_workspace_id": "9b8a6500-5ccb-49a9-885b-b5b081efed75"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
import time
from pyspark.sql import functions as fn
from datetime import datetime
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException
workspace_id = spark.conf.get("trident.workspace.id")
lakehouse_id = spark.conf.get("trident.lakehouse.id")
if not lakehouse_id:
    raise Exception("No lakehouse is attached to this notebook.")
client = fabric.FabricRestClient()

sql_endpoint_id = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['id']

md_sync_start_time = datetime.now()

try:
    response = client.post(f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}",json= {"commands":[{"$type":"MetadataRefreshCommand"}]})
    data = json.loads(response.text or '{}')
    batchId = data["batchId"]
    progressState = data["progressState"]
except FabricHTTPException as e:
    response = e.response
    data = response.json()
    if response.status_code == 400 and data.get("error", {"code":None}).get("code", None) == 'LockConflict': # safer execution - reduce risk of errors
        details = data["error"]["pbi.error"]["details"]
        batchId = next(
            (item["detail"]["value"] for item in data["error"]["pbi.error"]["details"] if item["code"] == "LockingBatchId"),
            None)

        print(f"MD Sync already running - using already running batch ID ({batchId})")
        progressState = 'inProgress'
    else:
        raise e

statusuri = f"/v1.0/myorg/lhdatamarts/{sql_endpoint_id}/batches/{batchId}"

statusresponsedata = ""

initial_delay = 1.0
max_delay = 32.0

while progressState == 'inProgress':
    print(f".", end='')
    statusresponsedata = client.get(statusuri).json()

    progressState = statusresponsedata["progressState"]


    if progressState == 'inProgress':
        time.sleep(initial_delay)
        initial_delay = min(initial_delay * 2.0, max_delay)

md_sync_finish_time = datetime.now()

print(f"\nMD Sync call finished in {(md_sync_finish_time - md_sync_start_time).total_seconds()}s")

class MDSyncFailed(Exception):
    pass
if progressState == 'success':
    table_details = [
        {
          'tableName': table['tableName'],
         'warningMessages': table.get('warningMessages', []),
         'lastSuccessfulUpdate': table.get('lastSuccessfulUpdate', 'N/A'),
         'tableSyncState':  table['tableSyncState'],
         'sqlSyncState':  table['sqlSyncState']
        }
        for table in statusresponsedata['operationInformation'][0]['progressDetail']['tablesSyncStatus'] if table['tableSyncState'] != "NotRun"
    ]
    if not table_details:
        print("No tables synced!")
        mssparkutils.notebook.exit(0)

    print("Tables synced:")
    for detail in table_details:
        print(f"Table: {detail['tableName']}\n\t - tableSyncState: {detail['tableSyncState']}\n\t - Warnings: {detail['warningMessages']}")
else:
    raise MDSyncFailed(json.dumps(statusresponsedata))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
