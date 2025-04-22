# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d58f4f2d-59d7-406d-ae4c-898354a6a75f",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "5941a6c0-8c98-4d79-b065-a3789e9e0960"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

SourceConnectionSettings='{}'
#https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python#sempy-fabric-refresh-dataset 
SourceSettings='{"dataset":"Finance-GL","refresh_type":"full","max_parallelism":"10","retry_count":"0"}'
TargetConnectionSettings='{}'
TargetSettings='{}'
ActivitySettings=None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import sempy.fabric as fabric
import pandas as pd
from datetime import datetime
import time

SourceConnectionSettings = SourceConnectionSettings or '{}'
SourceSettings = SourceSettings or '{}'

source_connection_settings = json.loads(SourceConnectionSettings)
refresh_settings = json.loads(SourceSettings)

workspace = source_connection_settings.get("workspace")

if not workspace:
    workspace_id = spark.conf.get("trident.workspace.id")
    workspace = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}").json()["displayName"]
    
refresh_settings["workspace"] = workspace

dataset = refresh_settings["dataset"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

refresh_request_id=fabric.refresh_dataset(**refresh_settings)
print (refresh_request_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

t1=datetime.now()
class RefreshError(Exception):
    pass
def check_refresh_requests():
    refresh_details =  fabric.get_refresh_execution_details(dataset=dataset, workspace=workspace, refresh_request_id=refresh_request_id)
    
    print(".", end='', flush=True)
    if refresh_details == "Cancelled":
        raise RefreshError(f"Refresh of {workspace}\{dataset} was cancelled." )   
    if  refresh_details.status == "Failed":
        first_error = next(item["Message"] for _, item in refresh_details.messages.iterrows() if item["Type"] == "Error")
        raise RefreshError(f"Refresh of {workspace}\{dataset} failed with error: {first_error}" )   
    return refresh_details.status == "Completed"

while not check_refresh_requests():
    time.sleep(5)

duration_secs=(datetime.now()-t1).total_seconds()
print (f"\n{workspace}\{dataset} Refreshed  {int(duration_secs // 60)} min and {int(duration_secs % 60)} sec.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
