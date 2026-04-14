# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c",
# META       "default_lakehouse_name": "FabricLH",
# META       "default_lakehouse_workspace_id": "9b8a6500-5ccb-49a9-885b-b5b081efed75",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Extract-JSON
# 
# This notebook ingests a JSON file from a configured Data Lake path into a Delta table within the Fabric Lakehouse. It flattens the JSON structure using a specified nodes in ActivitySettings, adds lineage and file metadata, and writes the data to the target table using the defined write mode (e.g., overwrite). 

# PARAMETERS CELL ********************

SourceSettings ='{"directory" : "unittest/JSON", "file" : "simple.json"}'
TargetSettings ='{"table":"simpleJSON", "schema":"dbo","mode":"overwrite" }'
ActivitySettings='{"record_path": "book"}'
SourceConnectionSettings = None
TargetConnectionSettings = None
LineageKey = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import json
import os
import sempy.fabric as fabric

workspaces = fabric.list_workspaces()

activity_settings = json.loads(ActivitySettings or '{}')
source_connection_settings = json.loads(SourceConnectionSettings or "{}")
source_lakehouse_id = source_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
source_workspace_id = source_connection_settings.get("workspaceId",fabric.get_workspace_id())
source_lakehouse_name = source_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=source_lakehouse_id, workspace=source_workspace_id))
source_workspace_name = workspaces.set_index("Id")["Name"].to_dict().get(source_workspace_id, "Unknown")

target_connection_settings = json.loads(TargetConnectionSettings or '{}')
target_lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
target_workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
target_lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=target_lakehouse_id, workspace=target_workspace_id))
target_workspace_name = workspaces.set_index("Id")["Name"].to_dict().get(target_workspace_id, "Unknown")

source_settings = json.loads(SourceSettings or '{}')
source_directory = source_settings["directory"]
if source_directory.startswith("Files/"):
    source_directory = source_directory[len("Files/"):]
source_file = source_settings["file"]
file_path = os.path.join("/lakehouse/default/Files",source_directory, source_file) #Todo: Lookup from Abss path

target_settings = json.loads(TargetSettings or '{}')
target_schema = target_settings.get("schema", "dbo") 
target_table = target_settings.get("table", source_file.split(".")[0])
if target_schema != "dbo":
    target_table = f"{target_schema}_{target_table}" #Todo: Write to Abss path
mode = target_settings.get("mode","overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source_workspace_name==target_workspace_name:
    print(f"Workspace: {source_workspace_name}")
    if source_lakehouse_name==target_lakehouse_name:
        print(f"Lakehouse: {source_lakehouse_name}")
    else:
        print(f"Source Lakehouse: {source_lakehouse_name}")
        print(f"Target Lakehouse: {target_lakehouse_name}")
else:
    print(f"Source Workspace: {source_workspace_name}, Lakehouse: {source_lakehouse_name}")
    print(f"Target Workspace: {target_workspace_name}, Lakehouse: {target_lakehouse_name}")

with open(file_path, 'r') as f:
    json_data = json.load(f)

df = pd.json_normalize(json_data, **activity_settings)
df["LineageKey"] = LineageKey
df["File"] = source_file

row_count = df.shape[0]

if mode == "overwrite":
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
spark.createDataFrame(df).write.mode(mode).options(**target_settings).format("delta").saveAsTable(target_table)

print(f"Wrote {row_count} rows from {file_path} to FabricLH.{target_schema}.{target_table}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
