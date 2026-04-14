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

# #### Export-Excel
# 
# This notebook extracts data from a SQL source such as a table, view, or stored procedure and exports the result to an Excel file and saves it to a specified directory in the Fabric Lakehouse. The script supports dynamic query generation and customizable Excel export options defined in the target settings.

# PARAMETERS CELL ********************

SourceSettings ='{"object":"EXEC FabricDW.config.[usp_PipelineQueue] @PipelineID=2"}'
TargetSettings ='{"directory": "Export/excel", "file": "PipelineQueue.xlsx", "header":"False"}'
SourceConnectionSettings = None
TargetConnectionSettings = None
ActivitySettings = None
LineageKey = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run SQL-Connection-Shared-Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import re
import struct
import json
import pyodbc
import pandas as pd
from pathlib import Path
import sempy.fabric as fabric
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, NullType

SourceSettings = SourceSettings or '{}'
TargetSettings = TargetSettings or '{}'

source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)
source_connection_settings = json.loads(SourceConnectionSettings or '{}')
target_connection_settings = json.loads(TargetConnectionSettings or '{}')
lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=lakehouse_id, workspace=workspace_id))
workspaces = fabric.list_workspaces()
workspace_name = fabric.list_workspaces().set_index("Id")["Name"].to_dict().get(workspace_id, "Unknown")

if "header" not in target_settings:
    target_settings["header"] = True

target_directory = target_settings["directory"]
target_file = target_settings["file"]

FILES_PREFIX = "Files"
LAKEHOUSE_DEFAULT_PREFIX = "/lakehouse/default/"
if not target_directory.startswith(FILES_PREFIX):
   target_directory = os.path.join(FILES_PREFIX, target_directory).replace("\\", "/")

target_path = os.path.join(target_directory, target_file)
temp_target_path = os.path.join(target_directory, f"_{target_file}")

del target_settings["directory"]
del target_settings["file"]

Path(target_path).parent.mkdir(parents=True, exist_ok=True)

sql_end_point= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point}"
pattern = '[ ,;{}()\n\t/=]'

# List Datasets from meta data
engine = create_engine(connection_string)

source_object = source_settings["object"]
lower_obj = source_object.lower()

if "select" in lower_obj or lower_obj.startswith("exec"):
    query = source_object  
elif "." in lower_obj:
    query = f"SELECT * FROM {source_object}"

with engine.connect() as alchemy_connection:
    df = pd.read_sql_query(query, alchemy_connection)

df.to_excel(target_path,**target_settings)

print(f"'{query}' exported to '{target_path}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
