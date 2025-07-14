# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d58f4f2d-59d7-406d-ae4c-898354a6a75f",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "5941a6c0-8c98-4d79-b065-a3789e9e0960",
# META       "known_lakehouses": [
# META         {
# META           "id": "d58f4f2d-59d7-406d-ae4c-898354a6a75f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Transform-LH-DuckDB
# This Python only notebook will read from any Lakehouse (not just bound one) and write to the same lakehouse (assuming silver layer schema)
# 
# 


# PARAMETERS CELL ********************

SourceSettings = '{"SourceObject": "dbo.LkFolder"}' # "condition" : "target.RowChecksum = source.RowChecksum","mode":"merge"
TargetSettings = '{"TableName":"silver.folderhierarchy_bar", "mode":"overwrite" }'
SourceConnectionSettings = '{"LakehouseName":"StageDM","LakehouseID":"aa2e3ebf-25c1-408f-980e-967a3976b102","WorkspaceID":"c1a5a7e8-c891-4a83-900f-d3297bec0a53"}'
TargetConnectionSettings = '{"LakehouseName":"StageDM","LakehouseID":"aa2e3ebf-25c1-408f-980e-967a3976b102","WorkspaceID":"c1a5a7e8-c891-4a83-900f-d3297bec0a53"}'
LineageKey = '00000000-0000-0000-0000-000000000000'
ActivitySettings = """
WITH RECURSIVE FolderHierarchy AS (
    SELECT 
        uid as FolderUID,
        parent_content as ParentFolderUID,
        title as Title,
        0 AS Level,
        Title AS FullPath
    FROM LkFolder
    WHERE uid IS NULL or parent_content ='A509EE'

    UNION ALL

    SELECT
        f.uid as FolderUID,
        f.parent_content as ParentFolderUID,
        f.title as Title,
        fh.Level + 1 AS Level,
        fh.FullPath || '->' || f.Title AS FullPath
    FROM LkFolder f
    JOIN FolderHierarchy fh
        ON f.parent_content = fh.FolderUID
)

SELECT * FROM FolderHierarchy ORDER BY FullPath
"""



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
source_settings = json.loads(SourceSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')
source_connection_settings = json.loads(SourceConnectionSettings or '{}')
target_connection_settings = json.loads(TargetConnectionSettings or '{}')
query=ActivitySettings

source_table =source_settings["SourceObject"]
target_table = target_settings["TableName"]
if '.' in source_table:
    source_schema_name, source_table_name = source_table.split('.')
else:
    source_schema_name = None
    source_table_name = target_table
if '.' in target_table:
    target_schema_name, target_table_name = target_table.split('.')
else:
    target_schema_name = None
    target_table_name = target_table
source_workspace_id= source_connection_settings["WorkspaceID"]
source_lakehouse_id = source_connection_settings["LakehouseID"]
target_workspace_id= target_connection_settings["WorkspaceID"]
target_lakehouse_id = target_connection_settings["LakehouseID"]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Read Table Using DuckDb and Relative Folder  (or ABFSS). 
import duckdb
con = duckdb.connect()
token = notebookutils.credentials.getToken('storage')
con.execute(f"CREATE or replace SECRET onelake (TYPE AZURE,PROVIDER ACCESS_TOKEN,ACCESS_TOKEN '{token}')")
if source_schema_name:
    table_path = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Tables/{source_schema_name}/{source_table_name}"
else:
    table_path = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Tables/{source_table_name}"

display(table_path)
sql = f"CREATE VIEW {source_table_name} AS SELECT * FROM delta_scan('{table_path}')"
con.execute(sql).fetchdf()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Run Transform from ActivitySettings
df = con.execute(query).fetchdf()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Write Back to Target
from deltalake import write_deltalake
if target_schema_name:
    table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{target_schema_name}/{target_table_name}"
else:
    table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{target_table_name}"

storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}
write_deltalake(
    table_path,
    df,
    mode="overwrite",
    schema_mode="overwrite",   
    storage_options=storage_options
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
