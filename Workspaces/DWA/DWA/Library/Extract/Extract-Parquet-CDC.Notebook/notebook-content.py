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

# # Extract-Parquet-CDC
# Apply parquet files in open mirroring CDC format to Lakehouse and update LSN in table  <BR>
# Format of parquet must have __rowmarker__ as per<BR>  https://learn.microsoft.com/en-us/fabric/database/mirrored-database/open-mirroring-landing-zone-format#format-requirements<BR>
# <BR>
# Folder layout must be as per Fabric Open Mirror standard with Databases Added:
# - /LandingZone/TableA
# - /LandingZone/TableB

# PARAMETERS CELL ********************

SourceSettings = '{"WorkspaceID":"c1a5a7e8-c891-4a83-900f-d3297bec0a53" ,"LakehouseID":"f17dda73-7365-4c4d-a81b-33a0eb3507b5","Directory":"/LandingZone"}'
TargetSettings = '{"cdcTable":"sqlcdcTables"}'
SourceConnectionSettings = '{"WorkspaceID":"c1a5a7e8-c891-4a83-900f-d3297bec0a53" ,"LakehouseID":"f17dda73-7365-4c4d-a81b-33a0eb3507b5", "LakehouseName":"StageDM2"}'
TargetConnectionSettings = '{"WorkspaceID":"c1a5a7e8-c891-4a83-900f-d3297bec0a53" ,"DatabaseID":"603d3056-64c5-478a-81f1-7db7e61af7ef"}'
ActivitySettings = None
LineageKey = '00000000-0000-0000-0000-000000000000'


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
import json
import re
import sempy.fabric as fabric
from datetime import datetime
from delta.tables import DeltaTable
import pandas as pd

source_settings = json.loads(SourceSettings or '{}')
source_connection_settings = json.loads(SourceConnectionSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')
target_connection_settings = json.loads(TargetConnectionSettings or '{}')
activity_settings = json.loads(ActivitySettings or '{}')
workspace_id = source_connection_settings['WorkspaceID']
lakehouse_id = source_connection_settings.pop("LakehouseID")
directory = source_settings.pop("Directory","\LandingZone")
database_id = target_connection_settings.pop("DatabaseID")
target_workspace_id = source_connection_settings['WorkspaceID']
target_lakehouse_name = source_connection_settings['LakehouseName']
cdc_table  = target_settings.pop("cdcTable")

client = fabric.FabricRestClient()
lakehouse_details=client.get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}")
default_lakehouse_schema=lakehouse_details.json().get("properties", {}).get("defaultSchema")
items = client.get(f"/v1/workspaces/{target_workspace_id}/SqLDatabases").json()["value"]
sql_database=next((endpoint for endpoint in items if endpoint["displayName"] == "Meta"))
sql_end_point = sql_database["properties"]["serverFqdn"]
sql_database_name = sql_database["properties"]["databaseName"]
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point};database={sql_database_name}"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") #Needed if dates below 1800
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
files_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/{directory}"
operation ="overwrite"
engine = create_engine(connection_string)
sql_connection = engine.raw_connection()
batch_start_time = datetime.now()

with engine.connect() as alchemy_connection:
    tables = pd.read_sql_query ("exec config.usp_cdcSqlTables", alchemy_connection) #List of cdcTables and PrimaryKeys (Required for Merge on non-standard Keys)

files = []
for folder in notebookutils.fs.ls(files_path):
    files.extend (sorted([x.path for x in notebookutils.fs.ls(folder.path) if not x.isDir]))

for file in files:
    t = datetime.now()
    file_path =file
    base_table_name= file.split('/')[-1].removesuffix('.parquet')
    schema_name, table_name = (base_table_name.split('.') + [None])[:2][::-1]
    table_df = tables[tables['Table'] == base_table_name]   #Retrieve Primary Key from the cdcSqlTables
    primary_keys = table_df ['PrimaryKeys'].iloc[0] if not table_df.empty else None

    df = spark.read.parquet(file_path ) 
    cdc_columns = ["__$start_lsn", "__$seqval", "__$operation", "__$update_mask"]
    existing_columns = [col for col in cdc_columns if col in df.columns]
    if existing_columns:
        df = df.drop(*existing_columns)   
    row = df.head(1)
    if row: 
        if "__rowMarker__" not in df.columns:
            raise Exception(f"File {file_path } does not contain the __rowMarker__ column. See https://learn.microsoft.com/en-us/fabric/database/mirrored-database/open-mirroring-landing-zone-format#format-requirements")
        operation = "overwrite" if row and row[0]["__rowMarker__"] == 0 else "merge"
        if '.' in base_table_name:
            schema_name, table_name = base_table_name.split('.', 1)
        else:
            table_name=base_table_name
        if not schema_name: 
            schema_name = default_lakehouse_schema
        if schema_name:
                table_name =f"{schema_name}/{base_table_name}"
        table_name = re.sub(r'_(\d{14})$', '', table_name)   #Strip timestamp if present
        table_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"
        if not notebookutils.fs.exists(table_path):
            operation="overwrite"

        if operation=="overwrite":
            df.write.mode(operation).option("overwriteSchema", "true").format("delta").save(table_path)
        else:
            if primary_keys is None:
                keys = [df.columns[0]]  
            else:
                keys = [key.strip() for key in primary_keys.split(',')]  # Split and clean keys
            merge_condition = ' AND '.join([f'target.{key} = source.{key}' for key in keys])        
            sink_df = DeltaTable.forPath(spark, table_path)
            sink_df.alias("target").merge(df.alias("source"), merge_condition).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()

    sql = 'exec config.usp_cdcSqlCommit @Table=?'
    values = (base_table_name,)
    cursor = sql_connection.cursor()
    cursor.execute(sql, values)
    sql_connection.commit()
    notebookutils.fs.rm(file_path, recurse=False)

    print(f"\t- {operation} for {base_table_name}=>{table_name} {(datetime.now() - t).total_seconds():.1f} seconds")

#Remove Empty Folders
subfolders = [f.path for f in mssparkutils.fs.ls(files_path) if f.isDir]
for folder in subfolders:
    if all(map(lambda i: i.isDir, mssparkutils.fs.ls(folder))):
        mssparkutils.fs.rm(folder, True)

duration_secs=(datetime.now()-batch_start_time).total_seconds()
print (f"Total Batch Time: {int(duration_secs // 60)} min and {int(duration_secs % 60)} sec. {len(files)} files. {duration_secs/(len(files) if len(files) else 1)} seconds per file.")

#Cleanup
sql_connection.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
