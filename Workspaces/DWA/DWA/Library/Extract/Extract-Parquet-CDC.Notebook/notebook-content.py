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

# # Extract-Parquet-CDC
# Apply parquet files in open mirroring CDC format to Lakehouse and update LSN in table  <BR>
# Format of parquet must have __rowmarker__ as per<BR>  https://learn.microsoft.com/en-us/fabric/database/mirrored-database/open-mirroring-landing-zone-format#format-requirements<BR>
# <BR>
# Folder layout must be as per Fabric Open Mirror standard with Databases Added:
# - /LandingZone/SchemaA.schema/TableA
# - /LandingZone/SchemaB.schema/TableB


# PARAMETERS CELL ********************

SourceSettings = None #'{"sqlTables":"config.auditSqlTables", "schema":"aw"}'
TargetSettings = None #'{"commitSproc":"config.usp_auditSqlCommit"}'
SourceConnectionSettings = None
TargetConnectionSettings = None
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

activity_settings = json.loads(ActivitySettings or '{}')

source_connection_settings = json.loads(SourceConnectionSettings or '{}')
source_lakehouse_id = source_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
source_workspace_id = source_connection_settings.get("workspaceId",fabric.get_workspace_id())
source_lakehouse_name = source_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=source_lakehouse_id, workspace=source_workspace_id))
source_workspace_name = fabric.resolve_workspace_name(source_workspace_id)

target_connection_settings = json.loads(TargetConnectionSettings or '{}')
target_lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
target_workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
target_lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=target_lakehouse_id, workspace=target_workspace_id))
target_workspace_name = fabric.resolve_workspace_name(target_workspace_id)

source_settings = json.loads(SourceSettings or '{}')
cdc_sql_table = source_settings.get('sqlTables','config.cdcSqlTables')
schema = source_settings.get('schema')
tables = source_settings.get('sqlTables','config.cdcSqlTables')

target_settings = json.loads(TargetSettings or '{}')
commit_sproc = target_settings.get('commitSproc','config.usp_cdcSqlCommit')


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

client = fabric.FabricRestClient()
items = client.get(f"/v1/workspaces/{source_workspace_id}/SqLDatabases").json()["value"]
sql_database=next((endpoint for endpoint in items if endpoint["displayName"] == "Meta"))
sql_end_point = sql_database["properties"]["serverFqdn"]
sql_database_name = sql_database["properties"]["databaseName"]
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point};database={sql_database_name}"

spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") #Needed if dates below 1800
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
files_path = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Files/LandingZone"
operation ="overwrite"
engine = create_engine(connection_string)
sql_connection = engine.raw_connection()
batch_start_time = datetime.now()
with engine.connect() as alchemy_connection:
    tables = pd.read_sql_query(f"exec config.usp_cdcSqlTables '{source_lakehouse_name}', '{cdc_sql_table}'", alchemy_connection)

folders_to_process = []
if schema:
    schema_folder = f"{schema}.schema"
    for folder in notebookutils.fs.ls(files_path):
        folder_name = folder.path.split("/")[-1]
        if folder_name == schema_folder:
            folders_to_process.append(folder)
            break  # Found the specific schema folder
else:
    folders_to_process = list(notebookutils.fs.ls(files_path))
    
files = []
for folder in folders_to_process:
    for subfolder in notebookutils.fs.ls(folder.path):
        if subfolder.isDir:
            files.extend(
                sorted([x.path for x in notebookutils.fs.ls(subfolder.path) if not x.isDir])
            )

for file in files:
    t = datetime.now()
    file_path =file
    file_name= file.split('/')[-1]
    table_name= file.split('/')[-2]
    schema_name=file.split('/')[-3].removesuffix('.schema')
    table_df = tables[tables['Table'].str.endswith(f'.{table_name}', na=False)]
    primary_keys = table_df ['PrimaryKeys'].iloc[0] if not table_df.empty else None
    source_table = table_df['Table'].iloc[0] if not table_df.empty else None
        
    df = spark.read.parquet(file_path ) 
    cdc_columns = ["__$start_lsn", "__$seqval", "__$operation", "__$update_mask"]
    existing_columns = [col for col in cdc_columns if col in df.columns]
    if existing_columns:
        df = df.drop(*existing_columns)   
    row = df.head(1)
    table_directory =f"{schema_name}.schema/{table_name}"
    table_path = f"abfss://{taget_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{schema_name}/{table_name}"
    full_table_name=f"{schema_name}.{table_name}"
    row_count = df.count()
    print(f"Extracting {row_count} rows into {target_lakehouse_name}.{full_table_name}")
    if row: 
        if "__rowMarker__" not in df.columns:
            raise Exception(f"File {file_path } does not contain the __rowMarker__ column. See https://learn.microsoft.com/en-us/fabric/database/mirrored-database/open-mirroring-landing-zone-format#format-requirements")
        operation = "overwrite" if row and row[0]["__rowMarker__"] == 0 else "merge"
        if not notebookutils.fs.exists(table_path):
            operation="overwrite"
        if operation=="overwrite":
            df.write.mode(operation).option("overwriteSchema", "true").format("delta").save(table_path)
        else:
            if primary_keys is None:
                keys = [df.columns[0]]  
            else:
                keys = [key.strip('[], ') for key in primary_keys.split(',')]   # Split and clean keys
            merge_condition = ' AND '.join([f'target.{key} = source.{key}' for key in keys])        
            sink_df = DeltaTable.forPath(spark, table_path)
            sink_df.alias("target").merge(df.alias("source"), merge_condition).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()
        print(f"\t- {operation} for {source_table}=>{schema_name}.{table_name} {(datetime.now() - t).total_seconds():.1f} seconds")
    sql = f'exec {commit_sproc} @Table=?'
    values = (tables)
    cursor = sql_connection.cursor()
    cursor.execute(sql, values)
    sql_connection.commit()
    values = (f"{source_table}",)
    cursor = sql_connection.cursor()
    cursor.execute(sql, values)
    sql_connection.commit()
    notebookutils.fs.rm(file_path, recurse=False)

# Remove Empty Folders - only for processed schemas
for processed_folder in folders_to_process:
    subfolders = [f.path for f in mssparkutils.fs.ls(processed_folder.path) if f.isDir]
    
    for folder in subfolders:
        folder_contents = mssparkutils.fs.ls(folder)
        if not folder_contents or all(item.isDir for item in folder_contents):
            mssparkutils.fs.rm(folder, True)
            print(f"Removed empty folder: {source_lakehouse_name}/Files/{folder.split('/Files/')[1]}")

duration_secs = (datetime.now() - batch_start_time).total_seconds()
print (f"Total Batch Time: {int(duration_secs // 60)} min and {int(duration_secs % 60)} sec. {len(files)} files. {duration_secs/(len(files) if len(files) else 1)} seconds per file.")
#Cleanup
sql_connection.close()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
