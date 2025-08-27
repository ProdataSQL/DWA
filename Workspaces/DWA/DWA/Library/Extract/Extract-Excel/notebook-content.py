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

# PARAMETERS CELL ********************

SourceSettings = '{"directory" : "landing/AdventureWorks/erp", "file" : "*.xlsx", "sheet":"*"}'# See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html for excel options
TargetSettings ='{"schema": "tst","mode":"overwrite" }'
ActivitySettings = '{"archiveDirectory":"","dedupe":null,"withChecksum":false}'
SourceConnectionSettings = None
TargetConnectionSettings = None
LineageKey = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
import re
import fnmatch
from datetime import datetime
import pandas as pd
from delta.tables import DeltaTable
import hashlib
from pathlib import Path
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_connection_settings = json.loads(SourceConnectionSettings or "{}")
source_lakehouse_id = source_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
source_workspace_id = source_connection_settings.get("workspaceId",fabric.get_workspace_id())
source_lakehouse_name = source_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=source_lakehouse_id, workspace=source_workspace_id))
source_workspace_name = fabric.list_workspaces().set_index("Id")["Name"].to_dict().get(source_workspace_id, "Unknown")
source = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Files"

target_connection_settings = json.loads(TargetConnectionSettings or '{}')
target_lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
target_workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
target_lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=target_lakehouse_id, workspace=target_workspace_id))
target_workspace_name = fabric.list_workspaces().set_index("Id")["Name"].to_dict().get(target_workspace_id, "Unknown")
client = fabric.FabricRestClient()
target_lakehouse_details=client.get(f"/v1/workspaces/{target_workspace_id}/lakehouses/{target_lakehouse_id}")
default_schema=target_lakehouse_details.json().get("properties", {}).get("defaultSchema") # Check if schema is enabled and get default schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

archive = source # Assumes files are to be archive on the source workspace and lakehouse

activity_settings = json.loads(ActivitySettings or "{}")
dedupe = activity_settings.get("dedupe")
with_checksum = bool(activity_settings.get("withChecksum"))
archive_directory = activity_settings.get("archiveDirectory")

source_settings = json.loads(SourceSettings or '{}')
source_directory = source_settings["directory"]
source_path = os.path.join(source, source_directory)
del source_settings["directory"]
source_file = source_settings["file"]
del source_settings["file"]
sheet_name = source_settings.pop("sheet_name", 0)
dtype = source_settings.get("dtype")
if dtype and isinstance(source_settings["dtype"],str):
    source_settings["dtype"] = eval(source_settings["dtype"])
if isinstance(dtype, dict):
    source_settings['names']=list(dtype.keys())
header_val = source_settings.get("header")

target_settings = json.loads(TargetSettings or '{}')
target_schema = target_settings.get("schema", "dbo")
target = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{target_schema}"
target_table = target_settings.get("table", source_file.split(".")[0]) 
write_mode = target_settings.get("mode", "overwrite")
if write_mode == "merge":
   merge_condition = target_settings["condition"]
   del target_settings["condition"]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not mssparkutils.fs.exists(source_path) or len(mssparkutils.fs.ls(source_path)) == 0:
    mssparkutils.notebook.exit(0) # no folder to process, quit

file_list = mssparkutils.fs.ls(source_path)
if len(file_list) == 0:
    mssparkutils.notebook.exit(0) # no files to process, quit
if source_file:
    files_to_process = [
        os.path.join(source_directory, f.name)
        for f in file_list
        if fnmatch.fnmatch(f.name, source_file)
    ]
else:
    files_to_process = [f.path for f in file_list]

if not files_to_process:
    print(f"No files matched pattern: {source_file}")
    mssparkutils.notebook.exit(0)
print (f"Files: {files_to_process}")

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

for file in files_to_process:
    file_path = os.path.join(source, file)
    excel_file = pd.ExcelFile(file_path)

    stem = Path(file_path).stem
    date_pattern = r"(?:_|-)?(\d{8})$"
    match = re.search(date_pattern, stem)
    if match:
        print("Date found:", match.group(1))
        file_date = match.group(1)
        source_file_base_name = re.sub(date_pattern, "", stem)
    else:
        source_file_base_name = stem

    if "dtype" in source_settings and isinstance(source_settings["dtype"], str):
        source_settings["dtype"] = eval(source_settings["dtype"])

    if sheet_name == "*":
        source_settings["sheet_name"] = None

    elif sheet_name == "":
        source_settings["sheet_name"] = 0
        print("Empty string passed as a sheet name, defaulting to first sheet.")
    
    if header_val == "None":
        has_header, header = False, None
    elif isinstance(header_val, bool):
        has_header, header = header_val, (None if header_val else 1)
    elif header_val == 0:
        has_header, header = False, 0
    else:
        has_header, header = True, 0
    source_settings["header"] = header

    print(source_settings)
    if not sheet_name and sheet_name is not None:
        sheets = {excel_file.sheet_names[0] : excel_file.parse(**source_settings)}
    elif isinstance(sheet_name,str) and sheet_name != "*":
        sheets = {sheet_name : excel_file.parse(**source_settings)}
    else:
        sheets = excel_file.parse(**source_settings)
    
    do_drop = write_mode == "overwrite"
    for sheet, df in sheets.items():
        in_row_count = df.shape[0]

        #Cleanse Headers
        df=df.dropna(how='all')
        pattern = '[  \',;{}()\n\t/=%\u00a0]'
        clean_headers = [re.sub(pattern, '_', str(col).strip()) for col in df.columns]
        rename_map = dict(zip(df.columns, clean_headers))
        df.reset_index(inplace=True)
        if len(sheets) > 1:
            table_name =  f"{source_file_base_name}_{sheet}"
        else:
            if target_table =="*":
                table = f"{source_file_base_name}"
            else:
                 table = target_table
        table_path = os.path.join(target, table)
        # checksum added BEFORE file and LineageKey columns (obviously)
        if with_checksum:
            df['RowChecksum'] = df.apply(lambda row: hashlib.sha1(''.join(row.astype(str)).encode()).hexdigest(), axis=1)
        if dedupe:
            df = df.drop_duplicates(["RowChecksum"]) if with_checksum else df.drop_duplicates(clean_headers)
        df.rename(columns={'index': 'RowNumber'}, inplace=True)
        df = df.rename(columns=rename_map).assign(LineageKey=LineageKey).assign(FileName=os.path.basename(file))

        # Write to delta/LH using Spark
        t = datetime.now()
        spark_df = spark.createDataFrame(df)
        if write_mode=="overwrite":
            spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
        out_row_count = df.shape[0]
        if write_mode == "merge":
            target_df = DeltaTab.forPath(spark, table_path)
            target_df.alias("target")\
                .merge(df.alias("source"), merge_condition)\
                .whenNotMatchedInsertAll().execute()

        end_time = datetime.now()

        print (f"- Read  {in_row_count} rows from '{sheet}' in '{file}'"
        f". Wrote {out_row_count} rows to {target_schema}.{table} in {(end_time-t).total_seconds():.2f} seconds"  )
    if archive_directory:
        date = file_date or t # gets date from file name or current date
        archive_folder = os.path.join(archive_directory,date.strftime("%Y"),date.strftime("%m"))
        archive_path = os.path.join(archive,archive_folder,os.path.basename(file))
        
        print(f"Archiving {file} to {archive_folder}.")
        mssparkutils.fs.mv(file_path, archive_path, True, True)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
