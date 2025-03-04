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

SourceConnectionSettings='{ "sharepoint_url":"prodata365.sharepoint.com","site":"Fabric","tenant_id":"d8ca992a-5fbe-40b2-9b8b-844e198c4c94","app_client_id":"app-fabricdw-dev-clientid","app_client_secret":"app-fabricdw-dev-clientsecret","keyvault":"kv-fabric-dev"}'
TargetConnectionSettings=None
# See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html for excel options
SourceSettings='{"Drive": "AdventureWorks", "Directory" : "AW/erp", "File" : "*.xlsx" }'
TargetSettings = '{"SchemaName":"aw"}' 
ActivitySettings=None
LineageKey = "00000000-0000-0000-0000-000000000000"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import os
from notebookutils import mssparkutils
from datetime import datetime
import pandas as pd
import re
from io import BytesIO

source_connection_settings = json.loads(SourceConnectionSettings or '{}')
source_settings = json.loads(SourceSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_name = spark.conf.get("trident.lakehouse.name")
tenant_id = source_connection_settings["tenant_id"]
client_id = source_connection_settings["app_client_id"]
keyvault = source_connection_settings["keyvault"]
client_secret_name = source_connection_settings["app_client_secret"]
sharepoint_url = source_connection_settings["sharepoint_url"]
site_name = source_connection_settings["site"]
source_drive_name = source_settings.get("Drive", "Documents")
source_directory = source_settings["Directory"].strip("/")
source_file = source_settings["File"]
if not source_directory.startswith("root:/"):
    source_directory = f"root:/{source_directory}"
source_directory = f"/{source_directory}"

source_settings.pop("Drive", None)
source_settings.pop("Directory", None)
source_settings.pop("File", None)

target_table = target_settings.get("TableName")
target_schema = target_settings.get("SchemaName","stg").strip("_ ")

is_target_table = bool
is_wildcard = any(char in source_file for char in ["*", ">"])


has_header : bool = True
if "header" in source_settings and source_settings["header"]=="None":
    source_settings["header"] = None
else:
    source_settings["header"] = 0

if "header" in source_settings:
    if isinstance(source_settings["header"], bool):
        has_header = source_settings["header"]
        source_settings["header"] = None if has_header else 1
    elif source_settings["header"] == 0:
        has_header = False

mode = target_settings.get("mode", "overwrite")

if "header" in source_settings and source_settings["header"]=="None":
    source_settings["header"] = None
else:
    source_settings["header"] = 0


sheet_name = source_settings.setdefault("sheet_name", 0)

if sheet_name == "*":
    source_settings["sheet_name"] = None
elif sheet_name == "":
    print("Empty string passed as a sheet name, defaulting to first sheet.")
elif isinstance(sheet_name, str) and "," in sheet_name :
    sheet_name = sheet_name.split(",")


if "dtype" in source_settings and isinstance(source_settings["dtype"],str):
    source_settings["dtype"] = eval(source_settings["dtype"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run SharePoint-Shared-Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

access_token = get_sharepoint_token(tenant_id, client_id, keyvault, client_secret_name)
headers = { 'Authorization': f'Bearer {access_token}' }
site = get_sharepoint_site(sharepoint_url, site_name, headers)
drive = get_sharepoint_drive(site["id"], source_drive_name, headers)

files = get_sharepoint_files_wildcard(site['id'], drive['id'], source_directory, source_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for file in files:
    source_path = os.path.join(source_directory, file["name"])
    file_info = get_sharepoint_file_info(site['id'], drive['id'], source_path, headers)
    download_url = file_info["@microsoft.graph.downloadUrl"]
    file_web_url = file_info["webUrl"]

    file_stream = requests.get(download_url)
    excel_file = pd.ExcelFile(BytesIO(file_stream.content))
    # sheet_name  = "" or []
    if not sheet_name and sheet_name is not None: # checks if empty or None
        if "sheet_name" in source_settings: del(source_settings["sheet_name"])
        sheets = {excel_file.sheet_names[0] : excel_file.parse(**source_settings)}
    # sheet_name = "sheet1,sheet2"
    elif isinstance(sheet_name, list):
        if "sheet_name" in source_settings: del(source_settings["sheet_name"])    
        sheets = {name: excel_file.parse(sheet_name=name, **source_settings) for name in sheet_name}
    # sheet_name = "sheet1"
    elif isinstance(sheet_name,str) and sheet_name != "*":
        sheets = {sheet_name : excel_file.parse(**source_settings)}
    # sheet_name = None
    else:
        sheets = excel_file.parse(**source_settings)
    t1 = datetime.now()

    FileName = file_info["name"]

    t_target_table = target_table or FileName.split('.')[0]
    t_target_table = f"{target_schema}.{t_target_table}"

    is_dynamic_table_name = (len(sheets) > 1) or (is_wildcard and not is_target_table)

    current_mode = mode
    for sheet, df in sheets.items():
        print(f" - Processing {sheet} from {FileName}. ")
        if is_target_table and not is_dynamic_table_name:
            current_target_table = t_target_table
        if is_dynamic_table_name:
            current_target_table =  f"{t_target_table}_{sheet}"
        else:
            current_target_table = target_table or FileName.split('.')[0]
            current_target_table = f"{target_schema}.{current_target_table}"
        pattern = '[ - ,;{}()\n\t/=%]'
        current_target_table = re.sub(pattern, '_', current_target_table.strip(pattern)).lower()
        row_count = df.shape[0]

        # Cleanse Headers
        pattern = '[ -:?><".,;{}()\n\t/=%]'
        new_column_names = [re.sub(pattern, '_', str(col).strip(pattern)) for col in df.columns]
        rename_map = dict(zip(df.columns, new_column_names))
        df.reset_index(inplace=True)

        df['index'] += 2 if has_header else 1

        df.rename(columns={'index': 'RowNumber'}, inplace=True)
        df = df.rename(columns=rename_map).assign(LineageKey=LineageKey).assign(FileUrl=file_web_url).assign(FileName=FileName)

        # Write to delta/LH using Spark
        if current_mode == "overwrite":
            spark.sql(f"DROP TABLE IF EXISTS {current_target_table}")
        df = spark.createDataFrame(df)
        t1 = datetime.now()
        df.write.mode(current_mode).format("delta").saveAsTable(f"{current_target_table}")
        if current_mode == "overwrite":
            current_mode = "append"
        print (f"- Write {row_count} rows from df to {lakehouse_name}.{current_target_table} in {(datetime.now()-t1).total_seconds()} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
