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

# #### Extract-SP-Excel
# 
# This notebook copies the excel data from a SharePoint site directly into a staging schema in Lakehouse.
# 
# "Sharepoint-Shared-Function" is also required to handle authentication and SharePoint API access, as there are other SharePoint related notebooks in our wider system.


# PARAMETERS CELL ********************

SourceConnectionSettings='{"sharePointUrl":"prodata365.sharepoint.com","site":"Fabric","tenantId":"d8ca992a-5fbe-40b2-9b8b-844e198c4c94","appClientId":"app-fabricdw-dev-clientid","appClientSecret":"app-fabricdw-dev-clientsecret","keyVault":"kv-fabric-dev"}'
TargetConnectionSettings='{"lakehouse":"Fabric_LH", "lakehouseId":"f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c", "workspaceId":"9b8a6500-5ccb-49a9-885b-b5b081efed75"}'
# See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html for excel options
SourceSettings='{"drive": "Unittest", "directory" : "AW", "file" : "*.xlsx","sheet_name" : "","header" : "None","dtype": "str"}'
TargetSettings = '{"schema":"tst","mode":"overwrite"}'  
ActivitySettings=None
LineageKey = "00000000-0000-0000-0000-000000000001"


#Unit Test
#SourceSettings='{"pivot":"True", "top":"5", "recursive":"True", "folderRegEx":"\\\\d{4}", "Drive": "Unittest", "Directory" : "AW", "File" : "*.xlsx","sheet_name" : "Sheet1",  "usecols" : "A:B", "skiprows" : 0, "nrows" : 5, "names":["Review_Date","Revision","Site_Name","Location_No" ,"Location_Data"], "date_columns":["Review_Date"] , "dtype":"str"}'
#TargetSettings = '{"TableName":"bar","SchemaName":"schema"}'






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

lakehouse_name = spark.conf.get("trident.lakehouse.name")
tenant_id = source_connection_settings["tenantId"]
client_id = source_connection_settings["appClientId"]
keyvault = source_connection_settings["keyVault"]
client_secret_name = source_connection_settings["appClientSecret"]
sharepoint_url = source_connection_settings["sharePointUrl"]
site_name = source_connection_settings["site"]
source_drive_name = source_settings.get("drive", "documents")
source_directory = source_settings["directory"].strip("/")
source_file = source_settings["file"]
if not source_directory.startswith("root:/"):
    source_directory = f"root:/{source_directory}"

recursive= bool(source_settings.pop("recursive", False))
folder_regex= source_settings.pop("folderRegEx", None)
dtype=source_settings.get("dtype", None)
pivot=bool(source_settings.pop("pivot", False))
if pivot: 
    pivot_names = source_settings.pop("names",None)
names=source_settings.get("names",None)
source_directory = f"/{source_directory}"
source_settings.pop("drive", None)
source_settings.pop("directory", None)
source_settings.pop("file", None)
date_format=source_settings.pop("dateformat", None)
date_columns=source_settings.pop("date_columns",None)
top=source_settings.pop("top", None)
top=int(top) if top is not None else None
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
if isinstance(dtype, dict):
    source_settings['names']=list(dtype.keys())


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

#Get List of Files from SharePoint
import requests 
ip = requests.get('https://api.ipify.org').text
print(f"Public IP address: {ip}")

access_token = get_sharepoint_token(tenant_id, client_id, keyvault, client_secret_name)
headers = { 'Authorization': f'Bearer {access_token}' }
site = get_sharepoint_site(sharepoint_url, site_name, headers)
drive = get_sharepoint_drive(site, source_drive_name, headers)
file_list = get_sharepoint_files_wildcard(site['id'], drive['id'], source_directory, source_file,headers, recursive, folder_regex)

display (len(file_list))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read sheets into dataframe dictionary


def clean_ordinal_suffix(date_str):
    return re.sub(r'(\d{1,2})(st|nd|rd|th)', r'\1', date_str)

def try_parse_date(val):
    try:
        parsed=pd.to_datetime(clean_ordinal_suffix(val), format='mixed', dayfirst=True).date()
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return val  # leave as-is if parsing fails

sheet_dfs = {} 
if top:
    files=file_list[:top]
else:
    files=file_list 

for file in files:
    t1 = datetime.now()
    source_path = os.path.join(source_directory, file["relative_path"])
    file_info = get_sharepoint_file_info(site['id'], drive['id'], source_path, headers)


    FileName = file_info["name"]
    download_url = file_info["@microsoft.graph.downloadUrl"]
    file_web_url = file_info["webUrl"]
    file_stream = requests.get(download_url)
    excel_file = pd.ExcelFile(BytesIO(file_stream.content))

    try:

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
    except Exception as e:
        print(f"❌ Error reading file: {FileName}")
        print(f"Error details: {e}")
        raise

    t_target_table = target_table or FileName.split('.')[0]
    t_target_table = f"{target_schema}.{t_target_table}"
    is_dynamic_table_name = (len(sheets) > 1) or (is_wildcard and not is_target_table)
    current_mode = mode
    for sheet, df in sheets.items():
        if pivot:
            df=df.set_index( df.columns[0]).T.reset_index(drop=True)
            if pivot_names:
                df.columns=pivot_names

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

        if date_columns: #Custom Parse Date as not standard format supported by Pandas. EG "30th September 2024"
            for col in date_columns:
                df[col] = df[col].apply(try_parse_date)
                
        # Cleanse Headers
        df=df.dropna(how='all')
        pattern = r'[ \-:?><\".,;{}\(\)\n\t/=%]'
        new_column_names = [re.sub(pattern, '_', str(col).strip()) for col in df.columns]
        rename_map = dict(zip(df.columns, new_column_names))
        df.reset_index(inplace=True)
        df=df.copy()
        df['index'] += 2 if has_header else 1
        df.rename(columns={'index': 'RowNumber'}, inplace=True)
        df = df.rename(columns=rename_map).assign(LineageKey=LineageKey).assign(FileUrl=file_web_url).assign(FileName=FileName)


        #Append to buffer
        sheet_dfs.setdefault(current_target_table, []).append (df)
        print (f"- Read {row_count} rows from {FileName} to datframe {current_target_table} in {(datetime.now()-t1).total_seconds():.2f} seconds")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write Sheets to Lakehouse
for current_target_table, dfs in sheet_dfs.items():
    t1 = datetime.now()
    df = pd.concat (dfs, ignore_index=True)
    spark_df = spark.createDataFrame(df) 
    if current_mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {current_target_table}")
 
    spark_df.write.mode(current_mode).format("delta").saveAsTable(current_target_table)
    print(f"- Wrote {len(df)} rows to {lakehouse_name}.{current_target_table} in {(datetime.now() - t1).total_seconds():.2f} seconds")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
