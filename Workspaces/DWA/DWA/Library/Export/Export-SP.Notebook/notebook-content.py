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
# META       "default_lakehouse_workspace_id": "9b8a6500-5ccb-49a9-885b-b5b081efed75"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Export-SP
# 
# This notebook extracts data from one or more SQL source objects—such as tables, views, or stored procedures—and exports the results to a file (Excel, CSV, or XML), which is then saved to a specified SharePoint location using Microsoft Graph API. It dynamically builds SQL queries based on the provided metadata, supports multi-sheet Excel exports, and allows configurable export options such as header inclusion, sheet naming, and file structure. 

# CELL ********************

# Environment parameters
SourceConnectionSettings = '{}'
TargetConnectionSettings = '{ "sharepoint_url":"prodata365.sharepoint.com","site":"Fabric","tenant_id":"d8ca992a-5fbe-40b2-9b8b-844e198c4c94","app_client_id":"b9b25184-7305-4612-89cb-41106bc4d80a","app_client_secret":"app-fabricdw-dev-clientsecret","KeyVault":"kv-fabric-dev"}'
SourceSettings = '{"DefaultDatabase":"FabricDW","SourceObject":"aw.FactFinance"}'
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_excel.html
# https://docs.python.org/3/library/datetime.html#datetime.date.strftime
TargetSettings = '{"Drive" : "Documents" , "Directory": "General/Controls/", "File" : "AidanTestSheet_%Y%m%d.xlsx","sheet_name":["SprocExample"]}'
ActivitySettings = '{}'

 
LineageKey : str = '00000000-0000-0000-0000-000000000000'

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
import requests
import io
import pyodbc
import pandas as pd
import json
import re
from datetime import datetime
import sempy.fabric as fabric

SourceSettings = SourceSettings or '{}'
TargetSettings = TargetSettings or '{}'
TargetConnectionSettings = TargetConnectionSettings or '{}'
source_settings = json.loads(SourceSettings)
target_connection_settings = json.loads(TargetConnectionSettings) 

target_settings = json.loads(TargetSettings)

if "header" not in target_settings:
    target_settings["header"] = True

tenant_id = target_connection_settings.get("tenant_id", spark.conf.get("trident.tenant.id"))
client_id = target_connection_settings["app_client_id"]
keyvault = target_connection_settings["KeyVault"]
client_secret_name = target_connection_settings["app_client_secret"]
sharepoint_url = target_connection_settings["sharepoint_url"]
site_name = target_connection_settings["site"]

target_drive_name = target_connection_settings.get("Drive") or target_settings.get("Drive", "Documents")

target_directory = target_connection_settings.get("Directory") or target_settings["Directory"]
target_directory = target_directory.strip("/")

target_file = target_settings["File"]
file_split = target_file.split(".")
if len(file_split) < 2:
    raise ValueError("target File must have a specified file time ({}.[csv]/[excel])")

file_type = file_split[-1]

CSV_FILE_TYPES = ("csv", "txt")
EXCEL_FILE_TYPES = ("xlsx")
XML_FILE_TYPES = ("xml")



sheet_names = target_settings.get("sheet_name") if file_type in EXCEL_FILE_TYPES else None



if not target_directory.startswith("root:/"):
    target_directory = f"root:/{target_directory}"
target_directory = f"/{target_directory}"

if "Drive" in target_connection_settings:
    del(target_connection_settings["Drive"])
if "Drive" in target_settings:
    del(target_settings["Drive"])

if "sheet_name" in target_settings:
    del(target_settings["sheet_name"])

del(target_settings["File"])
if "Directory" in target_settings:
    del(target_settings["Directory"])
if "Directory" in target_connection_settings:
    del(target_connection_settings["Directory"])


workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=spark.conf.get("trident.lakehouse.name")
sql_end_point= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point}"
pattern = '[ ,;{}()\n\t/=]'

engine = create_engine(connection_string)

source_objects = source_settings["SourceObject"]
default_database = source_settings["DefaultDatabase"]
if not isinstance(source_objects,list):
    source_objects = [source_objects]
queries = []

for source_object in source_objects:
    trimmed_object = source_object.lower().strip()
    if " " in trimmed_object and not trimmed_object.startswith("usp_") and ".usp_" not in trimmed_object:
        queries.append(source_object)
        continue

    source_object_separators = source_object.count(".")
    if source_object_separators == 0:
        source_object = f"{default_database}.dbo.{source_object}"
    elif source_object_separators == 1:
        source_object = f"{default_database}.{source_object}"

    queries.append(f'{"EXEC" if ".usp_" in source_object else "SELECT * FROM"} {source_object}')

if "index" not in target_settings:
    target_settings["index"] = False

if sheet_names is None:
    sheet_names = [f"Sheet{i+1}" for i in range(0,len(queries))]

if not isinstance(sheet_names,list):
    sheet_names = [sheet_names]

if len(queries) > 1 and file_type in CSV_FILE_TYPES:
    raise ValueError("More than one query provided with a CSV file. CSV files only support one query.")
if len(queries) > 1 and file_type in XML_FILE_TYPES:
    raise ValueError("More than one query provided with an XML file. XML files only support one query.")
if len(queries) != len(sheet_names):
    raise ValueError(f"{len(queries)} queries provided, but only {len(sheet_names)} sheet names.")

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
site_id = site["id"]
drive = get_sharepoint_drive(site["id"], target_drive_name, headers)
first_run = True


buffer = io.BytesIO()
if file_type in EXCEL_FILE_TYPES:
    excel_writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
with engine.connect() as alchemy_connection:
    for query,sheet_name in zip(queries,sheet_names):
        df = pd.read_sql_query(query, alchemy_connection)
        first_run = False
        while '(' in target_file and ')' in target_file:
            start_idx = None
            stack = []

            for i, char in enumerate(target_file):
                if char == '(':
                    if start_idx is None:
                        start_idx = i
                    stack.append(i)
                elif char == ')':
                    if stack:
                        stack.pop()
                        if not stack:
                            end_idx = i
                            break

            if start_idx is None or end_idx is None:
                break

            placeholder = target_file[start_idx + 1:end_idx]
            if placeholder in df.columns:
                last_value = df[placeholder].iloc[-1]
                target_file = target_file[:start_idx] + str(last_value) + target_file[end_idx + 1:]
            else:
                evaluated_value = eval(placeholder)
                target_file = target_file[:start_idx] + str(evaluated_value) + target_file[end_idx + 1:]

        display(df)
        target_file = re.sub(r'[\/\\:*?"<>|]', '', target_file)
        if file_type in EXCEL_FILE_TYPES:
            df.to_excel(excel_writer, sheet_name=sheet_name, **target_settings)
            print(f"- {len(df)} row(s) from '{query}' written to {sheet_name}.")
        elif file_type in CSV_FILE_TYPES:
            df.to_csv(buffer, **target_settings)
            print(f"- {len(df)} row(s) from '{query}' written to csv.")
            break
        elif file_type in XML_FILE_TYPES:
            df.to_xml(buffer, **target_settings)
            print(f"- {len(df)} row(s) from '{query}' written to xml.")
            break
            
        else:
            raise ValueError(f"Unsupported filetype on target File '{file_type}'.")

if file_type in EXCEL_FILE_TYPES:
    excel_writer.close()

target_file = datetime.utcnow().strftime(target_file)

target_path = os.path.join(target_directory, target_file)

file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive['id']}{target_path}:/content"

files_response = requests.put(file_url, headers=headers, data=buffer.getvalue())
files_response.raise_for_status()

if file_type in EXCEL_FILE_TYPES:
    print(f"{len(queries)} sheet(s) written to {files_response.json()['webUrl']}")
elif file_type in CSV_FILE_TYPES:
    print(f"CSV written to {files_response.json()['webUrl']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
