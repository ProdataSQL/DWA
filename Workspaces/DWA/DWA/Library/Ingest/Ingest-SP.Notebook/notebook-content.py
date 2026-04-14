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

# #### Ingest-SP
# 
# This notebook connects to SharePoint using the Microsoft Graph API to download one or multiple files from a specified document library directory. It handles authentication via Azure Key Vault and dynamically constructs API calls to support wildcard file patterns. Files are securely retrieved and saved to a specified target directory in the Fabric Lakehouse environment, ensuring compatibility with downstream data processing.
# 
# Authentication is handled by "Sharepoint-Shared-Function"


# PARAMETERS CELL ********************

SourceConnectionSettings = '{"delete":false, "tenantId":"d8ca992a-5fbe-40b2-9b8b-844e198c4c94","appClientId":"app-fabricdw-dev-clientid","appClientSecret":"app-fabricdw-dev-clientsecret","keyVault":"kv-fabric-dev","sharepointUrl":"prodata365.sharepoint.com","site":"Fabric","drive":"Unittest"}'
TargetConnectionSettings = '{"lakehouse": "FabricLH", "lakehouseId":"f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c","workspaceId":"9b8a6500-5ccb-49a9-885b-b5b081efed75"}'
SourceSettings = '{"directory" : "AW", "file" : "*.xlsx"}'
TargetSettings = '{"timestamp":true}'
ActivitySettings = None
LineageKey = 0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import os 
#from notebookutils import mssparkutils
from datetime import datetime
import fnmatch
import requests
import pandas as pd
from io import BytesIO 

source_connection_settings = json.loads(SourceConnectionSettings or '{}')
target_connection_settings = json.loads(TargetConnectionSettings or '{}')
source_settings = json.loads(SourceSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')
tenant_id = source_connection_settings["tenantId"]
client_id = source_connection_settings["appClientId"]
delete = bool(source_connection_settings.get("delete", False))
timestamp = bool(target_settings.get("timestamp", True))
key_vault = source_connection_settings["keyVault"]
client_secret_name = source_connection_settings["appClientSecret"]
sharepoint_url = source_connection_settings["sharepointUrl"]
site_name = source_connection_settings["site"]
URL = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_url}:/sites/{site_name}"
source_directory = source_settings["directory"].strip("/")
source_drive_name = source_connection_settings.get("drive", "Documents")
source_file = source_settings.get("File", "*")
target_directory = target_settings.get("directory")
if not target_directory:
    target_directory=f"Files/Landing/{source_drive_name}/{source_directory}"
if not source_directory.startswith("root:/"):
    source_directory = f"root:/{source_directory}"
source_directory = f"/{source_directory}"
target_file = target_settings.get("File", "")
FILES_PREFIX = "Files"
if not target_directory.startswith(FILES_PREFIX):
    target_directory = os.path.join(FILES_PREFIX, target_directory.lstrip("/"))
target_workspace_id= target_connection_settings["workspaceId"]
target_lakehouse_id = target_connection_settings["lakehouseId"]
timestamp = datetime.now().strftime("%Y%m%d")

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

access_token = get_sharepoint_token(tenant_id, client_id, key_vault, client_secret_name)
headers = get_sharepoint_headers()
site = get_sharepoint_site(sharepoint_url, site_name, headers)

drive = get_sharepoint_drive(site, source_drive_name, headers)

files = get_sharepoint_files_wildcard(site['id'], drive['id'], source_directory, source_file,headers)
is_wildcard : bool = "*" in source_file or "?" in source_file
target_file = target_file if not is_wildcard else ""

for file in files:
    source_path = os.path.join(source_directory, file["name"])
    file_info = get_sharepoint_file_info(site['id'], drive['id'], source_path, headers)
    download_url = file_info["@microsoft.graph.downloadUrl"]
    target_file_name = file_info["name"] if is_wildcard or not target_file else target_file
    file_stream = requests.get(download_url)
    file_stream.raise_for_status()
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    name, ext = os.path.splitext(target_file_name)
    if timestamp:
        target_file_name =f"{name}_{timestamp}{ext}"
    target_file_path = os.path.join(target_directory, target_file_name)
    file_uri = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/{target_file_path}"
    if ext.lower()=='.xlsx':
        excel_data = pd.read_excel(BytesIO(file_stream.content), sheet_name=None)
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            for sheet_name, df in excel_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False) # Could do validation here
        buffer.seek(0)
        local_path = f"/tmp/{target_file_name}"
        with open(local_path, 'wb') as f:
            f.write(buffer.read())
        mssparkutils.fs.cp(f"file:{local_path}", file_uri)
    else: 
        mssparkutils.fs.put(file_uri, str(file_stream.content), True)

    if delete:
        delete_sharepoint_file(drive['id'], source_path)
        print(f"- File \"{target_file_name}\" uploaded to Lakehouse folder \"{target_file_path}\" and deleted from sharepoint\"")
    else:
        print(f"- File \"{target_file_name}\" copied to Lakehouse folder \"{target_file_path}\"")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
