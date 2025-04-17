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

# #### Ingest-SP
# 
# This notebook connects to SharePoint using the Microsoft Graph API to download one or multiple files from a specified document library directory. It handles authentication via Azure Key Vault and dynamically constructs API calls to support wildcard file patterns. Files are securely retrieved and saved to a specified target directory in the Fabric Lakehouse environment, ensuring compatibility with downstream data processing.
# 
# Authentication is handled by "Sharepoint-Shared-Function"

# PARAMETERS CELL ********************

SourceConnectionSettings = '{ "tenant_id":"d8ca992a-5fbe-40b2-9b8b-844e198c4c94","app_client_id":"app-fabricdw-dev-clientid","app_client_secret":"app-fabricdw-dev-clientsecret","keyvault":"kv-fabric-dev","sharepoint_url":"prodata365.sharepoint.com","site":"Fabric"}'
TargetConnectionSettings = None
SourceSettings = '{"Drive":"Unittest", "Directory" : "tst", "File" : "tst_Account.csv"}'
TargetSettings = '{"Directory": "landing/sharepoint","File" : "Account.csv"}'
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
from notebookutils import mssparkutils
from datetime import datetime
import fnmatch

SourceSettings = SourceSettings or '{}'
SourceConnectionSettings = SourceConnectionSettings or '{}'
TargetSettings = TargetSettings or '{}'

source_connection_settings = json.loads(SourceConnectionSettings)
source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)

tenant_id = source_connection_settings["tenant_id"]
client_id = source_connection_settings["app_client_id"]
keyvault = source_connection_settings["keyvault"]
client_secret_name = source_connection_settings["app_client_secret"]
sharepoint_url = source_connection_settings["sharepoint_url"]
site_name = source_connection_settings["site"]
URL = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_url}:/sites/{site_name}"


source_directory = source_settings["Directory"].strip("/")
source_drive_name = source_settings.get("Drive", "Documents")
source_file = source_settings.get("File", "*")

if not source_directory.startswith("root:/"):
    source_directory = f"root:/{source_directory}"
source_directory = f"/{source_directory}"

target_directory = target_settings["Directory"]
target_file = target_settings.get("File", "")

FILES_PREFIX = "Files"
if not target_directory.startswith(FILES_PREFIX):
    target_directory = os.path.join(FILES_PREFIX, target_directory.lstrip("/"))


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
t = datetime.now()
drive = get_sharepoint_drive(site["id"], source_drive_name, headers)

#print (f"- Got drive {source_drive_name} ({drive['id']})  in {(datetime.now()-t).total_seconds()} seconds ")

t = datetime.now()

files = get_sharepoint_files_wildcard(site['id'], drive['id'], source_directory, source_file)

is_wildcard : bool = "*" in source_file or "?" in source_file
target_file = target_file if not is_wildcard else ""
t = datetime.now()
for file in files:
    source_path = os.path.join(source_directory, file["name"])
    file_info = get_sharepoint_file_info(site['id'], drive['id'], source_path, headers)
    
    download_url = file_info["@microsoft.graph.downloadUrl"]

    #print (f"- Got download url in {(datetime.now()-t).total_seconds()} seconds ")

    target_file_name = file_info["name"] if is_wildcard or not target_file else target_file
    
    t1 = datetime.now()

    file_stream = requests.get(download_url)
    
    file_stream.raise_for_status()

    if not os.path.exists(target_directory):
        os.makedirs(target_directory)

    target_path = os.path.join(target_directory, target_file_name)
    mssparkutils.fs.put(target_path, str(file_stream.content), True)

    print (f"- File \"{file_info['name']}\" downloaded to \"{target_path}\" in {(datetime.now()-t1).total_seconds()} seconds ")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
