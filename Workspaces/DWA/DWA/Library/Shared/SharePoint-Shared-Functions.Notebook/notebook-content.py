# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### SharePoint-Shared-Functions
# 
# This notebook provides functions to interact with SharePoint using Microsoft Graph API. It supports authentication via Azure Active Directory, retrieves SharePoint site, drive, and list details, and allows for operations like fetching files, columns, and rows, including handling wildcards for file searches. It includes error handling for missing resources, making it suitable for automating data retrieval from SharePoint and integrating with other data processing workflows.

# CELL ********************

import uuid
import fnmatch
def is_uuid(input: str) -> bool:
    try:
        uuid.UUID(input)
        return True
    except ValueError:
        return False
# Requires keyvault, client_secret_name, client_id, client_secret, tenant_id, site_name and sharepoint_url must be set to call SharePoint-Auth-Site 
def get_sharepoint_token(tenant_id: str, client_id: str, keyvault : str, client_secret_name : str) -> str:
    if  ".vault.azure.net" not in keyvault:
        keyvault = f"https://{keyvault}.vault.azure.net/"
    if not is_uuid(client_id):
        client_id = mssparkutils.credentials.getSecret(keyvault, client_id)
    client_secret = mssparkutils.credentials.getSecret(keyvault, client_secret_name)
    token_request_body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource" : "https://graph.microsoft.com/"
    }

    token_response = requests.post(f"https://login.microsoftonline.com/{tenant_id}/oauth2/token", data=token_request_body)
    token_response.raise_for_status()

    return token_response.json()["access_token"]
class SharePointSiteNotFoundException(Exception):
    pass

class SharePointDriveNotFoundException(Exception):
    pass

class SharePointListNotFoundException(Exception):
    pass

def get_sharepoint_header(access_token: str) -> dict:
    return {
        'Authorization': f'Bearer {access_token}'
    }

def get_sharepoint_site(sharepoint_url:str, site_name: str, headers: dict) -> dict:
    site_name  = site_name if "/"  in site_name else f"sites/{site_name}"
    site_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_url}:/{site_name}"
    site_response = requests.get(site_url, headers=headers)
    if site_response.status_code == 404:
        try: site_response.raise_for_status()
        except e: raise ExceptionGroup("", [SharePointSiteNotFoundException(f"{site_name} was not found."), e])
    site_response.raise_for_status()
    return site_response.json()

def get_sharepoint_drive(site_id: str, drive_name: str, headers: dict) -> dict:
    drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/"
    drives_response = requests.get(drives_url, headers=headers)
    drives_response.raise_for_status()
    drives = drives_response.json()['value']

    selected_drive = next(
        (drive for drive in drives if drive["name"] == source_drive_name),
        None
    )
    if selected_drive is None:
        drives_string = ", ".join(f"{item['name']} ({item['id']})" for item in drives)
        raise DriveNotFoundException(f"List \"{source_drive_name}\" not found on \"{site_name}\". Available drives are: {drives_string}.")
    return selected_drive

def get_sharepoint_file_info(site_id:str, drive_id:str, file_path: str,headers:dict): 
    file_path = file_path.strip("/")
    if not file_path.startswith("root:/"):
        file_path = f"root:/{file_path}"
    file_path = f"/{file_path}"

    files_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}{file_path}"
    files_response = requests.get(files_url, headers=headers)
    files_response.raise_for_status()
    return files_response.json()

def get_sharepoint_list(site_id:str, list_name: str,headers:dict) -> dict: 
    lists_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$select=name,id,displayName,webUrl"
    lists_response = requests.get(lists_url, headers=headers)
    lists_response.raise_for_status()
    lists = lists_response.json()['value']

    selected_list = next(
        (l for l in lists if l["name"] == source_list or l["displayName"] == source_list),
        None
    )

    if not selected_list:
        lists_string = ", ".join(f"{item['name']} ({item['displayName']})" for item in lists)
        raise SharePointListNotFoundException(f"List \"{source_list}\" not found on \"{site_name}\". Available lists are: {lists_string}.")

    if selected_list["displayName"] == source_list and selected_list["displayName"] != selected_list["name"]:
        print(f"Warning: Used displayName (\"{selected_list['displayName']}\") of this list, instead of its logical name \"{selected_list['name']}\". Update SourceSettings (ADFPipelines table) to use this logical name instead.")

    return selected_list

def get_sharepoint_list_column_rename_map(site_id: str, list_id: str,headers:dict) -> dict:
    columns_list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}?expand=columns"
    columns_response = requests.get(columns_list_url, headers=headers)
    columns_response.raise_for_status()
    data = columns_response.json()
    column_rename_map = {}
    return  {
        column["name"]: column["displayName"]
        for column in data['columns']
    }

def get_sharepoint_list_rows(site_id:str, list_id: str):
    list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?expand=columns,items(expand=fields)"

    all_items = []
    while list_url:
        list_response = requests.get(list_url, headers=headers)
        list_response.raise_for_status()

        data = list_response.json()

        all_items.extend(data['value'])
        # Check if there is a next link for pagination
        list_url = data.get('@odata.nextLink')
    return all_items
    
def get_sharepoint_files_wildcard(site_id:str, drive_id:str, source_directory:str, file_name: str) -> list:
    if not source_directory.startswith("/"):
        source_directory = f"/{source_directory}"
    directory_list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}{source_directory}:/children?$select=name,id,folder"
    directory_list_response = requests.get(directory_list_url, headers=headers)
    directory_list_response.raise_for_status()
    files_list = directory_list_response.json()


    return list(filter(lambda item: fnmatch.fnmatch(item["name"], file_name) and "folder" not in item, files_list["value"]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
