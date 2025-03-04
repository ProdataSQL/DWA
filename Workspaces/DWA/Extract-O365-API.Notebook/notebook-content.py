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

SourceSettings = '{"Url":["getOneDriveUsageAccountDetail", "getMailboxUsageDetail"]}'
TargetSettings = '{"SchemaName": "o365", "condition" : "target.Report_Refresh_Date = source.Report_Refresh_Date ","mode":"merge"}'
SourceConnectionSettings = '{"tenant_id":"d8ca992a-5fbe-40b2-9b8b-844e198c4c94","app_client_id":"app-o365-logs-clientid","app_client_secret":"app-o365-logs-clientsecret","keyvault":"kv-fabric-dev"}'
TargetConnectionSettings = None
ActivitySettings = '{"with_checksum" : true, "dedupe": false}'
LineageKey = '00000000-0000-0000-0000-000000000000'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid

def is_uuid(input: str) -> bool:
    try:
        uuid.UUID(input)
        return True
    except ValueError:
        return False
        
def get_graph_token(tenant_id: str, client_id: str, keyvault : str, client_secret_name : str) -> str:
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
def extract_noun(url):
    pattern = r'get(\w+)(?=\()|get(\w+)'
    search = re.search(pattern, url)
    
    if search:
        return search.group(1) if search.group(1) else search.group(2)
    return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
from datetime import datetime
from io import StringIO
import pandas as pd
import re
from pyspark.sql.functions import lit, input_file_name, expr, sha1,concat_ws, col, coalesce
from delta.tables import DeltaTable

url_business_keys = {"getOneDriveUsageAccountDetail":"Owner_Principal_Name","getMailboxUsageDetail":"User_Principal_Name"}

source_connection_settings = json.loads(SourceConnectionSettings or '{}')
source_settings = json.loads(SourceSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')
activity_settings = json.loads(ActivitySettings or '{}')

tenant_id = source_connection_settings["tenant_id"]
app_client_id = source_connection_settings["app_client_id"]
app_client_secret = source_connection_settings["app_client_secret"]
keyvault = source_connection_settings["keyvault"]

urls = source_settings.pop("Url")
schema_name = target_settings.get("SchemaName", "o365")

write_mode = target_settings.pop("mode", "overwrite")
dedupe = bool(activity_settings.get("dedupe"))
with_checksum = bool(activity_settings.get("with_checksum"))

if write_mode == "merge":
    merge_condition = target_settings.pop("condition")

if isinstance(urls, str):
    urls = [urls]

access_token = get_graph_token(tenant_id, app_client_id, keyvault, app_client_secret)

headers = {
    'Authorization': f'Bearer {access_token}'
}

GRAPH_REPORT_PREFIX = "https://graph.microsoft.com/v1.0/reports/"
for url in urls:
    if not url.startswith("https://graph.microsoft.com/v1.0/reports/"):
        url = f'{GRAPH_REPORT_PREFIX}{url}'
    table_name = extract_noun(url)
    full_table_name = f"{schema_name}.{table_name}".lower()
    if not spark.catalog.tableExists(full_table_name):
        write_mode = "overwrite"
    if any(part in url for part in ["(period", "(date"]):
        report_url = url    
    else:
        if write_mode != "overwrite":
            result = spark.sql(f"SELECT MAX(Report_Refresh_Date) AS MaxReportDate FROM {full_table_name}").collect()
            max_date = result[0]["MaxReportDate"]
            if max_date and "getMailboxUsageDetail" not in url:
                max_date_t = datetime.strptime(max_date, "%Y-%m-%d")
                today = datetime.today()
                report_url = f"{url}(date={max_date})"
            else:
                report_url = f"{url}(period='D90')"
        else:
                report_url = f"{url}(period='D90')"
    print(report_url)
    response = requests.get(report_url, headers=headers)
    response.raise_for_status()

    report_data = response.text
    
    df = pd.read_csv(StringIO(report_data))
    row_count = df.shape[0]
    print (f"- Read {row_count} rows from '{url}' to df. ")

    pattern = '[ ,;{}()\n\t/=%]'

    new_column_names = [re.sub(pattern, '_',  str(col).strip(pattern)) for col in df.columns]
    rename_map = dict(zip(df.columns, new_column_names))

    df.reset_index(inplace=True)
    df['index'] += 2


    df.rename(columns={'index': 'RowNumber'}, inplace=True)
    df = df.rename(columns=rename_map).assign(LineageKey=LineageKey).assign(FileName=url)

    df = spark.createDataFrame(df)

    if with_checksum:
        columns = [column for column in df.columns if column !="Report_Period"]
        df = df.withColumn("RowChecksum", sha1(concat_ws("", *columns)))

    if dedupe:
        df = df.drop_duplicates(["RowChecksum"]) if with_checksum else df.drop_duplicates(clean_headers)
    
    if write_mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

    if write_mode != "merge":
        df.write.mode(write_mode).format("delta").saveAsTable(full_table_name)
    else:
        target_df = DeltaTable.forPath(spark, f"Tables/{full_table_name.replace('.', '/')}")
        if url in url_business_keys:
            specific_merge_condition = f"{merge_condition} AND target.{url_business_keys[url]} = source.{url_business_keys[url]}"
        else:
            specific_merge_condition = f"{merge_condition}"
        target_df.alias("target")\
            .merge(df.alias("source"), merge_condition)\
            .whenNotMatchedInsertAll().execute()
    print (f"- Wrote df to {full_table_name}.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
