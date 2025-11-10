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
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ### Extract-AI-LH
# Run a GPT Prompt on one or more files, or without any files<BR>
# Files can be markdown files, txt or PDF
# Output is written to LH tables
# 
# #### Parameters
# ##### SourceConnectionSettings
# - api_provider [openai or azure_openai]. We recommend azure_openai if you wnat to use stronger entra authentication
# - api_family [chat or responses]. We recocommend responses if your region supports it
# - ai_endpoint = URL of AI end point. Supports Azure Open AI, Azure AI Foundry, Open AI, Perplexity and others
# - ai_secret. The secret name in KeyVault for API Key
# - keyvault. The URL of the KeyVault
# - api_version. The API version (Azure Open AI only). Eg 2025-04-01-preview
# - max_retries. The maximum number of retries is AI compute too busy (HTTP 429)
# 
# ##### ActivitySettings
# The body of request to by sent AI client as per 
# Responses and Completions API Documentation<BR>
# https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/responses?tabs=python-key
# https://learn.microsoft.com/en-us/azure/ai-foundry/openai/how-to/chatgpt
# 
# 
# 
# 
# 
# 
# 


# CELL ********************

!pip install openai
import importlib,sys
sys.modules.pop("typing_extensions", None)
import typing_extensions
importlib.reload(typing_extensions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

import json
SourceSettings = '{"Directory": "landing/ai/invoices/markdown", "File":"*.md", "delete":"False"}'
TargetSettings = '{ "SchemaName":"ai","TableName": "invoice1",  "Directory":"landing/ai/invoices//processed/"}'
SourceConnectionSettings = '{"api_family":"responses", "api_provider":"azure_openai","client_id":"dwa-training-app-id","client_secret":"dwa-training-app-secret","ai_endpoint":"https://ai-foundry-training-swe.cognitiveservices.azure.com/","KeyVault":"https://dw-training-kv.vault.azure.net/"}'
SinkConnectionSettings = '{}'
ActivitySettings='{}'
LineageKey : str = '00000000-0000-0000-0000-000000000000'   #Unique Identifier. Will be Random if Zero or None and not passed
RunId : str = '00000000-0000-0000-0000-000000000000'

#Unit Test for Sample Extract, Categorisation and basic reasoning on Invoices
ActivitySettings={"model":"gpt-4.1","instructions":"You are a specialized data extraction assistant for invoices","input":"Extract These Fields and return a single json object:Supplier : Customer, InvoiceNo, OrderNo, InvoiceDate, DueDate, Items: {Json Array of items on Invoice with attributes: description, Qty, Price, SubTotal}, Tax, Total,PaymentTerms,Valid: Return yes if the sum of the SubTotals equals the total on the invoice,Sector: Return the Industry Sector for the Supplier or Services","temperature":0.3}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import re
import ast
from operator import itemgetter

source_connection_settings = json.loads(SourceConnectionSettings or '{}')
source_settings = json.loads(SourceSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')
activity_settings = dict(ActivitySettings) if isinstance(ActivitySettings, dict) else json.loads(ActivitySettings or '{}')
if isinstance(activity_settings.get("temperature"), str): activity_settings["temperature"] = float(activity_settings["temperature"])

keyvault= source_connection_settings.get("KeyVault",None)
kv_client_id= source_connection_settings.get("client_id",None)
kv_client_secret= source_connection_settings.get("client_secret",None)
open_ai_secret_name =  source_connection_settings.get("ai_secret",None)
open_ai_max_retries = source_connection_settings.get("max_retries",1)
open_ai_endpoint = source_connection_settings.get("ai_endpoint",None)
open_ai_api_version = source_connection_settings.get("api_version","2025-04-01-preview")
api_family = source_connection_settings.get("api_family","responses")
api_family = "chat_completions" if "chat" in str(api_family).lower() else "responses"
api_provider = source_connection_settings.get("api_provider","azure_openai")
api_provider = "azure_openai" if "azure" in str(api_provider) else "openai"

model =  activity_settings.pop("model","gpt-4.1-mini") 
delete =   bool(ast.literal_eval(source_settings.get("Delete", "False")))
response_schema=activity_settings.get("schema",None) 
response_schema = json.loads (response_schema)  if isinstance(response_schema, str) else (response_schema or {})

table_name = target_settings.get("TableName","content")
schema_name =  target_settings.get("SchemaName","dbo")
table_layout = target_settings.get("table_layout","content")
source_directory = source_settings.get("Directory",None)
source_file = source_settings.get("File", "*.md")
processed_directory = target_settings.get("Directory",None)

if source_directory and source_directory.startswith("Files"):
    source_directory = os.path.join("Files",source_directory)
if processed_directory and not processed_directory.startswith("Files"):
    processed_directory = os.path.join("Files",processed_directory)

post_extract_spark_sql = activity_settings.pop('PostExtractSparkSQL', None)

if isinstance(model, str):
  models= model.split(",")
  model = [model.strip(" ") for model in models]
elif isinstance(model, list):
  assert(all([isinstance(model,str) for model in models]))
  models = model
else:
  raise ValueError("model must be either a string or a list of strings. Eg gpt-'4.1-mini', 'gpt-4.1'")


LAKEHOUSE_DEFAULT_PREFIX = "/lakehouse/default/"
FILES_DEFAULT_PREFIX = "Files"
if source_directory:
    if not source_directory.startswith(FILES_DEFAULT_PREFIX):
        source_directory = os.path.join(FILES_DEFAULT_PREFIX,source_directory)
    if not source_directory.startswith(LAKEHOUSE_DEFAULT_PREFIX):
        source_directory = os.path.join(LAKEHOUSE_DEFAULT_PREFIX,source_directory)
if processed_directory:
    if not processed_directory.startswith(FILES_DEFAULT_PREFIX):
        processed_directory = os.path.join(FILES_DEFAULT_PREFIX,processed_directory)
    if not processed_directory.startswith(LAKEHOUSE_DEFAULT_PREFIX):
        processed_directory = os.path.join(LAKEHOUSE_DEFAULT_PREFIX,processed_directory)

def clean_cols(df, names=None, pat=r'[ ,;{}()\n\t=]', replace=None):
    cols = list(names or df.columns)
    new  = [re.sub('_+','_', re.sub(pat,'_', c)).strip('_') for c in cols]
    if replace:  # optional dict of renames
        new = [replace.get(orig, replace.get(n, n)) for orig, n in zip(cols, new)]
    return df.toDF(*new)


def build_structured_input(prompt: str, file_ids=None, role: str = "user"):
    content = [{"type": "input_text", "text": prompt}]
    if file_ids:
        for fid in (file_ids if isinstance(file_ids, (list, tuple)) else [file_ids]):
            content.append({"type": "input_file", "file_id": fid})

    return [{
        "role": role,
        "content": content
    }]

def azure_ad_token_provider():
    return credential.get_token("https://cognitiveservices.azure.com/.default").token

def normalize_json_types(obj):
    """Recursively convert ints -> floats and ensure lists/dicts are consistent. Need this to avoid data type mismatch."""
    if isinstance(obj, dict):
        return {k: normalize_json_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [normalize_json_types(v) for v in obj]
    elif isinstance(obj, int):   # promote all ints to floats
        return float(obj)
    else:
        return obj

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
from openai import AzureOpenAI
from openai import OpenAI
from azure.identity import ClientSecretCredential
import re
import sempy.fabric as fabric 
import pandas as pd
from fnmatch import filter as fnfilter
from typing import Dict, Any, Iterable, Tuple, List
from io import BytesIO
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timezone
import base64, os

rows = []
open_ai_max_retries = int(open_ai_max_retries) if open_ai_max_retries is not None else 2
if kv_client_id:
  tenant_id=spark.conf.get("trident.tenant.id")
  client_id     = notebookutils.credentials.getSecret(keyvault, kv_client_id)
  client_secret = notebookutils.credentials.getSecret(keyvault, kv_client_secret)
  credential = ClientSecretCredential(tenant_id, client_id, client_secret)
  def azure_ad_token_provider():
    # Return a fresh token each time (auto-renew via azure-identity)
    return credential.get_token("https://cognitiveservices.azure.com/.default").token
  auth_kw= {"azure_ad_token_provider": azure_ad_token_provider}
else:
  api_key = notebookutils.credentials.getSecret(keyvault, open_ai_secret_name)
  auth_kw = {"api_key": api_key}

if api_provider== "azure_openai": 
  client = AzureOpenAI(
    **auth_kw,
    api_version    = open_ai_api_version,
    azure_endpoint = open_ai_endpoint,
    max_retries=open_ai_max_retries
  )
else:
  if open_ai_endpoint:
    open_ai_endpoint = (lambda s: urlunparse((p:=urlparse(s))._replace(path=((p.path.rstrip('/') + '/openai/v1') if ('azure.com' in s.lower() and not p.path.rstrip('/').endswith('/openai/v1')) else p.path))))(open_ai_endpoint)
    client = OpenAI(
        api_key      = notebookutils.credentials.getSecret(keyvault, open_ai_secret_name),
        base_url= open_ai_endpoint,
        max_retries=open_ai_max_retries
      )
  else:
    client = OpenAI(
        api_key      = api_key,
        max_retries=open_ai_max_retries
      )

if not source_directory:  
    files_df = pd.DataFrame([{"FileName": "", "SourceDirectory": ""}])
else:
  files =  [file for file in os.listdir(source_directory) if os.path.isfile(os.path.join(source_directory, file))]
  files = fnfilter(files, source_file)
  files_df = pd.DataFrame({"FileName": files})
  files_df["SourceDirectory"] = source_directory[len(LAKEHOUSE_DEFAULT_PREFIX):]

if api_family=="responses":
  activity_settings.setdefault("text",{"format": {"type": "json_object"}})
  prompt = activity_settings["input"] if isinstance(activity_settings.get("input"), str) else next((p["text"] for m in activity_settings["input"] for p in m.get("content", []) if p.get("type") == "input_text"), None)
else:
  prompt = next((m["content"] for m in activity_settings.get("messages", []) if m.get("role") == "user"), None)

if "response_format" in activity_settings:
    is_json = activity_settings.get("response_format", {}).get("type").lower().startswith("json")
elif "text" in activity_settings:
    is_json = activity_settings.get("text", {}).get("format", {}).get("type").lower().startswith("json")
else:
    is_json=False 


parent_df: Dict[str, pd.DataFrame] = {}
child_df: Dict[str, pd.DataFrame] = {}
  
start_time=time.time()
doc_count=0
file_user_prompt=None

for index, row in files_df.iterrows(): 
    doc_count+=1
    file_path = os.path.join(source_directory, row["FileName"]) if source_directory else None
    file_name = row["FileName"]
    print(f"Processing: {file_name if source_directory else 'Live Web Search or Reasoning'}")

    file_activity_settings=activity_settings
    if file_path:
      if file_path.lower().endswith(".pdf") and api_family=="responses": 
        if api_provider=="openai":
          file = client.files.create(
            file=open(file_path, "rb"),
            purpose="assistants"
          )
          file_user_prompt= build_structured_input(prompt, file_ids=file.id)
        else: #No Azure Open AI Support for file upload yet, so using base 64 encoding
          with open(file_path, "rb") as f:
            b64_pdf =base64.b64encode(f.read()).decode("utf-8")
          content = [{"type": "input_text", "text": prompt}]
          content.append({"type": "input_file", "mime_type": "application/pdf", "data": b64_pdf})
          file_user_prompt =[{"role": "user", "content": content }]
      else:
        with open(file_path, "rb") as f:
            file_user_prompt = f"{prompt}<INPUT>{f.read()}</INPUT>"
      if api_family=="responses":
          file_activity_settings['input']=file_user_prompt           
      else:
          next(m for m in file_activity_settings["messages"] if m.get("role") == "user")["content"] = file_user_prompt

    for model in models:
      model=model.strip()
      gpt_start_time=time.time()
      activity_settings['model']=model

      if api_family=="responses":
        response = client.responses.create(**file_activity_settings)
        content = response.output_text

        usage=json.dumps(response.usage.to_dict(), indent=2)
      else:
        response = response = client.chat.completions.create(**file_activity_settings)
        content = response.choices[0].message.content
        usage=json.dumps(response.usage.to_dict(), indent=2)

      if is_json and table_layout !="content":
        content_json=json.loads(content)
      else:
        content_json={"content":content}
      content_json['id']=response.id
      content_json['usage']=usage
      if file_name!="":
        content_json['file_name']=file_name 
      gpt_elapsed= time.time() -gpt_start_time
      content_json['duration_secs']= int(round(gpt_elapsed))
      content_json['model']=model
      content_json['created_date']=datetime.now(timezone.utc)  
      rows.append(content_json)
      print(f" - Prompt with Model [{model}] took {gpt_elapsed:.0f} secs")

rows = [normalize_json_types(r) for r in rows]
df = spark.createDataFrame(rows)  
display(df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, FloatType
from pyspark.sql import types as T

if table_layout == "multiple":
    parent_df=df
    child_obj={}
    array_cols = [f.name for f in df.schema.fields if 'array' in f.dataType.simpleString()]
    items_df = (
        parent_df
        .select("id", F.explode_outer("Items").alias("item"))
    )
    for item in array_cols:
        keys = (
            parent_df
            .select(F.explode_outer(item).alias("m"))
            .select(F.explode_outer(F.map_keys("m")).alias("k"))
            .distinct()
            .rdd.map(lambda r: r[0])
            .collect()
        )
        item_df = (
            parent_df
            .select("id", F.explode_outer(item).alias("item"))
            .select(
                "id",
                *[F.element_at("item", k).alias(k) for k in keys]
            )
        )
        child_obj[item]=item_df
else:
    parent_df=df
    child_obj=None


#Write Top Level Table. Eg ai.invoice
doc_start_time=time.time()
parent_df =  parent_df.select([
    F.col(c).cast(FloatType()) if isinstance(df.schema[c].dataType, NumericType) else F.col(c)
    for c in  parent_df.columns
])
full_table_name = f"{schema_name}.{table_name}"
parent_df=clean_cols(parent_df)
parent_df.write.mode("append").saveAsTable(full_table_name)
elapsed = time.time() - doc_start_time
print(f"  - Wrote {parent_df.count()} rows to {full_table_name} in {elapsed:.0f} secs")


# Save to LH and Archive FIles if needed
#Write Nested Tables. Eg ai.invoice_items
if child_obj:
    doc_start_time=time.time()
    for  k, v in child_obj.items():
        nested_table_name = f"{schema_name}.{table_name}_{k}"
        print (nested_table_name)
        if v:
            v=clean_cols(v)
            v.write.mode("append").saveAsTable(nested_table_name)
            elapsed = time.time() - doc_start_time
            print(f"  - Wrote {v.count()} rows to {nested_table_name} in {elapsed:.0f} secs")
        else:
            elapsed = time.time() - doc_start_time

#Arechive Files if needed
for index, row in files_df.iterrows(): 
    file_name = row["FileName"]
    file_path = os.path.join(source_directory[len(LAKEHOUSE_DEFAULT_PREFIX):], file_name)
    processed_file_path = os.path.join(processed_directory[len(LAKEHOUSE_DEFAULT_PREFIX):], file_name)
    if notebookutils.fs.exists(file_path) and processed_directory:
        processed_file_path = os.path.join(processed_directory[len(LAKEHOUSE_DEFAULT_PREFIX):], file_name)
        if notebookutils.fs.exists(processed_file_path):
            notebookutils.fs.rm(processed_file_path)
        if delete:
            notebookutils.fs.mv(file_path, processed_file_path, True, True)
        else:
            notebookutils.fs.cp(file_path, processed_file_path, True)

print ('')
elapsed = time.time() - start_time
print(f"Completed. Extracted {doc_count} documents in {elapsed:.0f} secs")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
