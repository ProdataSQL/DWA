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

# CELL ********************

pip install azure-ai-documentintelligence 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

SourceSettings = '{"directory": "landing/ai/contracts", "delete": false}'
TargetSettings = '{"directory": "landing/ai/contracts/markdown" }'
SourceConnectionSettings = '{"diEndpoint": "https://di-ne-dev.cognitiveservices.azure.com/",  "diSecret" : "di-ne-key", "KeyVault": "https://dw-training-kv.vault.azure.net/"}'
TargetConnectionSettings = '{}'
ActivitySettings=None # add Split Marker 
LineageKey : str = '00000000-0000-0000-0000-000000000000'
RunId : str = '00000000-0000-0000-0000-000000000000'


#Unit Tests
#Sample Public Contracts
SourceSettings = '{"directory": "landing/ai/contracts", "delete": false}'
TargetSettings = '{"directory": "landing/ai/contracts/markdown" , "lakehouseId":"f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c", "workspaceId":"9b8a6500-5ccb-49a9-885b-b5b081efed75"}'
ActivitySettings=None # add archive directory and whree to split

#Invoices 
SourceSettings = '{"directory": "landing/ai/invoices/pdf"}'
TargetSettings = '{"directory": "landing/ai/invoices/markdown" }'
ActivitySettings=None # add archive directory and whree to split


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Function to Split One Document into Multiple using SplitMarker
import re
import time
PAGE_BREAK = "<!-- PageBreak -->"

def make_marker_regex(phrase: str) -> re.Pattern:
    p = phrase.strip()
    if p.endswith(":"):
        p = p[:-1].rstrip()
    # Escape each token, join with flexible whitespace
    tokens = re.split(r"\s+", p)
    pattern = r"\b" + r"\s+".join(re.escape(t) for t in tokens if t) + r"\s*:?"
    return re.compile(pattern, re.IGNORECASE)

def sections_from_text(text: str, marker_phrase: str):
    marker_re = make_marker_regex(marker_phrase)
    pages = [p.strip() for p in text.split(PAGE_BREAK)]
    starts = [i for i, pg in enumerate(pages) if marker_re.search(pg)]
    if not starts:
        return []

    sections = []
    for idx, start in enumerate(starts):
        end = (starts[idx + 1] - 1) if idx + 1 < len(starts) else (len(pages) - 1)
        body = ("\n\n" + PAGE_BREAK + "\n\n").join(pages[start : end + 1])
        sections.append({
            "section_id": idx + 1,
            "start_page": start + 1,
            "end_page": end + 1,
            "num_pages": end - start + 1,
            "content": body,
        })
    return sections


#This is to fix a bug in Reading PDF Headings with Azure Document Intelligence
def format_document_headings(doc):
    lines = doc.split('\n')
    out = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        # Fix escaped dot (e.g. '10\.') to '10.'
        line = re.sub(r'(\d+)\\\.', r'\1.', line)

        # Check if line is only numbering e.g. '10.', '10.1', etc.
        if re.match(r'^\d+(\.\d+)*\.?$', line):
            next_line = lines[i+1].strip() if i + 1 < len(lines) else ''

            if next_line:
                # For level-1 headings (single number + dot), add '## ' prefix
                if re.match(r'^\d+\.$', line):  
                    combined = f'## {line} {next_line}'
                else:
                    combined = f'{line} {next_line}'

                out.append(combined)
                i += 2
                continue

        out.append(line)
        i += 1

    return '\n'.join(out)


class CustomTokenCredential:
    def get_token(self, *scopes, **kwargs):
        return AccessToken(notebookutils.credentials.getToken('storage'), expires_on=int(time.time()) + 3000)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import time
from operator import itemgetter
import sempy.fabric as fabric
import fsspec  
from pathlib import PurePath
from azure.core.credentials import AzureKeyCredential,AccessToken
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeOutputOption

#Settings
source_connection_settings = json.loads(SourceConnectionSettings or "{}")
keyvault,di_secret,di_endpoint, =  itemgetter("KeyVault", "diSecret","diEndpoint")(json.loads(SourceConnectionSettings or "{}"))
source_lakehouse_id = source_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
source_workspace_id = source_connection_settings.get("workspaceId",fabric.get_workspace_id())
source_lakehouse_name = source_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=source_lakehouse_id, workspace=source_workspace_id))
source_workspace_name = fabric.resolve_workspace_name(source_workspace_id)

target_connection_settings = json.loads(TargetConnectionSettings or '{}')
target_lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
target_workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
target_lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=target_lakehouse_id, workspace=target_workspace_id))
target_workspace_name = fabric.resolve_workspace_name(target_workspace_id)

source_settings = json.loads(SourceSettings or "{}")
delete =   bool(source_settings.get("delete", False))
source_directory = source_settings["directory"]
source = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Files"
source_path = os.path.join(source, source_directory)

target_settings = json.loads(TargetSettings or "{}")
processed_directory = target_settings["directory"]
processed_path = os.path.join(source, processed_directory )
activity_settings = json.loads(ActivitySettings or "{}")
split_marker=  activity_settings.get("split_marker", None)


if not mssparkutils.fs.exists(source_path) or len(mssparkutils.fs.ls(source_path)) == 0:
    mssparkutils.notebook.exit(0) # no folder to process, quit

files  = mssparkutils.fs.ls(source_path)
files = [fi for fi in files if getattr(fi, "size", 0) and int(getattr(fi, "size", 0)) > 0]  #Fiter out Folders and zero length files

credential = AzureKeyCredential(notebookutils.credentials.getSecret(keyvault, di_secret))
document_intelligence_client = DocumentIntelligenceClient(di_endpoint, credential)

start_time=time.time()
for file in files:
    file_path = file.path
    file_name = PurePath(file_path).name
    print(f"Processing: {file_name}")
    
    fs = fsspec.filesystem("abfss", account_name="onelake" , account_host="onelake.dfs.fabric.microsoft.com", credential=CustomTokenCredential())
    
    #with open(file_path, "rb") as f:
    with fs.open(file_path, "rb") as f:
        poller = document_intelligence_client.begin_analyze_document(
            "prebuilt-layout",
            body=f.read(),
            output=[AnalyzeOutputOption.FIGURES],
            output_content_format="MARKDOWN"
        )
    
    result = poller.result()
    markdown_contents = format_document_headings(result.content)#Fix for Bug in document intelligence


    if split_marker:
        for section in sections_from_text( markdown_contents, split_marker):
            file= os.path.join(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Files", processed_directory, f"{file_name}_section_{section['section_id']}.md")
            notebookutils.fs.put( file, section['content'],overwrite=True)
            print (f" -Wrote {file_name}_section_{section['section_id']}.md")
    else:
        file= os.path.join(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Files", processed_directory, f"{file_name}.md")
        notebookutils.fs.put(file, markdown_contents,overwrite=True)
        print (f" -Wrote {file_name}.md")
    if delete:
        notebookutils.fs.rm(file_path)

elapsed = time.time() - start_time
print(f"Processed {len(files)} Documents in {elapsed:.0f} secs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
