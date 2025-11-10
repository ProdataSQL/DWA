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

%pip install openai 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

SourceSettings = '{"directory": "landing/ai/contracts/markdown"}'
TargetSettings = '{"directory": "landing/ai/contracts/archive", "kustoDatabase":"FabricEH", "kustoTable":"contract", "chunkChars":"32768" }'
SourceConnectionSettings = '{"openAIEndpoint": "https://open-ai-we.openai.azure.com/",   "openAISecret" : "open-ai-we-key", "kustoUri":"https://trd-gvbqgkgfycghtwm0z3.z6.kusto.fabric.microsoft.com", "KeyVault": "https://kv-fabric-dev.vault.azure.net/", "model":"text-embedding-3-large"}'
SinkConnectionSettings = '{}'
ActivitySettings='{}' # "ArchiveDirectory": "landing/ai/contracts/Archive/Markdown/"
LineageKey : str = '00000000-0000-0000-0000-000000000000'
RunId : str = '00000000-0000-0000-0000-000000000000'



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from operator import itemgetter
import sempy.fabric as fabric

#Settings
source_connection_settings = json.loads(SourceConnectionSettings or "{}")
keyvault,openai_endpoint, openai_secret,kustoUri =  itemgetter("KeyVault", "openAIEndpoint", "openAISecret","kustoUri")(json.loads(SourceConnectionSettings or "{}"))
source_lakehouse_id = source_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
source_workspace_id = source_connection_settings.get("workspaceId",fabric.get_workspace_id())
source_lakehouse_name = source_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=source_lakehouse_id, workspace=source_workspace_id))
source_workspace_name = fabric.resolve_workspace_name(source_workspace_id)
source_model = source_connection_settings.get("workspaceId","text-embedding-3-large")

source_settings = json.loads(SourceSettings or "{}")
delete =   bool(source_settings.get("delete", False))
source_directory = source_settings["directory"]

target_settings = json.loads(TargetSettings or "{}")
processed_directory = target_settings.get("directory", "")
table_name = target_settings['kustoTable'] 
kustoDatabase= target_settings['kustoDatabase']
CHUNK_CHARS = int(target_settings.get("chunkChars",32768))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
import uuid
import os
import re
import fsspec 
from typing import List, Tuple, Dict, Optional
import pandas as pd
from openai import AzureOpenAI
import requests
from pathlib import PurePath
from azure.core.credentials import AzureKeyCredential,AccessToken

DIM = 3072                                  # 1536 for ada-002, 3072 for text-embedding-3-large
#CHUNK_CHARS = 32768                         #Parameterisd into TargetSettings Set this for smaller chunks. 
CHUNK_OVERLAP = 100
DO_EMBED      = True
EMBED_MODEL   = source_model
EMBED_DIM     = 3072
EMBED_BATCH   = 32
TABLE_DOCS      = f"{table_name}_docs"
TABLE_CHUNKS    = f"{table_name}_chunks"


if DO_EMBED:
    from openai import AzureOpenAI
    client = AzureOpenAI(
        api_key        = notebookutils.credentials.getSecret(keyvault, openai_secret),
        api_version    = "2024-02-01",
        azure_endpoint = openai_endpoint
    )


class CustomTokenCredential:
    def get_token(self, *scopes, **kwargs):
        return AccessToken(notebookutils.credentials.getToken('storage'), expires_on=int(time.time()) + 3000)

def _mgmt(url):  
    return f"{url.rstrip('/')}/v1/rest/mgmt"

def _query(url):
    return f"{url.rstrip('/')}/v1/rest/query"

def get_existing_doc_ids() -> Dict[str, str]:
    token = notebookutils.credentials.getToken("kusto")
    url = _query(kustoUri)   
    csl = f"{TABLE_DOCS} | project file_name, doc_id"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        json={"db": kustoDatabase, "csl": csl},
        timeout=60,
    )
    r.raise_for_status()
    result = r.json()
    doc_id_map = {}
    if "Tables" in result and result["Tables"]:
        table = result["Tables"][0]
        rows = table.get("Rows", [])
        for row in rows:
            if len(row) >= 2:
                file_name, doc_id = row[0], row[1]
                if file_name and doc_id:
                    doc_id_map[file_name] = doc_id
    
    print(f"Found {len(doc_id_map)} existing doc_id mappings")
    return doc_id_map
    

def soft_delete_by_file_name(table: str, file_name: str, whatif: bool = False):
    token = notebookutils.credentials.getToken("kusto")
    url = _mgmt(kustoUri)
    safe = file_name.replace('"', '\\"')
    opts = "with (whatif=true) " if whatif else ""

    csl = f'''.delete table {table} records {opts}<| {table} | where file_name == "{safe}"'''
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        json={"db": kustoDatabase, "csl": csl},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()

H_ATX = re.compile(r'^(?P<hashes>#{1,6})\s+(?P<title>.+?)(?:\s+#+\s*)?$', re.MULTILINE)

def find_setext_headings(text: str) -> List[Tuple[int, str, int]]:
    results = []
    lines = text.splitlines(keepends=True)
    i, pos = 0, 0
    while i < len(lines) - 1:
        title = lines[i].rstrip("\r\n")
        underline = lines[i+1].rstrip("\r\n")
        if title.strip() and underline and set(underline) <= {"="}:
            results.append((1, title.strip(), pos))
            i += 2; pos += len(lines[i-2]) + len(lines[i-1])
        elif title.strip() and underline and set(underline) <= {"-"}:
            results.append((2, title.strip(), pos))
            i += 2; pos += len(lines[i-2]) + len(lines[i-1])
        else:
            pos += len(lines[i]); i += 1
    return results

H_ATX = re.compile(r'^(?P<hashes>#{1,6})\s+(?P<title>.+?)(?:\s+#+\s*)?$', re.MULTILINE)

def parse_headings(md: str) -> List[Tuple[int, str, int]]:
    atx = [(len(m.group("hashes")), m.group("title").strip(), m.start())
           for m in H_ATX.finditer(md)]
    setext = find_setext_headings(md)
    all_h = atx + setext
    all_h.sort(key=lambda x: x[2])
    return all_h

# Regex to detect appendix/exhibit patterns
APPENDIX_EXHIBIT_RE = re.compile(r'^\s*(APPENDIX|EXHIBIT|Appendix|Exhibit)\s+([A-Za-z0-9]+|[IVXLCDMivxlcdm]+)\b', re.IGNORECASE)

def is_appendix_or_exhibit(title: str) -> bool:
    """Check if a title indicates an appendix or exhibit section."""
    match_result = bool(APPENDIX_EXHIBIT_RE.match(title.strip()))
    return match_result

def classify_section_type(title: str) -> str:
    t = (title or "").strip()
    tl = t.lower()
    if tl.startswith("schedule "): return "schedule"
    if tl.startswith("annex "):    return "annex"
    if "table of contents" in tl or tl == "contents": return "toc"
    if is_appendix_or_exhibit(title): return "appendix"
    return "clause"

NUM_PREFIX_RE = re.compile(r'^(?P<num>(?:\d+(?:\.\d+)*|[A-Z]\.|[a-z]\.)|(?:Schedule|Annex)\s+\w+)', re.IGNORECASE)

def split_section_number_and_title(title: str) -> Tuple[str, str]:
    t = (title or "").strip()
    m = NUM_PREFIX_RE.match(t)
    if m:
        num = m.group("num").rstrip(" .:-")
        rest = t[m.end():].lstrip(" .:-")
        return num, rest or t
    return "", t

def find_page_breaks(text: str) -> List[int]:
    page_break_pattern = re.compile(r'<!--\s*PageBreak\s*-->', re.IGNORECASE)
    positions = [match.start() for match in page_break_pattern.finditer(text)]
    return positions

def get_page_number_from_position(char_position: int, page_break_positions: List[int]) -> int:
    if not page_break_positions:
        return 1
    
    page_num = 1
    for break_pos in page_break_positions:
        if char_position >= break_pos:
            page_num += 1
        else:
            break
    return page_num

def get_total_pages_from_breaks(page_break_positions: List[int]) -> int:
    return len(page_break_positions) + 1

def sections_from_markdown(md: str, page_break_positions: List[int]) -> List[Dict]:
    heads = parse_headings(md)  # [(level, title, start_index), ...]
    total_pages = get_total_pages_from_breaks(page_break_positions)

    if not heads:
        return [{
            "section_id": 0,
            "level": 0,
            "title": "Document",
            "path": "Document",
            "type": "other",
            "start": 0,
            "end": len(md),
            "text": md,
            "sec_number": "",
            "page_start": 1,
            "page_end": total_pages
        }]

    sections = []
    section_id_counter = 0
    
    # Check if there's content before the first heading
    if heads and heads[0][2] > 0:  # If first heading doesn't start at position 0
        # Extract content before first heading
        first_heading_pos = heads[0][2]
        pre_heading_text = md[:first_heading_pos].strip()
        
        # Only create a section if there's meaningful content (not just whitespace/comments)
        if pre_heading_text and len(pre_heading_text.replace('<!--', '').replace('-->', '').strip()) > 0:
            page_start = get_page_number_from_position(0, page_break_positions)
            page_end = get_page_number_from_position(first_heading_pos, page_break_positions)
            
            sections.append({
                "section_id": section_id_counter,
                "level": 1,
                "title": "Agreement",
                "sec_number": "",
                "start": 0,
                "end": first_heading_pos,
                "text": md[:first_heading_pos],
                "type": "clause",
                "page_start": page_start,
                "page_end": page_end
            })
            section_id_counter += 1
    
    # Process regular headings with appendix consolidation
    i = 0
    while i < len(heads):
        lvl, title, start = heads[i]
        
        # Find the end of this section
        end = len(md)  # Default to end of document
        
        # Check if this is an appendix/exhibit
        section_type = classify_section_type(title)
        
        if section_type == "appendix":
            # For appendices, include all subsections until we hit the next same-level or higher section
            # or another appendix
            j = i + 1
            while j < len(heads):
                next_lvl, next_title, next_start = heads[j]
                next_type = classify_section_type(next_title)
                
                # Stop if we hit another appendix, or a section at same/higher level
                if next_type == "appendix" or next_lvl <= lvl:
                    end = next_start
                    break
                j += 1
            
            if j == len(heads):  # We reached the end
                end = len(md)
        else:
            # Regular section - find next section at same or higher level
            j = i + 1
            while j < len(heads):
                next_lvl, next_title, next_start = heads[j]
                if next_lvl <= lvl:
                    end = next_start
                    break
                j += 1
            
            if j == len(heads):  # We reached the end
                end = len(md)
        
        # Extract the full text for this section (including any subsections for appendices)
        text = md[start:end]
        sec_num, clean_title = split_section_number_and_title(title)

        page_start = get_page_number_from_position(start, page_break_positions)
        page_end = get_page_number_from_position(end, page_break_positions)

        sections.append({
            "section_id": section_id_counter,
            "level": lvl,
            "title": clean_title,
            "sec_number": sec_num,
            "start": start,
            "end": end,
            "text": text,
            "type": section_type,
            "page_start": page_start,
            "page_end": page_end
        })
        section_id_counter += 1
        
        # Skip the subsections we've already included in appendices
        if section_type == "appendix":
            # Skip all the headers we consumed
            while i + 1 < len(heads) and heads[i + 1][2] < end:
                i += 1
        
        i += 1
    
    # Build hierarchical paths
    stack = []
    for s in sections:
        while stack and stack[-1]["level"] >= s["level"]:
            stack.pop()
        stack.append({"level": s["level"], "title": s["title"]})
        s["path"] = " › ".join(x["title"] for x in stack)

    return sections

def chunk_within_section(text: str, size: int, overlap: int, section_start_char: int, page_break_positions: List[int], section_type: str = "clause") -> List[Dict]:

    # For appendices/exhibits, return the entire section as one chunk
    if section_type == "appendix":
        page_num = get_page_number_from_position(section_start_char, page_break_positions)
        result = [{"text": text, "page_number": page_num}]

        return result
    
    # Regular chunking for non-appendix sections
    if size <= 0: 
        page_num = get_page_number_from_position(section_start_char, page_break_positions)
        return [{"text": text, "page_number": page_num}]
    
    chunks = []
    i = 0
    step = max(size - overlap, 1)
    
    while i < len(text):
        chunk_text = text[i:i+size]
        # Get page number for this chunk based on its position in the overall document
        chunk_char_position = section_start_char + i
        page_number = get_page_number_from_position(chunk_char_position, page_break_positions)
        
        chunks.append({
            "text": chunk_text,
            "page_number": page_number
        })
        i += step
    
    return chunks

def approx_tokens(s: str) -> int:
    # quick proxy if you don't run a tokenizer: ~4 chars per token
    return max(1, int(len(s) / 4))

def embed_batch(texts: List[str]) -> List[List[float]]:
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [d.embedding for d in resp.data]

def batched(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


source_abfss = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Files"
source_path = os.path.join(source_abfss, source_directory)
if not notebookutils.fs.exists(source_path) :
     notebookutils.notebook.exit(0) # no folder to process, quit

files  = notebookutils.fs.ls(source_path)
files = [
    fi for fi in files
    if int(getattr(fi, "size", 0) or 0) > 0
    and str(getattr(fi, "name", getattr(fi, "path", getattr(fi, "__str__", lambda: str(fi))()))).lower().endswith(".md")
]

existing_doc_ids = get_existing_doc_ids()
fs = fsspec.filesystem("abfss", account_name="onelake" , account_host="onelake.dfs.fabric.microsoft.com", credential=CustomTokenCredential())   
docs_rows   = []
chunks_rows = []
chunk_id=1
for file in files:
    file_path = file.path
    file_name = PurePath(file_path).name
    print(f"Processing: \"{file_name}\"")
    start_time = time.perf_counter()
    
    with fs.open(file_path, "rb") as f:
        md = f.read()
    if isinstance(md, bytes):
        md = md.decode("utf-8", errors="ignore")

    doc_id = existing_doc_ids.get(file_name, str(uuid.uuid4()))
    if file_name in existing_doc_ids:
        print(f"  - Reusing existing doc_id: {doc_id}")
    else:
        print(f"  - Generated new doc_id: {doc_id}")
    
    title = os.path.splitext(file_name)[0]

    page_break_positions = find_page_breaks(md)
    total_pages = get_total_pages_from_breaks(page_break_positions)

    sections = sections_from_markdown(md,page_break_positions)

    doc_chunks = []
    appendix_sections = []
    for s in sections:
        sec_text = s["text"]

        sec_chunks = chunk_within_section(
            sec_text, 
            CHUNK_CHARS, 
            CHUNK_OVERLAP, 
            s["start"], 
            page_break_positions,
            s["type"]  # Pass section type to chunking function
        )
        

        for idx, chunk_info in enumerate(sec_chunks):
            chunk_row = {
                "doc_id": doc_id,
                "file_name": file_name,
                "section_number": s["sec_number"],
                "section_title": s["title"],
                "section_level": s["level"],
                "section_path": s["path"],
                "section_type": s["type"],
                "page_start": s.get("page_start", 1),
                "page_end": s.get("page_end", 1),
                "chunk_id":  chunk_id,
                "chunk_text": chunk_info["text"],
                "chunk_tokens": approx_tokens(chunk_info["text"]),
                "embedding": None
            }
            doc_chunks.append(chunk_row)
            chunk_id+=1

    if DO_EMBED and doc_chunks:
        print(f"  - Generating embeddings for {len(doc_chunks)} chunks...")
        for batch in batched(doc_chunks, EMBED_BATCH):
            embs = embed_batch([c["chunk_text"] for c in batch])
            for c, e in zip(batch, embs):
                c["embedding"] = e
    print(f"\n=== SUMMARY for {file_name} ===")
    appendix_chunks = [c for c in doc_chunks if c["section_type"] == "appendix"]
    regular_chunks = [c for c in doc_chunks if c["section_type"] != "appendix"]
    
    print(f"Total sections: {len(sections)}")
    print(f"Regular chunks: {len(regular_chunks)}")
    print(f"Appendix chunks: {len(appendix_chunks)}")
    chunks_rows.extend(doc_chunks)

    docs_rows.append({
        "doc_id": doc_id,
        "file_name": file_name,
        "title": title,
        "pages": str(total_pages)
    })

    elapsed = time.perf_counter() - start_time
    print(f"  - Processed in {elapsed:.2f} secs, {len(doc_chunks)} chunks created")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType
)
from pyspark.sql import functions as F


for row in chunks_rows:
    v = row.get("embedding")
    if v is not None and not isinstance(v, str):
        row["embedding"] = json.dumps(v, ensure_ascii=False)

docs_schema = StructType([
    StructField("doc_id",       StringType(), True),
    StructField("file_name",    StringType(), True),
    StructField("title",        StringType(), True),
    StructField("pages",        StringType(), True),
])

chunks_schema = StructType([
    StructField("doc_id",         StringType(), True),
    StructField("file_name",      StringType(), True),
    StructField("section_number", StringType(), True),
    StructField("section_title",  StringType(), True),
    StructField("section_level",  IntegerType(), True),
    StructField("section_path",   StringType(), True),
    StructField("section_type",   StringType(), True),
    StructField("page_start",     LongType(), True),
    StructField("page_end",       LongType(), True),
    StructField("chunk_id",       StringType(), True),
    StructField("chunk_text",     StringType(), True),
    StructField("chunk_tokens",   IntegerType(), True),
    StructField("embedding",      StringType(), True),  # JSON text for ADX dynamic
])

docs_df   = spark.createDataFrame(docs_rows,   schema=docs_schema)
chunks_df = spark.createDataFrame(chunks_rows, schema=chunks_schema)
print("Cleaning up existing records...")
for row in docs_df.select("file_name").distinct().toLocalIterator():
    fn = row["file_name"]
    print(f"  - Deleting existing records for: {fn}")
    soft_delete_by_file_name(TABLE_CHUNKS, fn)
    soft_delete_by_file_name(TABLE_DOCS,   fn)


if "embedding" in chunks_df.columns and not isinstance(chunks_df.schema["embedding"].dataType, StringType):
    chunks_df = chunks_df.withColumn("embedding", F.to_json(F.col("embedding")))

# 3) Write to ADX/Eventhouse using Current Identity (best for Fabric)
print("Writing to Kusto...")
accessToken = notebookutils.credentials.getToken('kusto')

(docs_df.write
.format("com.microsoft.kusto.spark.datasource")
.option("kustoCluster",  kustoUri)        
.option("kustoDatabase", kustoDatabase)
.option("kustoTable",    TABLE_DOCS)
.option("accessToken", accessToken )
.mode("Append")
.save())

(chunks_df.write
.format("com.microsoft.kusto.spark.datasource")
.option("kustoCluster",  kustoUri)
.option("kustoDatabase", kustoDatabase)
.option("kustoTable",    TABLE_CHUNKS)
.option("accessToken", accessToken )
.mode("Append")
.save())

print(f"Successfully wrote docs={docs_df.count()} rows, chunks={chunks_df.count()} rows to Eventhouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
