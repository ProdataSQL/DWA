# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Export-Excel
# 
# This notebook extracts data from a SQL source such as a table, view, or stored procedure and exports the result to an Excel file and saves it to a specified directory in the Fabric Lakehouse. The script supports dynamic query generation and customizable Excel export options defined in the target settings.

# PARAMETERS CELL ********************

TargetDirectory = "Export/excel"
TargetFileName= "PipelineStart.xlsx"
SourceObject="EXEC dwa.usp_PipelineStart @PipelineID=36"
SourceSettings ='{}'
TargetSettings ='{"header":"False"}'
SourceConnectionSettings='{"ConnString":"Driver={ODBC Driver 18 for SQL Server};Server=fkm4vwf6l6zebg4lqrhbtdcmsq-absyvg6llsuutcc3wwyid37nou.datawarehouse.pbidedicated.windows.net,1433;Database=FabricDW;Encrypt=Yes;TrustServerCertificate=Yes"}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import struct
import json
import pyodbc
import pandas as pd

TargetSettings = TargetSettings or '{}'
SourceSettings = SourceSettings or '{}'
SourceConnectionSettings = SourceConnectionSettings or '{}'

token = mssparkutils.credentials.getToken('https://analysis.windows.net/powerbi/api')
token_bytes = token.encode("UTF-16-LE")
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
SQL_COPT_SS_ACCESS_TOKEN = 1256

LAKEHOUSE_PREFIX = "/lakehouse/default/Files"
 #set the target directory
if not TargetDirectory.startswith(LAKEHOUSE_PREFIX):
    TargetDirectory = os.path.join(LAKEHOUSE_PREFIX, TargetDirectory)

file_path= os.path.join(TargetDirectory, TargetFileName)

source_connection = json.loads(SourceConnectionSettings)
reader_options = json.loads(SourceSettings)
target_settings_obj= json.loads(TargetSettings)

if len(SourceObject.split(" ")) == 1:
    if SourceObject.upper().startswith(("USP_", "SP_")):
        SourceObject = f"EXEC {SourceObject}"
    else:
        SourceObject = f"SELECT * FROM  {SourceObject}"

# Create the connection string with the access token
connection_string = source_connection["ConnString"]
connection = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

sql_query = pd.read_sql_query (SourceObject,connection)
df = pd.DataFrame(sql_query)

if not os.path.exists (TargetDirectory):
    os.makedirs(TargetDirectory) 

df.to_excel(file_path,**target_settings_obj)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
