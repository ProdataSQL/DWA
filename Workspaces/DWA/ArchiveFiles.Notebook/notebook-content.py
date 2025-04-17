# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "19785e4d-5572-4ced-bfab-f26e7c5de3ce",
# META       "default_lakehouse_name": "FabricLH",
# META       "default_lakehouse_workspace_id": "9b8a6500-5ccb-49a9-885b-b5b081efed75",
# META       "known_lakehouses": [
# META         {
# META           "id": "19785e4d-5572-4ced-bfab-f26e7c5de3ce"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Archivefiles
# 
# This notebook connects to a SQL Server database, retrieves file details via a stored procedure, and moves files from a source directory to an archive directory within a lakehouse structure. It handles file operations, error checking, and updates the database to mark the files as archived. 

# PARAMETERS CELL ********************

Server = 'fkm4vwf6l6zebg4lqrhbtdcmsq-absyvg6llsuutcc3wwyid37nou.datawarehouse.pbidedicated.windows.net'
Database = 'FabricDW'
PackageGroup='AW'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyodbc
import struct
import os
import shutil

LAKEHOUSE_PREFIX = "/lakehouse/default/"
# Obtain an access token
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
token_bytes = token.encode("UTF-16-LE")
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
SQL_COPT_SS_ACCESS_TOKEN = 1256

# Create the connection string with the access token
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={Server},1433;Database={Database};Encrypt=Yes;TrustServerCertificate=Yes"
connection = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
cursor = connection.cursor()

# Execute the stored procedure to fetch the required columns
query = f"EXEC [dwa].[usp_ArchiveList] '{PackageGroup}'"
cursor.execute(query)
records = cursor.fetchall()

# Loop through the records and copy files
for record in records:
    source_directory = record.SourceDirectory
    archive_directory = record.ArchiveDirectory
    filename = record.Filename.split('?')[0]
    pipelineid = record.PipelineID

    print(filename)
    # Ensure that SourceDirectory starts with "Files/"
    if not source_directory.startswith("Files/"):
        source_directory = f"Files/{source_directory}"

    # set the SourceDirectory 
    if not archive_directory.startswith(LAKEHOUSE_PREFIX):
        archive_directory = os.path.join(LAKEHOUSE_PREFIX, archive_directory)
    if not source_directory.startswith(LAKEHOUSE_PREFIX):
        source_directory = os.path.join(LAKEHOUSE_PREFIX, source_directory)

    # Construct full source and destination paths
    source_path = f"{source_directory}/{filename}"
    destination_path = f"{archive_directory}/{filename}"

    dequeue = f"EXEC [dwa].[usp_ArchiveDelete] @PipelineID='{pipelineid}',@Filename = '{filename}'"

    try:
        # Copy the file from source to archive       
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        shutil.move(source_path, destination_path)        
        print(f"Copied '{filename}' from '{source_directory}' to '{archive_directory}'")  

        #De-Queue 
        try:
            cursor.execute(dequeue)
            connection.commit() 
        except Exception as e:
            print(f"Error executing '{dequeue}': {str(e)}")          
     
    except FileNotFoundError:
        print(f"File '{filename}' not found in '{source_path}'")
        raise FileNotFound(f"File '{filename}' not found in '{source_path}'")

# Close the database connection
connection.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
