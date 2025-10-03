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
# META       "default_lakehouse_workspace_id": "5941a6c0-8c98-4d79-b065-a3789e9e0960",
# META       "known_lakehouses": [
# META         {
# META           "id": "d58f4f2d-59d7-406d-ae4c-898354a6a75f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Extract-XML
# 
# This notebook transforms XML files using a provided XSLT stylesheet and loads the resulting data into a staging schema in Lakehouse.
# 
# The XSLT logic is embedded via the ActivitySettings parameter, allowing for flexible parsing and formatting of XML structures. It also supports structured ingestion of XML content stored in Lakehouse file paths.

# PARAMETERS CELL ********************

SourceSettings ='{"directory" : "unittest/XML", "file" : "xmlTest.xml"}'
TargetSettings ='{"table" : "xmlTest","schema" : "dbo", "mode":"overwrite" }'
ActivitySettings='''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="text" encoding="UTF-8"/>
  <xsl:template match="/data">
    <xsl:text>"Name","Email","Grade","Age"&#10;</xsl:text>
    <xsl:apply-templates select="student"/>
  </xsl:template>
  <xsl:template match="student">
	<xsl:text>"</xsl:text>
    <xsl:value-of select="@name"/>
    <xsl:text>","</xsl:text>
    <xsl:value-of select="email"/>
    <xsl:text>","</xsl:text>
    <xsl:value-of select="grade"/>
    <xsl:text>","</xsl:text>
    <xsl:value-of select="age"/>
    <xsl:text>"&#10;</xsl:text>
  </xsl:template>
</xsl:stylesheet>
''' #Stores the XSLT stylesheet for the source file
# all of these are optional and set to their default
SourceConnectionSettings = None
TargetConnectionSettings = None
LineageKey = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from lxml import etree
import os
import json
import pandas as pd
from io import StringIO
import sempy.fabric as fabric

# Settings
workspaces = fabric.list_workspaces()

source_connection_settings = json.loads(SourceConnectionSettings or "{}")
source_lakehouse_id = source_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
source_workspace_id = source_connection_settings.get("workspaceId",fabric.get_workspace_id())
source_lakehouse_name = source_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=source_lakehouse_id, workspace=source_workspace_id))
source_workspace_name = workspaces.set_index("Id")["Name"].to_dict().get(source_workspace_id, "Unknown")

target_connection_settings = json.loads(TargetConnectionSettings or '{}')
target_lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
target_workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
target_lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=target_lakehouse_id, workspace=target_workspace_id))
target_workspace_name = workspaces.set_index("Id")["Name"].to_dict().get(target_workspace_id, "Unknown")

TargetSettings = TargetSettings or "{}"
SourceSettings = SourceSettings or "{}"

target_settings = json.loads(TargetSettings or "{}")
source_settings = json.loads(SourceSettings or "{}")

source_directory = source_settings["directory"]
source_file = source_settings["file"]

FILES_PREFIX = "Files"
if not source_directory.startswith(FILES_PREFIX):
    source_directory = os.path.join(FILES_PREFIX, source_directory)

LAKEHOUSE_PREFIX = "/lakehouse/default"
if not source_directory.startswith(LAKEHOUSE_PREFIX):
    source_directory = os.path.join(LAKEHOUSE_PREFIX, source_directory)

target_schema = target_settings.get("schema", "dbo") 
target_table = target_settings.get("table", source_file.split(".")[0])

if target_schema != "dbo":
    target_table = f"{target_schema}_{target_table}"


file_path= os.path.join(source_directory, source_file)

mode = target_settings.get("mode","overwrite")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source_workspace_name==target_workspace_name:
    print(f"Workspace: {source_workspace_name}")
    if source_lakehouse_name==target_lakehouse_name:
        print(f"Lakehouse: {source_lakehouse_name}")
    else:
        print(f"Source Lakehouse: {source_lakehouse_name}")
        print(f"Target Lakehouse: {target_lakehouse_name}")
else:
    print(f"Source Workspace: {source_workspace_name}, Lakehouse: {source_lakehouse_name}")
    print(f"Target Workspace: {target_workspace_name}, Lakehouse: {target_lakehouse_name}")
    
xml_data = etree.parse(file_path)
xslt_stylesheet = etree.XML(ActivitySettings)
parsed_xml = str(etree.XSLT(xslt_stylesheet)(xml_data))

df = pd.read_csv(StringIO(parsed_xml))

df["LineageKey"] = LineageKey
df["File"] = source_file
row_count = df.shape[0]

if mode == "overwrite":
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

spark.createDataFrame(df).write.mode(mode).format("delta").saveAsTable(target_table)

print(f"Wrote {row_count} rows from {file_path} to {target_schema}.{target_table}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
