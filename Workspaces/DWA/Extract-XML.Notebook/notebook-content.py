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

# #### Extract-XML
# 
# This notebook transforms XML files using a provided XSLT stylesheet and loads the resulting data into a staging schema in Lakehouse.
# 
# The XSLT logic is embedded via the ActivitySettings parameter, allowing for flexible parsing and formatting of XML structures. It also supports structured ingestion of XML content stored in Lakehouse file paths.

# PARAMETERS CELL ********************

SourceSettings ='{"Directory" : "unittest/XML", "File" : "xmlTest.xml"}'
TargetSettings ='{"TableName" : "xmlTest","SchemaName" : "dbo", "mode":"overwrite" }'
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

TargetSettings = TargetSettings or "{}"
SourceSettings = SourceSettings or "{}"

target_settings = json.loads(TargetSettings)
source_settings = json.loads(SourceSettings)

source_directory = source_settings["Directory"]
source_file = source_settings["File"]

FILES_PREFIX = "Files"
if not source_directory.startswith(FILES_PREFIX):
    source_directory = os.path.join(FILES_PREFIX, source_directory)

LAKEHOUSE_PREFIX = "/lakehouse/default"
if not source_directory.startswith(LAKEHOUSE_PREFIX):
    source_directory = os.path.join(LAKEHOUSE_PREFIX, source_directory)

target_schema = target_settings.get("SchemaName", "dbo") 
target_table = target_settings.get("TableName", source_file.split(".")[0])

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

print(f"Wrote {row_count} rows from {file_path} to FabricLH.dbo.{target_table}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
