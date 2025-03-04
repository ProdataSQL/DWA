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
'''
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
