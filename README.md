# DWA - Data Warehouse Automation Framework<BR> (Fabric Edition)
## Welcome
Welcome to the [Prodata](https://www.prodata.ie) Fabric Data warheouse Automation Framework. DWA is a meta data automation framework for data warheousing and data engineering. 
We have been developing and improving this framework for 15 years and now it is available for Open Source for you to try.
<BR>
## Overview
![image](https://github.com/user-attachments/assets/678020fe-ead9-41f9-a77e-597350fa5e45)

The DWA Framework supports the entire enterprise data warheouse lifecycle or other data engineering project by reducing data engineering pipelines and tasks ot meta data and enforcing common 
Enterprise Features (logging, lineage, error handling), orchestration and a very flexible template library

## Gettign Started and Documentation
You can browse this Github repos to use some of the Templates for ideas on your project.<BR>
If you wnat to install the framework and try it out, or look deeper into documentation, examples and demos then 
read the wiki below
<a href ="https://github.com/ProdataSQL/DWA">https://github.com/ProdataSQL/DWA</a>


## Engaging with Prodata for a Quick Start


## Main Features
### 1. Template Driven Data Engineering Library
Common tasks are reduced to re-usaable tempalets which are extensible and can be added withotu any changes to the DWA framework with completely dynamic orchestration and execution.

We create new Templates all the time and some are bespoke to customers ERP systems like SAP, Oracle ERP | PPM | Finnacials, etc

Some Sample Templates are below
* [Ingest-SFTP](https://github.com/ProdataSQL/DWA/blob/main/Library/Ingest/Ingest-SFTP.ipynb). Extract from SFTP into LH.
* [Extract-CSV](https://github.com/ProdataSQL/DWA/blob/main/Library/Extract/Extract-CSV.ipynb). Extract from CSB into LH. Merge, clean headers, archive, dedupe, checksum, etc.
* [Extract-SP-Excel](https://github.com/ProdataSQL/DWA/blob/main/Library/Extract/Extract-SP-Excel.ipynb). Extract from SharePoint directly to LH. Support for Wildcards and multiple worksheets.
* [Extract-O365-API](https://github.com/ProdataSQL/DWA/blob/main/Library/Ops/Extract-O365-API.ipynb). Extract Office 365 logs to create historic log in LH
* [Extract-Dictionary](https://github.com/ProdataSQL/DWA/blob/main/Library/Ops/Extract-Dictionary.ipynb). Extract Data Dictionary of all artefacts, tables and semantic model details.
* [Extract-Fabric-Logs](https://github.com/ProdataSQL/DWA/blob/main/Library/Ops/Extract-Fabric-Logs.ipynb). Extract Refresh Logs. Coming Soon - Log Analytics DAX query logs.
* [Extract-XML](https://github.com/ProdataSQL/DWA/blob/main/Library/Extract/Extract-XML.ipynb). Extract XML and optioinally use XLST to flaten into table.
  
