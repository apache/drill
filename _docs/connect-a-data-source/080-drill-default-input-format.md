---
title: "Drill Default Input Format"
date: 2018-12-08
parent: "Storage Plugin Configuration"
---
You can define a default input format to tell Drill what file type exists in a
workspace within a file system. 

Normally, Drill determines the file type based on file
extensions and *magic numbers* when searching a workspace. Magic numbers are file signatures that Drill uses to identify Parquet files. If Drill cannot identify the file type based on file extensions or magic
numbers, the query fails. Defining a default input format can prevent queries
from failing in situations where Drill cannot determine the file type.

If you do not define the default file type in a workspace or incorrectly define the default file type, and Drill cannot
determine the file type without this information, the query fails. You can define one default input format per workspace. You can define a default input format for any of the file types that
Drill supports. Currently, Drill supports the following input types:

  * Avro
  * CSV, TSV, or PSV
  * Parquet
  * JSON
  * Hadoop Sequence Files

You must have a [defined workspace]({{ site.baseurl }}/docs/workspaces) before you can define a default input format.

To define a default input format for a workspace:

  1. Navigate to the [Drill Web UI]({{ site.baseurl }}/docs/plugin-configuration-basics/#using-the-drill-web-console). The Drillbit process must be running on the node before you connect to the Drill Web UI.
  2. Select **Storage** in the toolbar.
  3. Click **Update** next to the storage plugin configuration for which you want to define a default input format for a workspace.
  4. In the Configuration area, locate the workspace, and change the `defaultInputFormat` attribute to any of the supported file types.

## Example of Defining a Default Input Format

```
{
  "type": "file",
  "enabled": true,
  "connection": "hdfs://",
  "workspaces": {
    "root": {
      "location": "/drill/testdata",
      "writable": false,
      "defaultInputFormat": "csv"
  },
  "local" : {
    "location" : "/max/proddata",
    "writable" : true,
    "defaultInputFormat" : "json"
}
```
