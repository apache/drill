---
title: "Drill Default Input Format"
parent: "Storage Plugin Configuration"
---
You can define a default input format to tell Drill what file type exists in a
workspace within a file system. Drill determines the file type based on file
extensions and magic numbers when searching a workspace.

Magic numbers are file signatures that Drill uses to identify Parquet files.
If Drill cannot identify the file type based on file extensions or magic
numbers, the query fails. Defining a default input format can prevent queries
from failing in situations where Drill cannot determine the file type.

If you incorrectly define the file type in a workspace and Drill cannot
determine the file type, the query fails. For example, if JSON files do not have a `.json` extension, the query fails.

You can define one default input format per workspace. If you do not define a
default input format, and Drill cannot detect the file format, the query
fails. You can define a default input format for any of the file types that
Drill supports. Currently, Drill supports the following types:

  * Avro
  * CSV, TSV, or PSV
  * Parquet
  * JSON
  * MapR-DB*

\* Only available when you install Drill on a cluster using the mapr-drill package.

## Defining a Default Input Format

You define the default input format for a file system workspace through the
Drill Web UI. You must have a [defined workspace]({{ site.baseurl }}/docs/workspaces) before you can define a
default input format.

To define a default input format for a workspace, complete the following
steps:

  1. Navigate to the Drill Web UI at `<drill_node_ip_address>:8047`. The Drillbit process must be running on the node before you connect to the Drill Web UI.
  2. Select **Storage** in the toolbar.
  3. Click **Update** next to the file system for which you want to define a default input format for a workspace.
  4. In the Configuration area, locate the workspace for which you would like to define the default input format, and change the `defaultInputFormat` attribute to any of the supported file types.

     **Example**
     
        {
          "type": "file",
          "enabled": true,
          "connection": "hdfs://",
          "workspaces": {
            "root": {
              "location": "/drill/testdata",
              "writable": false,
              "defaultInputFormat": csv
          },
          "local" : {
            "location" : "/max/proddata",
            "writable" : true,
            "defaultInputFormat" : "json"
        }

## Querying Compressed Files

You can query compressed GZ files, such as JSON and CSV, as well as uncompressed files. The file extension specified in the `formats . . . extensions` property of the storage plugin must precede the gz extension in the file name. For example, `proddata.json.gz` or `mydata.csv.gz` are valid file names to use in a query, as shown in the example in ["Querying the GZ File Directly"]({{site.baseurl"}}/docs/querying-plain-text-files/#query-the-gz-file-directly).
