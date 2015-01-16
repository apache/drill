---
title: "Querying a File System"
parent: "Query Data"
---
Files and directories are like standard SQL tables to Drill. You can specify a
file system "database" as a prefix in queries when you refer to objects across
databases. In Drill, a file system database consists of a storage plugin name
followed by an optional workspace name, for example <storage
plugin>.<workspace> or hdfs.logs.

The following example shows a query on a file system database in a Hadoop
distributed file system:

       SELECT * FROM hdfs.logs.`AppServerLogs/20104/Jan/01/part0001.txt`;

The default `dfs` storage plugin instance registered with Drill has a
`default` workspace. If you query data in the `default` workspace, you do not
need to include the workspace in the query. Refer to
[Workspaces](/drill/docs/workspaces) for
more information.

Drill supports the following file types:

  * Plain text files, including:
    * Comma-separated values (CSV, type: text)
    * Tab-separated values (TSV, type: text)
    * Pipe-separated values (PSV, type: text)
  * Structured data files:
    * JSON (type: json)
    * Parquet (type: parquet)

The extensions for these file types must match the configuration settings for
your registered storage plugins. For example, PSV files may be defined with a
`.tbl` extension, while CSV files are defined with a `.csv` extension.

