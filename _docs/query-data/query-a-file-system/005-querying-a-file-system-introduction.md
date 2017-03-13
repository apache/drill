---
title: "Querying a File System Introduction"
date: 2017-03-13 22:49:09 UTC
parent: "Querying a File System"
---
Files and directories are like standard SQL tables to Drill. You can specify a
file system "database" as a prefix in queries when you refer to objects across
databases. In Drill, a file system database consists of a storage plugin name
followed by an optional workspace name, for example <storage
plugin>.<workspace> or hdfs.logs.

The following example shows a query on a file system database in a Hadoop
distributed file system:

       SELECT * FROM hdfs.logs.`AppServerLogs/20104/Jan/01/part0001.txt`;

The default `dfs` storage plugin configuration registered with Drill has a
`default` workspace. If you query data in the `default` workspace, you do not
need to include the workspace in the query. Refer to
[Workspaces]({{ site.baseurl }}/docs/workspaces) for
more information.

Drill supports the following file types:

  * Plain text files, including:
    * Comma-separated values (CSV, type: text)
    * Tab-separated values (TSV, type: text)
    * Pipe-separated values (PSV, type: text)
  * Structured data files:
    * Avro (type: avro) (This file type is experimental. See [Querying Avro Files]({{site.baseurl}}/docs/querying-avro-files/).)
    * JSON (type: json)
    * Parquet (type: parquet)

The extensions for these file types must match the configuration settings for
your registered storage plugins. For example, PSV files may be defined with a
`.tbl` extension, while CSV files are defined with a `.csv` extension.  

##Implicit Columns  
Drill 1.8 introduces implicit columns. Implicit columns provide file information, such as the directory path to a file and the file extension. You can query implicit columns in files, directories, nested directories, and files. 

The following table lists the implicit columns available and their descriptions:  
  
| Implicit   Column Name | Description                                                                                |
|------------------------|--------------------------------------------------------------------------------------------|
| FQN                    | The   fully qualified name. Contains the full path to the file, including the file   name. |
| FILEPATH               | The   full path to the file, without the file name.                                        |
| FILENAME               | The   file name with the file extension. Does not include the path to the file.            |
| SUFFIX                 | The   file suffix without the dot (.) at the beginning.                                    |  

To access implicit columns, you must explicitly include the columns in a query, as shown in the following example:  

       0: jdbc:drill:zk=local> SELECT fqn, filepath, filename, suffix FROM dfs.`/dev/data/files/test.csvh` LIMIT 1;  
       
       +-------------------------------------+--------------------------+---------------+----------------+
       |             fqn                     |      filepath            |  filename     | suffix         |
       +-------------------------------------+--------------------------+---------------+----------------+
       | /dev/data/files/test.csvh           | /dev/data/files          | test.csvh     | csvh           |
       +-------------------------------------+--------------------------+---------------+----------------+   

{% include startnote.html %}If a table has a column with the same name as an implicit column, such as “suffix,” the implicit column overrides the table column.{% include endnote.html %} 

If a column name has the same name as an implicit column, you can change the default implicit column name using the [ALTER SYSTEM|SESSION SET]({{site.baseurl}}/docs/alter-system/) command with the appropriate parameter, as shown in the following example:  

       ALTER SYSTEM SET `drill.exec.storage.implicit.suffix.column.label` = appendix;  

Use the following configuration options to change the default implicit column names:  

       drill.exec.storage.implicit.fqn.column.label
       drill.exec.storage.implicit.filepath.column.label
       drill.exec.storage.implicit.filename.column.label
       drill.exec.storage.implicit.suffix.column.label
 



