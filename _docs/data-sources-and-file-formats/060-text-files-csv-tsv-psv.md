---
title: "Text Files: CSV, TSV, PSV"
parent: "Data Sources and File Formats"
---

Best practices for reading text files are:

* Select data from particular columns  
* Cast data
* Use a distributed file system  

### Select Data from Particular Columns

Converting text files to another format, such as Parquet, using the CTAS command and a SELECT * statement is not recommended. Instead, select data from particular columns using the [COLUMN[n] syntax]({{site.baseurl}}/docs/querying-plain-text-files), and then assign meaningful column
names using aliases. For example:

    CREATE TABLE parquet_users AS SELECT CAST(COLUMNS[0] AS INT) AS user_id,
    COLUMNS[1] AS username, CAST(COLUMNS[2] AS TIMESTAMP) AS registration_date
    FROM `users.csv1`;

You need to select particular columns instead of using SELECT * for performance reasons. Drill reads CSV, TSV, and PSV files into a list of
VARCHARS, rather than individual columns. While parquet supports and Drill reads lists, as of this release of Drill, the read path for complex data is not optimized. 

### Cast data

You can also improve performance by casting the VARCHAR data to INT, FLOAT, DATETIME, and so on when you read the data from a text file. Drill performs better reading fixed-width than reading VARCHAR data. 

### Use a Distributed File System
Using a distributed file system, such as HDFS, instead of a local file system to query the files also improves performance because currently Drill does not split files on block splits.

## Configuring Drill to Read Text Files
In the storage plugin configuration, you [set the attributes]({{site.baseurl}}/docs/plugin-configuration-basics/#list-of-attributes-and-definitions) that affect how Drill reads CSV, TSV, PSV (comma-, tab-, pipe-separated) files:  

* comment  
* escape  
* deliimiter  
* quote  
* skipFirstLine

Set the `sys.options` property setting `exec.storage.enable_new_text_reader` to true (the default) before attempting to use these attributes. 

## Examples of Querying Text Files
The examples in this section show the results of querying CSV files that use and do not use a header, include comments, and use an escape character:

### Using a Header in a File

![CSV with header]({{ site.baseurl }}/docs/img/csv_with_header.png)

    0: jdbc:drill:zk=local> SELECT * FROM dfs.`/tmp/csv_with_header.csv2`;
    +------------------------+
    |        columns         |
    +------------------------+
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    +------------------------+

### Not Using a Header in a File

![CSV no header]({{ site.baseurl }}/docs/img/csv_no_header.png)

    0: jdbc:drill:zk=local> SELECT * FROM dfs.`/tmp/csv_no_header.csv`;
    +------------------------+
    |        columns         |
    +------------------------+
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    +------------------------+
    7 rows selected (0.112 seconds)

### Escaping a Character in a File

![CSV with escape]({{ site.baseurl }}/docs/img/csv_with_escape.png)

    0: jdbc:drill:zk=local> SELECT * FROM dfs.`/tmp/csv_with_escape.csv`;
    +------------------------------------------------------------------------+
    |                                columns                                 |
    +------------------------------------------------------------------------+
    | ["hello","1","2","3 \" double quote is the default escape character"]  |
    | ["hello","1","2","3"]                                                  |
    | ["hello","1","2","3"]                                                  |
    | ["hello","1","2","3"]                                                  |
    | ["hello","1","2","3"]                                                  |
    | ["hello","1","2","3"]                                                  |
    | ["hello","1","2","3"]                                                  |
    +------------------------------------------------------------------------+
    7 rows selected (0.104 seconds)

### Adding Comments to a File

![CSV with comments]({{ site.baseurl }}/docs/img/csv_with_comments.png)

    0: jdbc:drill:zk=local> SELECT * FROM dfs.`/tmp/csv_with_comments.csv2`;
    +------------------------+
    |        columns         |
    +------------------------+
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    | ["hello","1","2","3"]  |
    +------------------------+
    7 rows selected (0.111 seconds)

## Strategies for Using Attributes
The attributes, such as skipFirstLine, apply to all workspaces defined in a storage plugin. A typical use case defines separate storage plugins for different root directories to query the files stored below the directory. An alternative use case defines multiple formats within the same storage plugin and names target files using different extensions to match the formats.

You can deal with a mix of text files with and without headers either by creating two separate format plugins or by creating two format plugins within the same storage plugin. The former approach is typically easier than the latter.

### Creating Two Separate Storage Plugin Configurations
A storage plugin configuration defines a root directory that Drill targets. You can use a different configuration for each root directory that sets attributes to match the files stored below that directory. All files can use the same extension, such as .csv, as shown in the following example:

Storage Plugin A

    "csv": {
      "type": "text",
      "extensions": [
        "csv"
      ],
      "delimiter": ","
    },
    . . .


Storage Plugin B

    "csv": {
      "type": "text",
      "extensions": [
        "csv"
      ],
      "comment": "&",
      "skipFirstLine": true,
      "delimiter": ","
    },

### Creating One Storage Plugin Configuration to Handle Multiple Formats
You can use a different extension for files with and without a header, and use a storage plugin that looks something like the following example. This method requires renaming some files to use the csv2 extension.

    "csv": {
      "type": "text",
      "extensions": [
        "csv"
      ],
      "delimiter": ","
    },
    "csv_with_header": {
      "type": "text",
      "extensions": [
        "csv2"
      ],
      "comment": "&",
      "skipFirstLine": true,
      "delimiter": ","
    },


