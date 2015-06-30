---
title: "Text Files: CSV, TSV, PSV"
parent: "Data Sources and File Formats"
---
The section ["Plugin Configuration Basics"]({{site.baseurl}}/docs/plugin-configuration-basics) covers attributes that you configure for use with a CSV, TSV, PSV (comma-, tab-, pipe-separated) values text file. This section presents examples of how to use those attributes and [tips for performant querying]({{site.baseurl}}/docs/text-files-csv-tsv-psv/#tips-for-performant-querying) of these text files. 

## Managing Headers in Text Files
In the storage plugin configuration, you set attributes on the text reader format configuration. This section presents examples of using the following attributes defined in ["List of Attributes and Definitions"]({{site.baseurl}}/docs/plugin-configuration-basics/#list-of-attributes-and-definitions):

* String lineDelimiter = "\n";  
  One or more characters used to denote a new record. Allows reading files with windows line endings.  
* char fieldDelimiter = ',';  
  A single character used to separate each value.  
* char quote = '"';  
  A single character used to start/end a quoted value.  
* char escape = '"';  
  A single character used to escape a quote inside of a value.  
* char comment = '#';  
  A single character used to denote a comment line.  
* boolean skipFirstLine = false;  
  Set to true to avoid reading headers as data.  

You can deal with a mix of text files with and without headers either by creating two separate format plugins or by creating two format plugins within the same storage plugin. The former approach is typically easier than the latter.

### Creating Two Separate Format Plugins
Format plugins are associated with a particular storage plugin. Storage plugins define a root directory that Drill targets when using the storage plugin. You can define separate storage plugins for different root directories, and define each of the format attributes to match the files stored below that directory. All files can use the .csv extension.

For example:

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

### Creating Two Format Plugins within the Same Storage Plugin
Give a different extension to files with a header and to files without a header, and use a storage plugin that looks something like the following example. This method requires renaming some files to use the csv2 extension.

For example:

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

## Tips for Performant Querying

Converting these files to another format, such as Parquet, using the CTAS command and a SELECT * statement is not recommended. Drill reads CSV, TSV, and PSV files into a list of
VARCHARS, rather than individual columns. While parquet supports lists and
Drill reads them, the read path for complex data is not yet optimized. Select data from particular columns using the [COLUMN[n] syntax]({{site.baseurl}}/docs/querying-plain-text-files), and then assign meaningful column
names using aliases. For example:

CREATE TABLE parquet_users AS SELECT CAST(COLUMNS[0] AS INT) AS user_id,
COLUMNS[1] AS username, CAST(COLUMNS[2] AS TIMESTAMP) AS registration_date
FROM `users.csv1`;

Cast the VARCHAR data to INT, FLOAT, DATETIME, and so on. You get better performance reading fixed-width than reading VARCHAR data. 

Using a distributed file system, such as HDFS, instead of a local file system to query the files also improves performance because currently Drill does not split files on block splits.