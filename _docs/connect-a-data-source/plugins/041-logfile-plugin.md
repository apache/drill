---
title: "Logfile Plugin"
date: 2018-11-14
parent: "Connect a Data Source"
---

# Drill Regex/Logfile Plugin
Starting in Drill 1.14, the Regex/Logfile Plugin for Apache Drill allows Drill to read and query arbitrary files where the schema can be defined by a regex.  The original intent was for this to be used for log files, however, it can be used for any structured data.

## Example Use Case:  MySQL Log
If you wanted to analyze log files such as the MySQL log sample shown below using Drill, it may be possible using various string fucntions, or you could write a UDF specific to this data however, this is time consuming, difficult and not reusable.

```
070823 21:00:32       1 Connect     root@localhost on test1
070823 21:00:48       1 Query       show tables
070823 21:00:56       1 Query       select * from category
070917 16:29:01      21 Query       select * from location
070917 16:29:12      21 Query       select * from location where id = 1 LIMIT 1
```
This plugin will allow you to configure Drill to directly query logfiles of any configuration.

## Configuration Options
* **`type`**:  This tells Drill which extension to use.  In this case, it must be `logRegex`.  This field is mandatory.
* **`regex`**:  This is the regular expression which defines how the log file lines will be split.  You must enclose the parts of the regex in grouping parentheses that you wish to extract.  Note that this plugin uses Java regular expressions and requires that shortcuts such as `\d` have an additional slash:  ie `\\d`.  This field is mandatory.
* **`extension`**:  This option tells Drill which file extensions should be mapped to this configuration.  Note that you can have multiple configurations of this plugin to allow you to query various log files.  This field is mandatory.
* **`maxErrors`**:  Log files can be inconsistent and messy.  The `maxErrors` variable allows you to set how many errors the reader will ignore before halting execution and throwing an error.  Defaults to 10.
* **`schema`**:  The `schema` field is where you define the structure of the log file.  This section is optional.  If you do not define a schema, all fields will be assigned a column name of `field_n` where `n` is the index of the field. The undefined fields will be assigned a default data type of `VARCHAR`.

### Defining a Schema
The schema variable is an JSON array of fields which have at the moment, three possible variables:
* **`fieldName`**:  This is the name of the field.
* **`fieldType`**:  Defines the data type.  Defaults to `VARCHAR` if undefined. At the time of writing, the reader supports: `VARCHAR`, `INT`, `SMALLINT`, `BIGINT`, `FLOAT4`, `FLOAT8`, `DATE`, `TIMESTAMP`, `TIME`.
* **`format`**: Defines the for date/time fields.  This is mandatory if the field is a date/time field.

In the future, it is my hope that the schema section will allow for data masking, validation and other transformations that are commonly used for analysis of log files.

### Example Configuration:
The configuration below demonstrates how to configure Drill to query the example MySQL log file shown above.


```
"log" : {
      "type" : "logRegex",
      "extension" : "log",
      "regex" : "(\\d{6})\\s(\\d{2}:\\d{2}:\\d{2})\\s+(\\d+)\\s(\\w+)\\s+(.+)",
      "maxErrors": 10,
      "schema": [
        {
          "fieldName": "eventDate",
          "fieldType": "DATE",
          "format": "yyMMdd"
        },
        {
          "fieldName": "eventTime",
          "fieldType": "TIME",
          "format": "HH:mm:ss"
        },
        {
          "fieldName": "PID",
          "fieldType": "INT"
        },
        {
          "fieldName": "action"
        },
        {
          "fieldName": "query"
        }
      ]
   }
 ```


## Example Usage

This format plugin gives you two options for querieng fields.  If you define the fields, you can query them as you would any other data source.  If you do nof define a field in the column `schema` variable, Drill will extract all fields and give them the name `field_n`.  The fields are indexed from `0`.  Therefore if you have a dataset with 5 fields the following query would be valid:

```
SELECT field_0, field_1, field_2, field_3, field_4
FROM ..
```

### Implicit Fields
In addition to the fields which the user defines, the format plugin has two implicit fields whcih can be useful for debugging your regex.  These fields do not appear in `SELECT *` queries and only will be retrieved when included in a query.

* **`_raw`**:  This field returns the complete lines which matched your regex.
* **`_unmatched_rows`**:  This field returns rows which **did not** match the regex.  Note: This field ONLY returns the unmatching rows, so if you have a data file of 10 lines, 8 of which match, `SELECT _unmatched_rows` will return 2 rows.  If however, you combine this with another field, such as `_raw`, the `_unmatched_rows` will be `null` when the rows match and have a value when it does not.





