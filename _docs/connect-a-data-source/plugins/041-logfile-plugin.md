---
title: Logfile Plugin"
date:  2018-07-19 00:42:18 UTC
parent: "Connect a Data Source"
---

Starting in Drill 1.14, you can configure a Logfile plugin that enables Drill to directly read and query log files of any format. For example, you can configure a Logfile plugin to query MySQL log files like the following:  

       070823 21:00:32       1 Connect     root@localhost on test1
       070823 21:00:48       1 Query       show tables
       070823 21:00:56       1 Query       select * from category
       070917 16:29:01      21 Query       select * from location
       070917 16:29:12      21 Query       select * from location where id = 1 LIMIT 1  

To configure the Logfile plugin, you must first add the `drill-logfile-plugin-1.0.0` JAR file to Drill and then add the Logfile configuration to a `dfs` storage plugin, as described in the following sections.  

## Adding `drill-logfile-plugin-1.0.0.jar` to Drill  

You can either [download](https://github.com/cgivre/drill-logfile-plugin/releases/download/v1.0/drill-logfile-plugin-1.0.0.jar) or build the `drill-logfile-plugin-1.0.0` JAR file with Maven, by running the following commands:  

       git clone https://github.com/cgivre/drill-logfile-plugin.git 
       cd drill-logfile-plugin
       mvn clean install -DskipTests 

       //The JAR file builds to targets/.  

Add the JAR file to the `<DRILL_INSTALL>/jars/3rdParty/` directory.  

## Configuring the Logfile Plugin  

To configure the Logfile plugin, update or create a new `dfs` storage plugin instance and then add the Logfile configuration to the `<extensions>` section of the `dfs` storage plugin configuration.  

The following configuration shows a Logfile configuration that you could use if you wanted Drill to query MySQL log files (like the one in the log sample above):   

       "log" : {
             "type" : "log",
             "extensions" : [ "log" ],
             "fieldNames" : [ "date", "time", "pid", "action", "query" ],
             "dataTypes" : [ "DATE", "TIME", "INT", "VARCHAR", "VARCHAR" ],
             "dateFormat" : "yyMMdd",
             "timeFormat" : "HH:mm:ss",
             "pattern" : "(\\d{6})\\s(\\d{2}:\\d{2}:\\d{2})\\s+(\\d+)\\s(\\w+)\\s+(.+)",
             "errorOnMismatch" : false
             }  

Refer to [Storage Plugin Configuration]({{site.baseurl}}/docs/storage-plugin-configuration/) for information about how to configure storage plugins.


### Logfile Configuration Options

* **`pattern`**:  This is the regular expression which defines how the log file lines will be split.  You must enclose the parts of the regex in grouping parentheses that you wish to extract.  Note that this plugin uses Java regular expressions and requires that shortcuts such as `\d` have an additional slash:  ie `\\d`.
* **`fieldNames`**:  This is a list of field names which you are extracting. Note that you must have the same number of fields as extracting groups in your pattern.
* **`dataTypes`**:  This field allows you to define the data types for all the fields extracted from your log.  You may either leave the list blank entirely, in which case all fields will be interpreted as `VARCHAR` or you must define a data tyoe for every field.  At this time, it supports: `INT` or `INTEGER`, `DOUBLE` or `FLOAT8`, `FLOAT` or  `FLOAT4`, `VARCHAR`, `DATE`, `TIME`, and `TIMESTAMP`.
* **`dateFormat`**:   This defines the default date format which will be used to parse dates.  Leave blank if not needed.
* **`timeFormat`**:   This defines the default time format which will be used to parse time.  Leave blank if not needed.
* **`type`**:  This tells Drill which extension to use.  In this case, it must be `log`.
* **`extensions`**:  This option tells Drill which file extensions should be mapped to this configuration.  Note that you can have multiple configurations of this plugin to allow you to query various log files.
* **`errorOnMismatch`**:  False by default, but allows the option of either throwing an error on lines that don't match the pattern or dumping the line to a field called `unmatched_lines` when false.


