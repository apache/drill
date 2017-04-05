---
title: "Hive-to-Drill Data Type Mapping"
date: 2017-04-05 00:09:55 UTC
parent: "Data Sources and File Formats"
---
Using Drill you can read tables created in Hive that use data types compatible with Drill. Drill currently does not support writing Hive tables. The map of SQL types and Hive types shows that several Hive types need to be cast to the supported SQL type in a Drill query:
 
* TINYINT and SMALLINT  
   Cast these types to INTEGER.  
* BINARY  
  Cast the Hive BINARY type to VARCHAR.  

{% include startnote.html %}In the 1.7 release, Drill automatically converts the Hive CHAR data type to VARCHAR. You no longer need to cast the Hive CHAR data type to VARCHAR when querying Hive tables.{% include endnote.html %}


## Map of SQL and Hive Types
<!-- See DRILL-1570 -->

| Supported SQL Type | Hive Type               | Description                                                |
|--------------------|-------------------------|------------------------------------------------------------|
| BIGINT             | BIGINT                  | 8-byte signed integer                                      |
| BOOLEAN            | BOOLEAN                 | TRUE (1) or FALSE (0)                                      |
| VARCHAR            | CHAR                    | Character string, fixed-length max 255                     |
| DATE               | DATE                    | Years months and days in the form in the form YYYY-­MM-­DD   |
| DECIMAL*           | DECIMAL                 | 38-digit precision                                         |
| FLOAT              | FLOAT                   | 4-byte single precision floating point number              |
| DOUBLE             | DOUBLE                  | 8-byte double precision floating point number              |
| INTEGER            | INT, TINYINT, SMALLINT  | 1-, 2-, or 4-byte signed integer                           |
| INTERVAL           | N/A                     | A day-time or year-month interval                          |
| TIME               | N/A                     | Hours minutes seconds 24-hour basis                        |
| N/A                | TIMESTAMP               | Conventional UNIX Epoch timestamp.                         |
| TIMESTAMP          | TIMESTAMP               | JDBC timestamp in yyyy-mm-dd hh:mm:ss format               |
| None               | STRING                  | Binary string (16)                                         |
| VARCHAR            | VARCHAR                 | Character string variable length                           |
| VARBINARY          | BINARY                  | Binary string                                              |

\* In this release, Drill disables the DECIMAL data type, including casting to DECIMAL and reading DECIMAL types from Parquet and Hive. To enable the DECIMAL type, set the `planner.enable_decimal_data_type` option to `true`.

## Unsupported Types
Drill does not support the following Hive types:

* LIST
* MAP
* STRUCT
* TIMESTAMP (Unix Epoch format)
* UNION

Currently, the Apache Hive version used by Drill does not support the Hive timestamp in Unix Epoch format. The workaround is to use the JDBC format for the timestamp, which Hive accepts and Drill uses. The type mapping example shows how to use the workaround as follows. 

* The timestamp value appears in the example CSV file in JDBC format: 2015-03-25 01:23:15.  
* Workaround: The Hive table defines column i in the CREATE EXTERNAL TABLE command as a timestamp column.  
* The Drill extract function verifies that Drill interprets the timestamp correctly.

## Type Mapping Example
This example demonstrates the mapping of Hive data types to Drill data types. Using a CSV that has the following contents, you create a Hive table having values of different supported types:

     8223372036854775807,true,3.5,-1231.4,3.14,42,"SomeText",2015-03-25,2015-03-25 01:23:15 

### Example Assumptions
The example makes the following assumptions:

* The CSV resides in the following location in the Drill sandbox: `/mapr/demo.mapr.com/data/`  
* You [enabled the DECIMAL data type]({{site.baseurl}}/docs/supported-data-types/#enabling-the-decimal-type) in Drill.  

### Define an External Table in Hive

In Hive, you define an external table using the following query:

    hive> CREATE EXTERNAL TABLE types_demo ( 
          a bigint, 
          b boolean, 
          c DECIMAL(3, 2), 
          d double, 
          e float, 
          f INT, 
          g VARCHAR(64), 
          h date,
          i timestamp
          ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
          LINES TERMINATED BY '\n' 
          STORED AS TEXTFILE LOCATION '/mapr/demo.mapr.com/data/mytypes.csv';

\* In this release, Drill disables the DECIMAL data type, including casting to DECIMAL and reading DECIMAL types from Parquet and Hive. To enable the DECIMAL type, set the `planner.enable_decimal_data_type` option to `true`.

You check that Hive mapped the data from the CSV to the typed values as as expected:

    hive> SELECT * FROM types_demo;
    OK
    8223372036854775807	true	3.5	-1231.4	3.14	42	"SomeText"	2015-03-25   2015-03-25 01:23:15
    Time taken: 0.524 seconds, Fetched: 1 row(s)

### Connect Drill to Hive and Query the Data  

{% include startnote.html %}Drill 1.8 implements the IF EXISTS parameter for the DROP TABLE and DROP VIEW commands, making IF a reserved word in Drill. As a result, you must include backticks around the Hive \``IF`` conditional function when you use it in a query on Hive tables. Alternatively, you can use the CASE statement instead of the IF function.{% include endnote.html %}

In Drill, you use the [Hive storage plugin]({{site.baseurl}}/docs/hive-storage-plugin). Using the Hive storage plugin connects Drill to the Hive metastore containing the data.
	
	0: jdbc:drill:> USE hive;
	+------------+------------+
	|     ok     |  summary   |
	+------------+------------+
	| true       | Default schema changed to 'hive' |
	+------------+------------+
	1 row selected (0.067 seconds)
	
The data in the Hive table shows the expected values.
	
	0: jdbc:drill:> SELECT * FROM hive.`types_demo`;
	+---------------------+------+------+---------+------+----+------------+------------+-----------+
	|   a                 |   b  |  c   |     d   |  e   | f  |     g      |     h      |     i     |
	+---------------------+---------+---------+------+----+------------+------------+-----------+
	| 8223372036854775807 | true | 3.50 | -1231.4 | 3.14 | 42 | "SomeText" | 2015-03-25 | 2015-03-25 01:23:15.0 |
	+---------------------+------+------+---------+------+----+------------+------------+-----------+
	1 row selected (1.262 seconds)
	
To validate that Drill interprets the timestamp in column i correctly, use the extract function to extract part of the date:

    0: jdbc:drill:> select extract(year from i) from hive.`types_demo`;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015       |
    +------------+
    1 row selected (0.387 seconds)
