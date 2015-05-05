---
title: "Hive-to-Drill Data Type Mapping"
parent: "Data Sources and File Formats"
---
Using Drill you can read tables created in Hive that use data types compatible with Drill. Drill currently does not support writing Hive tables. Drill supports the following Hive types for querying:

<!-- See DRILL-1570 -->

| Supported SQL Type | Hive Type | Description                                                |
|--------------------|-----------|------------------------------------------------------------|
| BIGINT             | BIGINT    | 8-byte signed integer                                      |
| BOOLEAN            | BOOLEAN   | TRUE (1) or FALSE (0)                                      |
| CHAR               | CHAR      | Character string, fixed-length max 255                     |
| DATE               | DATE      | Years months and days in the form in the form YYYY-­MM-­DD |
| DECIMAL            | DECIMAL   | 38-digit precision                                         |
| FLOAT              | FLOAT     | 4-byte single precision floating point number              |
| DOUBLE             | DOUBLE    | 8-byte double precision floating point number              |
| INT or INTEGER     | INT       | 4-byte signed integer                                      |
| INTERVALDAY        | N/A       | Integer fields representing a day                          |
| INTERVALYEAR       | N/A       | Integer fields representing a year                         |
| SMALLINT           | SMALLINT  | 2-byte signed integer                                      |
| TIME               | N/A       | Hours minutes seconds 24-hour basis                        |
| N/A                | TIMESTAMP | Conventional UNIX Epoch timestamp.                         |
| TIMESTAMP          | TIMESTAMP | JDBC timestamp in yyyy-mm-dd hh:mm:ss format               |
| None               | STRING    | Binary string (16)                                         |
| VARCHAR            | VARCHAR   | Character string variable length                           |

## Unsupported Types
Drill does not support the following Hive types:

* LIST
* MAP
* STRUCT
* TIMESTAMP (Unix Epoch format)
* UNION

The Hive version used in MapR supports the Hive timestamp in Unix Epoch format. Currently, the Apache Hive version used by Drill does not support this timestamp format. The workaround is to use the JDBC format for the timestamp, which Hive accepts and Drill uses, as shown in the following type mapping example. The timestamp value appears in the example CSV file in JDBC format: 2015-03-25 01:23:15. The Hive table defines column i in the CREATE EXTERNAL TABLE command as a timestamp column. The Drill extract function verifies that Drill interprets the timestamp correctly.

## Type Mapping Example
This example demonstrates the mapping of Hive data types to Drill data types. Using a CSV that has the following contents, you create a Hive table having values of different supported types:

     8223372036854775807,true,3.5,-1231.4,3.14,42,"SomeText",2015-03-25,2015-03-25 01:23:15 

The example assumes that the CSV resides on the MapR file system (MapRFS) in the Drill sandbox: `/mapr/demo.mapr.com/data/`
 
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

You check that Hive mapped the data from the CSV to the typed values as as expected:

    hive> SELECT * FROM types_demo;
    OK
    8223372036854775807	true	3.5	-1231.4	3.14	42	"SomeText"	2015-03-25   2015-03-25 01:23:15
    Time taken: 0.524 seconds, Fetched: 1 row(s)

In Drill, you use the Hive storage plugin that has the following definition.

	{
	  "type": "hive",
	  "enabled": true,
	  "configProps": {
	    "hive.metastore.uris": "thrift://localhost:9083",
	    "hive.metastore.sasl.enabled": "false"
	  }
	}

Using the Hive storage plugin connects Drill to the Hive metastore containing the data.
	
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
