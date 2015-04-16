---
title: "Parquet Format"
parent: "Data Sources and File Formats"
---
## Parquet Format
[Apache Parquet](http://parquet.incubator.apache.org/documentation/latest) has the following characteristics:

* Self-describing
* Columnar format
* Language-independent 

Self-describing data embeds the schema or structure with the data itself. Hadoop use cases drive the growth of self-describing data formats, such as Parquet and JSON, and of NoSQL databases, such as HBase. These formats and databases are well suited for the agile and iterative development cycle of Hadoop applications and BI/analytics. Optimized for working with large files, Parquet arranges data in columns, putting related values in close proximity to each other to optimize query performance, minimize I/O, and facilitate compression. Parquet detects and encodes the same or similar data using a technique that conserves resources.

Apache Drill includes the following support for Parquet:

* Querying self-describing data in files or NoSQL databases without having to define and manage schema overlay definitions in centralized metastores
* Creating Parquet files from other file formats, such as JSON, without any set up
* Generating Parquet files that have evolving or changing schemas and querying the data on the fly
* Handling Parquet data types

### Reading Parquet Files
When a read of Parquet data occurs, Drill loads only the necessary columns of data, which reduces I/O. Reading only a small piece of the Parquet data from a data file or table, Drill can examine and analyze all values for a column across multiple files. You can create a Drill table from one format and store the data in another format, including Parquet.

### Writing Parquet Files
CREATE TABLE AS SELECT (CTAS) can use any data source provided by the storage plugin. To write Parquet data using the CTAS command, set the session store.format option as shown in the next section. Alternatively, configure the storage plugin to point to the directory containing the Parquet files.

Although the data resides in a single table, Parquet output generally consists of multiple files that resemble MapReduce output having numbered file names,  such as 0_0_0.parquet in a directory.

### Configuring the Parquet Storage Format
To read or write Parquet data, you need to include the Parquet format in the storage plugin format definitions. The `dfs` plugin definition includes the Parquet format. 

Use the `store.format` option to set the CTAS output format of a Parquet row group at the session or system level.

Use the ALTER command to set the `store.format` option.
         
        ALTER SESSION SET `store.format` = 'parquet';
        ALTER SYSTEM SET `store.format` = 'parquet';
        
### Configuring the Size of Parquet Files
Configuring the size of Parquet files by setting the `store.parquet.block-size` can improve write performance. The block size is the size of MFS, HDFS, or the file system. 

The larger the block size, the more memory Drill needs for buffering data. Parquet files that contain a single block maximize the amount of data Drill stores contiguously on disk. Given a single row group per file, Drill stores the entire Parquet file onto the block, avoiding network I/O.

To maximize performance, set the target size of a Parquet row group to the number of bytes less than or equal to the block size of MFS, HDFS, or the file system by using the `store.parquet.block-size`:         
        
        ALTER SESSION SET `store.parquet.block-size` = 536870912;         
        ALTER SYSTEM SET `store.parquet.block-size` = 536870912  

The default block size is 536870912 bytes.

### Type Mapping
The high correlation between Parquet and SQL data types makes reading Parquet files effortless in Drill. Writing to Parquet files takes more work than reading. Because SQL does not support all Parquet data types, to prevent Drill from inferring a type other than one you want, use the [cast function] ({{ site.baseurl }}/docs/sql-functions) Drill offers more liberal casting capabilities than SQL for Parquet conversions if the Parquet data is of a logical type. 

The following general process converts a file from JSON to Parquet:

* Create or use an existing storage plugin that specifies the storage location of the Parquet file, mutability of the data, and supported file formats.
* Take a look at the JSON data. 
* Create a table that selects the JSON file.
* In the CTAS command, cast JSON string data to corresponding [SQL types]({{ site.baseurl }}/docs/json-data-model/data-type-mapping).

### Example: Read JSON, Write Parquet
This example demonstrates a storage plugin definition, a sample row of data from a JSON file, and a Drill query that writes the JSON input to Parquet output. 

#### Storage Plugin Definition
You can use the default dfs storage plugin installed with Drill for reading and writing Parquet files. The storage plugin needs to configure the writable option of the workspace to true, so Drill can write the Parquet output. The dfs storage plugin defines the tmp writable workspace, which you can use in the CTAS command to create a Parquet table.

#### Sample Row of JSON Data
A JSON file called sample.json contains data consisting of strings, typical of JSON data. The following example shows one row of the JSON file:

        {"trans_id":0,"date":"2013-07-26","time":"04:56:59","amount":80.5,"user_info":
          {"cust_id":28,"device":"WEARABLE2","state":"mt"
          },"marketing_info":
            {"camp_id":4,"keywords": ["go","to","thing","watch","made","laughing","might","pay","in","your","hold"]
            },
            "trans_info":
              {"prod_id":[16],
               "purch_flag":"false"
              }
        }
              

#### CTAS Query      
The following example shows a CTAS query that creates a table from JSON data shown in the last example. The command casts the date, time, and amount strings to SQL types DATE, TIME, and DOUBLE. String-to-VARCHAR casting of the other strings occurs automatically.

    CREATE TABLE dfs.tmp.sampleparquet AS 
    (SELECT trans_id, 
    cast(`date` AS date) transdate, 
    cast(`time` AS time) transtime, 
    cast(amount AS double) amountm,
    user_info, marketing_info, trans_info 
    FROM dfs.`/Users/drilluser/sample.json`);
        
The CTAS query does not specify a file name extension for the output. Drill creates a parquet file by default, as indicated by the file name in the output:

    +------------+---------------------------+
    |  Fragment  | Number of records written |
    +------------+---------------------------+
    | 0_0        | 5                         |
    +------------+---------------------------+
    1 row selected (1.369 seconds)

You can query the Parquet file to verify that Drill now interprets the converted string as a date.

    SELECT extract(year from transdate) AS `Year`, t.user_info.cust_id AS Customer 
    FROM dfs.tmp.`sampleparquet` t;

    +------------+------------+
    |    Year    |  Customer  |
    +------------+------------+
    | 2013       | 28         |
    | 2013       | 86623      |
    | 2013       | 11         |
    | 2013       | 666        |
    | 2013       | 999        |
    +------------+------------+
    5 rows selected (0.039 seconds)

For more examples of and information about using Parquet data, see ["Evolving Parquet as self-describing data format – New paradigms for consumerization of Hadoop data"](https://www.mapr.com/blog/evolving-parquet-self-describing-data-format-new-paradigms-consumerization-hadoop-data#.VNeqQbDF_8f).

#### SQL Data Types to Parquet
The first table in this section maps SQL data types to Parquet data types, limited intentionally by Parquet creators to minimize the impact on disk storage:

<table>
  <tr>
    <th>SQL Type</th>
    <th>Parquet Type</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>BIGINT</td>
    <td>INT64</td>
    <td>8-byte signed integer</td>
  </tr>
  <tr>
    <td>BOOLEAN</td>
    <td>BOOLEAN</td>
    <td>TRUE (1) or FALSE (0)</td>
  </tr>
  <tr>
    <td>N/A</td>
    <td>BYTE_ARRAY</td>
    <td>Arbitrarily long byte array</td>
  </tr>
  <tr>
    <td>FLOAT</td>
    <td>FLOAT</td>
    <td>4-byte single precision floating point number</td>
  </tr>
  <tr>
    <td>DOUBLE</td>
    <td>DOUBLE</td>
    <td>8-byte double precision floating point number</td>
  </tr>
  <tr>
    <td>INTEGER</td>
    <td>INT32</td>
    <td>4-byte signed integer</td>
  </tr>
  <tr>
    <td>None</td>
    <td>INT96</td>
    <td>12-byte signed int</td>
  </tr>
</table>

#### SQL Types to Parquet Logical Types
Parquet also supports logical types, fully described on the [Apache Parquet site](https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md). Embedded types, JSON and BSON, annotate a binary primitive type representing a JSON or BSON document. The logical types and their mapping to SQL types are:
 
<table>
  <tr>
    <th>SQL Type</th>
    <th>Drill Description</th>
    <th>Parquet Logical Type</th>
    <th>Parquet Description</th>
  </tr>
  <tr>
    <td>DATE</td>
    <td>Years months and days in the form in the form YYYY-­MM-­DD</td>
    <td>DATE</td>
    <td>Date, not including time of day. Uses the int32 annotation. Stores the number of days from the Unix epoch, 1 January 1970.</td>
  </tr>
  <tr>
    <td>VARCHAR</td>
    <td>Character string variable length</td>
    <td>UTF8 (Strings)</td>
    <td>Annotates the binary primitive type. The byte array is interpreted as a UTF-8 encoded character string.</td>
  </tr>
  <tr>
    <td>None</td>
    <td></td>
    <td>INT_8</td>
    <td>8 bits, signed</td>
  </tr>
  <tr>
    <td>None</td>
    <td></td>
    <td>INT_16</td>
    <td>16 bits, usigned</td>
  </tr>
  <tr>
    <td>INT</td>
    <td>4-byte signed integer</td>
    <td>INT_32</td>
    <td>32 bits, signed</td>
  </tr>
  <tr>
    <td>DOUBLE</td>
    <td>8-byte double precision floating point number</td>
    <td>INT_64</td>
    <td>64 bits, signed</td>
  </tr>
  <tr>
    <td>None</td>
    <td></td>
    <td>UINT_8</td>
    <td>8 bits, unsigned</td>
  </tr>
  <tr>
    <td>None</td>
    <td></td>
    <td>UINT_16</td>
    <td>16 bits, unsigned</td>
  </tr>
  <tr>
    <td>None</td>
    <td></td>
    <td>UINT_32</td>
    <td>32 bits, unsigned</td>
  </tr>
  <tr>
    <td>None</td>
    <td></td>
    <td>UINT_64</td>
    <td>64 bits, unsigned</td>
  </tr>
  <tr>
    <td>DECIMAL</td>
    <td>38-digit precision</td>
    <td>DECIMAL</td>
    <td>Arbitrary-precision signed decimal numbers of the form unscaledValue * 10^(-scale)</td>
  </tr>
  <tr>
    <td>TIME</td>
    <td>Hours, minutes, seconds, milliseconds; 24-hour basis</td>
    <td>TIME_MILLIS</td>
    <td>Logical time, not including the date. Annotates int32. Number of milliseconds after midnight.</td>
  </tr>
  <tr>
    <td>TIMESTAMP</td>
    <td>Year, month, day, and seconds</td>
    <td>TIMESTAMP_MILLIS</td>
    <td>Logical date and time. Annotates an int64 that stores the number of milliseconds from the Unix epoch, 00:00:00.000 on 1 January 1970, UTC.</td>
  </tr>
  <tr>
    <td>INTERVALDAY and INTERVALYEAR</td>
    <td>Integer fields representing a period of time depending on the type of interval</td>
    <td>INTERVAL</td>
    <td>An interval of time. Annotates a fixed_len_byte_array of length 12. Months, days, and ms in unsigned little-endian format.</td>
  </tr>
</table>

### Data Description Language Support
Parquet supports the following data description languages:

* Apache Avro
* Apache Thrift
* Google Protocol Buffers 

Implement custom storage plugins, such as an Avro plugin, to create Parquet readers/writers for these formats. 