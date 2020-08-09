---
title: "Querying HBase"
date: 2020-08-08
parent: "Query Data"
---

This section covers the following topics:

* [Tutorial--Querying HBase Data]({{site.baseurl}}/docs/querying-hbase/#tutorial-querying-hbase-data)  
  A simple tutorial that shows how to use Drill to query HBase data.  
* [Working with HBase Byte Arrays]({{site.baseurl}}/docs/querying-hbase/#working-with-hbase-byte-arrays)  
  How to work with HBase byte arrays for serious applications  
* [Querying Big Endian-Encoded Data]({{site.baseurl}}/docs/querying-hbase/#querying-big-endian-encoded-data)  
  How to use optimization features in Drill 1.2 and later  
* [Leveraging HBase Ordered Byte Encoding]({{site.baseurl}}/docs/querying-hbase/#leveraging-hbase-ordered-byte-encoding)  
  How to use Drill 1.2 to leverage new features introduced by [HBASE-8201 Jira](https://issues.apache.org/jira/browse/HBASE-8201)

## Tutorial--Querying HBase Data

This tutorial shows how to connect Drill to an HBase data source, create simple HBase tables, and query the data using Drill.

----------

### Configure the HBase Storage Plugin

To query an HBase data source using Drill, first configure the [HBase storage plugin]({{site.baseurl}}/docs/hbase-storage-plugin/) for your environment. 

----------

### Create the HBase tables

You create two tables in HBase, students and clicks, that you can query with Drill. You use the CONVERT_TO and CONVERT_FROM functions to convert binary text to/from typed data. You use the CAST function to convert the binary data to an INT in step 4 of [Query HBase Tables]({{site.baseurl}}/docs/querying-hbase/#query-hbase-tables). When converting an INT or BIGINT number, having a byte count in the destination/source that does not match the byte count of the number in the binary source/destination, use CAST.

To create the HBase tables, complete the following
steps:

1. Pipe the following commands to the HBase shell to create students and clicks tables in HBase:
  
          echo "create 'students','account','address'" | hbase shell
          echo "create 'clicks','clickinfo','iteminfo'" | hbase shell

2. Issue the following command to create a `testdata.txt` file:

    `cat > testdata.txt`

3. Copy and paste the following `put` commands on the line below the **cat** command. Press Return, and then CTRL Z to close the file.

        put 'students','student1','account:name','Alice'
        put 'students','student1','address:street','123 Ballmer Av'
        put 'students','student1','address:zipcode','12345'
        put 'students','student1','address:state','CA'
        put 'students','student2','account:name','Bob'
        put 'students','student2','address:street','1 Infinite Loop'
        put 'students','student2','address:zipcode','12345'
        put 'students','student2','address:state','CA'
        put 'students','student3','account:name','Frank'
        put 'students','student3','address:street','435 Walker Ct'
        put 'students','student3','address:zipcode','12345'
        put 'students','student3','address:state','CA'
        put 'students','student4','account:name','Mary'
        put 'students','student4','address:street','56 Southern Pkwy'
        put 'students','student4','address:zipcode','12345'
        put 'students','student4','address:state','CA'
        put 'clicks','click1','clickinfo:studentid','student1'
        put 'clicks','click1','clickinfo:url','http://www.google.com'
        put 'clicks','click1','clickinfo:time','2014-01-01 12:01:01.0001'
        put 'clicks','click1','iteminfo:itemtype','image'
        put 'clicks','click1','iteminfo:quantity','1'
        put 'clicks','click2','clickinfo:studentid','student1'
        put 'clicks','click2','clickinfo:url','http://www.amazon.com'
        put 'clicks','click2','clickinfo:time','2014-01-01 01:01:01.0001'
        put 'clicks','click2','iteminfo:itemtype','image'
        put 'clicks','click2','iteminfo:quantity','1'
        put 'clicks','click3','clickinfo:studentid','student2'
        put 'clicks','click3','clickinfo:url','http://www.google.com'
        put 'clicks','click3','clickinfo:time','2014-01-01 01:02:01.0001'
        put 'clicks','click3','iteminfo:itemtype','text'
        put 'clicks','click3','iteminfo:quantity','2'
        put 'clicks','click4','clickinfo:studentid','student2'
        put 'clicks','click4','clickinfo:url','http://www.ask.com'
        put 'clicks','click4','clickinfo:time','2013-02-01 12:01:01.0001'
        put 'clicks','click4','iteminfo:itemtype','text'
        put 'clicks','click4','iteminfo:quantity','5'
        put 'clicks','click5','clickinfo:studentid','student2'
        put 'clicks','click5','clickinfo:url','http://www.reuters.com'
        put 'clicks','click5','clickinfo:time','2013-02-01 12:01:01.0001'
        put 'clicks','click5','iteminfo:itemtype','text'
        put 'clicks','click5','iteminfo:quantity','100'
        put 'clicks','click6','clickinfo:studentid','student3'
        put 'clicks','click6','clickinfo:url','http://www.google.com'
        put 'clicks','click6','clickinfo:time','2013-02-01 12:01:01.0001'
        put 'clicks','click6','iteminfo:itemtype','image'
        put 'clicks','click6','iteminfo:quantity','1'
        put 'clicks','click7','clickinfo:studentid','student3'
        put 'clicks','click7','clickinfo:url','http://www.ask.com'
        put 'clicks','click7','clickinfo:time','2013-02-01 12:45:01.0001'
        put 'clicks','click7','iteminfo:itemtype','image'
        put 'clicks','click7','iteminfo:quantity','10'
        put 'clicks','click8','clickinfo:studentid','student4'
        put 'clicks','click8','clickinfo:url','http://www.amazon.com'
        put 'clicks','click8','clickinfo:time','2013-02-01 22:01:01.0001'
        put 'clicks','click8','iteminfo:itemtype','image'
        put 'clicks','click8','iteminfo:quantity','1'
        put 'clicks','click9','clickinfo:studentid','student4'
        put 'clicks','click9','clickinfo:url','http://www.amazon.com'
        put 'clicks','click9','clickinfo:time','2013-02-01 22:01:01.0001'
        put 'clicks','click9','iteminfo:itemtype','image'
        put 'clicks','click9','iteminfo:quantity','10'

4. Issue the following command to put the data into HBase:  
  
        cat testdata.txt | hbase shell

----------

### Query HBase Tables

[Start Drill]({{site.baseurl}}/docs/installing-drill-in-embedded-mode/) and complete the following steps to query the HBase tables you created.

1. Use the HBase storage plugin configuration.  
    `USE hbase;`  
2. Issue the following query to see the data in the students table:  
    `SELECT * FROM students;`  
    
    The query returns results that are not useable. In the next step, you convert the data from byte arrays to UTF8 types that are meaningful.
  
        |-------------|-----------------------|---------------------------------------------------------------------------|
        |  row_key    |  account              |                                address                                    |
        |-------------|-----------------------|---------------------------------------------------------------------------|
        | [B@e6d9eb7  | {"name":"QWxpY2U="}   | {"state":"Q0E=","street":"MTIzIEJhbGxtZXIgQXY=","zipcode":"MTIzNDU="}     |
        | [B@2823a2b4 | {"name":"Qm9i"}       | {"state":"Q0E=","street":"MSBJbmZpbml0ZSBMb29w","zipcode":"MTIzNDU="}     |
        | [B@3b8eec02 | {"name":"RnJhbms="}   | {"state":"Q0E=","street":"NDM1IFdhbGtlciBDdA==","zipcode":"MTIzNDU="}     |
        | [B@242895da | {"name":"TWFyeQ=="}   | {"state":"Q0E=","street":"NTYgU291dGhlcm4gUGt3eQ==","zipcode":"MTIzNDU="} |
        |-------------|-----------------------|---------------------------------------------------------------------------|
        4 rows selected (1.335 seconds)

3. Issue the following query, that includes the CONVERT_FROM function, to convert the `students` table to typed data:

         SELECT CONVERT_FROM(row_key, 'UTF8') AS studentid, 
                CONVERT_FROM(students.account.name, 'UTF8') AS name, 
                CONVERT_FROM(students.address.state, 'UTF8') AS state, 
                CONVERT_FROM(students.address.street, 'UTF8') AS street, 
                CONVERT_FROM(students.address.zipcode, 'UTF8') AS zipcode 
         FROM students;

    {% include startnote.html %}Use dot notation to drill down to a column in an HBase table: tablename.columnfamilyname.columnnname{% include endnote.html %}
    

    The query returns results that look much better:

        |------------|------------|------------|------------------|------------|
        | studentid  |    name    |   state    |       street     |  zipcode   |
        |------------|------------|------------|------------------|------------|
        | student1   | Alice      | CA         | 123 Ballmer Av   | 12345      |
        | student2   | Bob        | CA         | 1 Infinite Loop  | 12345      |
        | student3   | Frank      | CA         | 435 Walker Ct    | 12345      |
        | student4   | Mary       | CA         | 56 Southern Pkwy | 12345      |
        |------------|------------|------------|------------------|------------|
        4 rows selected (0.504 seconds)

4. Query the clicks table to see which students visited google.com:
  
        SELECT CONVERT_FROM(row_key, 'UTF8') AS clickid, 
               CONVERT_FROM(clicks.clickinfo.studentid, 'UTF8') AS studentid, 
               CONVERT_FROM(clicks.clickinfo.`time`, 'UTF8') AS `time`,
               CONVERT_FROM(clicks.clickinfo.url, 'UTF8') AS url 
        FROM clicks WHERE clicks.clickinfo.url LIKE '%google%'; 

        |------------|------------|--------------------------|-----------------------|
        |  clickid   | studentid  |           time           |         url           |
        |------------|------------|--------------------------|-----------------------|
        | click1     | student1   | 2014-01-01 12:01:01.0001 | http://www.google.com |
        | click3     | student2   | 2014-01-01 01:02:01.0001 | http://www.google.com |
        | click6     | student3   | 2013-02-01 12:01:01.0001 | http://www.google.com |
        |------------|------------|--------------------------|-----------------------|
        3 rows selected (0.294 seconds)

5. Query the clicks table to get the studentid of the student having 100 items. Use CONVERT_FROM to convert the textual studentid and itemtype data, but use CAST to convert the integer quantity.

        SELECT CONVERT_FROM(tbl.clickinfo.studentid, 'UTF8') AS studentid, 
               CONVERT_FROM(tbl.iteminfo.itemtype, 'UTF8'), 
               CAST(tbl.iteminfo.quantity AS INT) AS items 
        FROM clicks tbl WHERE tbl.iteminfo.quantity=100;

        |------------|------------|------------|
        | studentid  |   EXPR$1   |   items    |
        |------------|------------|------------|
        | student2   | text       | 100        |
        |------------|------------|------------|
        1 row selected (0.656 seconds)


## Working with HBase Byte Arrays

The trivial example in the previous section queried little endian-encoded data in HBase. For serious applications, you need to understand how to work with HBase byte arrays. If you want Drill to interpret the underlying HBase row key as something other than a byte array, you need to know the encoding of the data in HBase. By default, HBase stores data in little endian and Drill assumes the data is little endian, which is unsorted. The following table shows the sorting of typical row key IDs in bytes, encoded in little endian and big endian, respectively:

| IDs in Byte Notation Little Endian Sorting | IDs in Decimal Notation | IDs in Byte Notation Big Endian Sorting | IDs in Decimal Notation |
|--------------------------------------------|-------------------------|-----------------------------------------|-------------------------|
| 0 x 010000 . . . 000                       | 1                       | 0 x 00000001                            | 1                       |
| 0 x 010100 . . . 000                       | 17                      | 0 x 00000002                            | 2                       |
| 0 x 020000 . . . 000                       | 2                       | 0 x 00000003                            | 3                       |
| . . .                                      |                         | 0 x 00000004                            | 4                       |
| 0 x 050000 . . . 000                       | 5                       | 0 x 00000005                            | 5                       |
| . . .                                      |                         | . . .                                   |                         |
| 0 x 0A0000 . . . 000                       | 10                      | 0 x 0000000A                            | 10                      |
|                                            |                         | 0 x 00000101                            | 17                      |

## Querying Big Endian-Encoded Data

Drill optimizes scans of HBase tables when you use the ["CONVERT_TO and CONVERT_FROM data types"]({{ site.baseurl }}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions) on big endian-encoded data. Drill provides the \*\_BE encoded types for use with CONVERT_TO and CONVERT_FROM to take advantage of these optimizations. Here are a few examples of the \*\_BE types.

* DATE_EPOCH_BE  
* TIME_EPOCH_BE  
* TIMESTAMP_EPOCH_BE  
* UINT8_BE  
* BIGINT_BE  

For example, Drill returns results performantly when you use the following query on big endian-encoded data:

```
SELECT
  CONVERT_FROM(BYTE_SUBSTR(row_key, 1, 8), 'DATE_EPOCH_BE') d,
  CONVERT_FROM(BYTE_SUBSTR(row_key, 9, 8), 'BIGINT_BE') id,
  CONVERT_FROM(tableName.f.c, 'UTF8') 
FROM hbase.`TestTableCompositeDate` tableName
WHERE
  CONVERT_FROM(BYTE_SUBSTR(row_key, 1, 8), 'DATE_EPOCH_BE') < DATE '2015-06-18' AND
  CONVERT_FROM(BYTE_SUBSTR(row_key, 1, 8), 'DATE_EPOCH_BE') > DATE '2015-06-13';
```

This query assumes that the row key of the table represents the DATE_EPOCH type encoded in big-endian format. The Drill HBase plugin will be able to prune the scan range since there is a condition on the big endian-encoded prefix of the row key. For more examples, see the [test code](https://github.com/apache/drill/blob/95623912ebf348962fe8a8846c5f47c5fdcf2f78/contrib/storage-hbase/src/test/java/org/apache/drill/hbase/TestHBaseFilterPushDown.java).

To query HBase data:

1. Connect the data source to Drill using the [HBase storage plugin]({{site.baseurl}}/docs/hbase-storage-plugin/).  

    `USE hbase;`

2. Determine the encoding of the HBase data you want to query. Ask the person in charge of creating the data.  
3. Based on the encoding type of the data, use the ["CONVERT_TO and CONVERT_FROM data types"]({{ site.baseurl }}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions) to convert HBase binary representations to an SQL type as you query the data.  
    For example, use CONVERT_FROM in your Drill query to convert a big endian-encoded row key to an SQL BIGINT type:  

    `SELECT CONVERT_FROM(BYTE_SUBSTR(row_key, 1, 8),'BIGINT_BE’) FROM my_hbase_table;`

The [BYTE_SUBSTR function]({{ site.baseurl }}/docs/string-manipulation/#byte_substr) separates parts of a HBase composite key in this example. The Drill optimization is based on the capability in Drill 1.2 and later to push conditional filters down to the storage layer when HBase data is in big endian format. 

Drill can performantly query HBase data that uses composite keys, as shown in the last example, if only the first component of the composite is encoded in big endian format. If the HBase row key is not stored in big endian, do not use the \*\_BE types. If you want to convert a little endian byte array to integer, use BIGINT instead of BIGINT_BE, for example, as an argument to CONVERT_FROM. 

## Leveraging HBase Ordered Byte Encoding

Drill 1.2 leverages new features introduced by [HBASE-8201 Jira](https://issues.apache.org/jira/browse/HBASE-8201) that allows ordered byte encoding of different data types. This encoding scheme preserves the sort order of the native data type when the data is stored as sorted byte arrays on disk. Thus, Drill will be able to process data through the HBase storage plugin if the row keys have been encoded in OrderedBytes format.

To execute the following query, Drill prunes the scan range to only include the row keys representing [-32,59] range, thus reducing the amount of data read.

```
SELECT
 CONVERT_FROM(t.row_key, 'INT_OB') rk,
 CONVERT_FROM(t.`f`.`c`, 'UTF8') val
FROM
  hbase.`TestTableIntOB` t
WHERE
  CONVERT_FROM(row_key, 'INT_OB') >= cast(-32 as INT) AND
  CONVERT_FROM(row_key, 'INT_OB') < cast(59 as INT);
```

For more examples, see the [test code](https://github.com/apache/drill/blob/95623912ebf348962fe8a8846c5f47c5fdcf2f78/contrib/storage-hbase/src/test/java/org/apache/drill/hbase/TestHBaseFilterPushDown.java).

By taking advantage of ordered byte encoding, Drill 1.2 and later can performantly execute conditional queries without a secondary index on HBase big endian data. 


