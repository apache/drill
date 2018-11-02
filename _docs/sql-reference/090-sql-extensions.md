---
title: "SQL Extensions"
date: 2018-11-02
parent: "SQL Reference"
---
Drill extends SQL to explore smaller-scale data in ways not possible with SQL. Using intuitive SQL extensions you work with self-describing data and complex data types. Extensions to SQL include capabilities for exploring self-describing data, such as files and HBase, directly in the native format.

Drill provides language support for pointing to [storage plugin]({{site.baseurl}}/docs/connect-a-data-source-introduction) interfaces that Drill uses to interact with data sources. Use the name of a storage plugin to specify a file system *database* as a prefix in queries when you refer to objects across databases. Query files, including compressed .gz files, and [directories]({{ site.baseurl }}/docs/querying-directories), as you would query an SQL table. You can query multiple files in a directory.

Drill extends the SELECT statement for reading complex, multi-structured data. The extended CREATE TABLE AS provides the capability to write data of complex/multi-structured data types. Drill extends the [lexical rules](http://drill.apache.org/docs/lexical-structure) for working with files and directories, such as using back ticks for including file names, directory names, and reserved words in queries. Drill syntax supports using the file system as a persistent store for query profiles and diagnostic information.

## Extensions for Hive- and HBase-related Data Sources

Drill supports Hive and HBase as a plug-and-play data source. Drill can read tables created in Hive that use [data types compatible]({{ site.baseurl }}/docs/hive-to-drill-data-type-mapping) with Drill.  You can query Hive tables without modifications. You can query self-describing data without requiring metadata definitions in the Hive metastore. Primitives, such as JOIN, support columnar operation. 

## Extensions for JSON-related Data Sources
For reading JSON numbers as DOUBLE or reading all JSON data as VARCHAR, use a [store.json option]({{site.baseurl}}/docs/handling-different-data-types/#reading-numbers-of-different-types-from-json). Drill extends SQL to provide access to repeating values in arrays and arrays within arrays (array indexes). You can use these extensions to reach into deeply nested data. Drill extensions use standard JavaScript notation for referencing data elements in a hierarchy, as shown in ["Analyzing JSON."]({{ site.baseurl }}/docs/json-data-model/#analyzing-json)

## Extensions for Parquet Data Sources
SQL does not support all Parquet data types, so Drill infers data types in many instances. Users [cast] ({{ site.baseurl }}/docs/sql-functions) data types to ensure getting a particular data type. Drill offers more liberal casting capabilities than SQL for Parquet conversions if the Parquet data is of a logical type. You can use the default dfs storage plugin installed with Drill for reading and writing Parquet files as shown in the section, [“Parquet Format.”]({{ site.baseurl }}/docs/parquet-format)


## Extensions for Text Data Sources
Drill handles plain text files and directories like standard SQL tables and can infer knowledge about the schema of the data. Drill extends SQL to handle structured file types, such as comma separated values (CSV) files. An extension of the SELECT statement provides COLUMNS[n] syntax for accessing CSV rows in a readable format, as shown in ["COLUMNS[n] Syntax."]({{ site.baseurl }}/docs/querying-plain-text-files/#columns[n]-syntax)

## SQL Function Extensions
Drill provides the following functions for analyzing nested data.

| Function          | SQL  | Drill                                                                                                                                                                      |
|-------------------|------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| FLATTEN           | None | Separates the elements in nested data from a repeated field into individual records.                                                                                       |
| KVGEN             | None | Returns a repeated map, generating key-value pairs to simplify querying of complex data having unknown column names. You can then aggregate or filter on the key or value. |
| REPEATED_COUNT    | None | Counts the values in an array.                                                                                                                                             |
| REPEATED_CONTAINS | None | Searches for a keyword in an array.                                                                                                                                        |

## Other Extensions

The [`sys` tables](/docs/querying-system-tables/) provide port, version, and option information.  For example, Drill connects to a random node. You query the sys table to know where you are connected:

    SELECT host FROM sys.drillbits WHERE `current` = true;
    +------------+
    |    host    |
    +------------+
    | 10.1.1.109 |
    +------------+

    SELECT commit_id FROM sys.version;
    +-------------------------------------------+
    |                 commit_id                 |
    +-------------------------------------------+
    | e3fc7e97bfe712dc09d43a8a055a5135c96b7344  |
    +-------------------------------------------+
    1 row selected (0.105 seconds)
