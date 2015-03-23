---
title: "SQL Extensions"
parent: "SQL Reference"
---
Drill extends SQL to work with Hadoop-scale data and to explore smaller-scale data in ways not possible with SQL. Using intuitive SQL extensions you work with self-describing data and complex data types. Extensions to SQL include capabilities for exploring self-describing data, such as files and HBase, directly in the native format.

Drill provides language support for pointing to [storage plugin]() interfaces that Drill uses to interact with data sources. Use the name of a storage plugin to specify a file system *database* as a prefix in queries when you refer to objects across databases. Query files, including compressed .gz files and directories like an SQL table using a single query. 

Drill extends the SELECT statement for reading complex, multi-structured data. The extended CREATE TABLE AS SELECT, provides the capability to write data of complex/multi-structured data types. Drill extends the [lexical rules](http://drill.apache.org/docs/lexical-structure) for working with files and directories, such as using back ticks for including file names, directory names, and reserved words in queries. Drill syntax supports using the file system as a persistent store for query profiles and diagnostic information.

## Extensions for Hive- and HBase-related Data Sources

Drill supports Hive and HBase as a plug-and-play data source. You can query Hive tables with no modifications and creating model in the Hive metastore. Primitives, such as JOIN, support columnar operation.

## Extensions for JSON-related Data Sources
For reading all JSON data as text, use the all text mode extension. Drill extends SQL to provide access to repeating values in arrays and arrays within arrays (array indexes). You can use these extensions to reach into deeply nested data. Drill extensions use standard JavaScript notation for referencing data elements in a hierarchy.

## Extensions for Text Data Sources
Drill handles plain text files and directories like standard SQL tables and can infer knowledge about the schema of the data. You can query compressed .gz files.

## SQL Commands Extensions

The following table describes key Drill extensions to SQL commands.

<table>
  <tr>
    <th>Command</th>
    <th>SQL</th>
    <th>Drill</th>
  </tr>
  <tr>
    <td>ALTER (SESSION | SYSTEM)</td>
    <td>None</td>
    <td>Changes a system or session option.</td>
  </tr>
  <tr>
    <td>CREATE TABLE AS SELECT</td>
    <td>Creates a table from selected data in an existing database table.</td>
    <td>Stores selected data from one or more data sources on the file system.</td>
  </tr>
  <tr>
    <td>CREATE VIEW</td>
    <td>Creates a virtual table. The fields in a view are fields from one or more real tables in the database.</td>
    <td>Creates a virtual structure for and stores the result set. The fields in a view are fields from files in a file system, Hive, Hbase, MapR-DB tables</td>
  </tr>
  <tr>
    <td>DESCRIBE</td>
    <td>Obtains information about the &lt;select list&gt; columns</td>
    <td>Obtains information about views created in a workspace and tables created in Hive, HBase, and MapR-DB.</td>
  </tr>
  <tr>
    <td>EXPLAIN</td>
    <td>None</td>
    <td>Obtains a query execution plan.</td>
  </tr>
  <tr>
    <td>INSERT</td>
    <td>Loads data into the database for querying.</td>
    <td>No INSERT function. Performs schema-on-read querying and execution; no need to load data into Drill for querying.</td>
  </tr>
  <tr>
    <td>SELECT</td>
    <td>Retrieves rows from a database table or view.</td>
    <td>Retrieves data from Hbase, Hive, MapR-DB, file system or other storage plugin data source.</td>
  </tr>
  <tr>
    <td>SHOW (DATABASES | SCHEMAS | FILES | TABLES)</td>
    <td>None</td>
    <td>Lists the storage plugin data sources available for querying or the Hive, Hbase, MapR-DB tables, or views for the data source in use. Supports a FROM clause for listing file data sources in directories.</td>
  </tr>
  <tr>
    <td>USE</td>
    <td>Targets a database in SQL schema for querying.</td>
    <td>Targets Hbase, Hive, MapR-DB, file system or other storage plugin data source, which can be schema-less for querying.</td>
  </tr>
</table>

## SQL Function Extensions
The following table describes key Drill functions for analyzing nested data.

<table>
  <tr>
    <th>Function</th>
    <th>SQL</th>
    <th>Drill</th>
  </tr>
  <tr>
    <td>CAST</td>
    <td>Casts database data from one type to another.</td>
    <td>Casts database data from one type to another and also casts data having no metadata into a readable type. Allows liberal casting of schema-less data.</td>
  </tr>
  <tr>
    <td>CONVERT_TO</td>
    <td>Converts an expression from one type to another using the CONVERT command.</td>
    <td>Converts an SQL data type to complex types, including Hbase byte arrays, JSON and Parquet arrays and maps.</td>
  </tr>
  <tr>
    <td>CONVERT_FROM</td>
    <td>Same as above</td>
    <td>Converts from complex types, including Hbase byte arrays, JSON and Parquet arrays and maps to an SQL data type.</td>
  </tr>
  <tr>
    <td>FLATTEN</td>
    <td>None</td>
    <td>Separates the elements in nested data from a repeated field into individual records.</td>
  </tr>
  <tr>
    <td>KVGEN</td>
    <td>None</td>
    <td>Returns a repeated map, generating key-value pairs to simplify querying of complex data having unknown column names. You can then aggregate or filter on the key or value.</td>
  </tr>
  <tr>
    <td>REPEATED_COUNT</td>
    <td>None</td>
    <td>Counts the values in a JSON array.</td>
  </tr>
</table>

## Other Extensions

[`sys` database system tables]() provide port, version, and option information. Drill Connects to a random node, know where you’re connected:

select host from sys.drillbits where `current` = true;
+------------+
|    host    |
+------------+
| 10.1.1.109 |
+------------+

select commit_id from sys.version;
+------------+
| commit_id  |
+------------+
| e3ab2c1760ad34bda80141e2c3108f7eda7c9104 |

