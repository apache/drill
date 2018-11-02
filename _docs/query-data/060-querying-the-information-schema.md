---
title: "Querying the INFORMATION SCHEMA"
date: 2018-11-02 17:50:26 UTC
parent: "Query Data"
---
When you are using Drill to connect to multiple data sources, you need a
simple mechanism to discover what each data source contains. The information
schema is an ANSI standard set of metadata tables that you can query to return
information about all of your Drill data sources (or schemas). Data sources
may be databases or file systems; they are all known as "schemas" in this
context. You can query the following INFORMATION_SCHEMA tables:

  * SCHEMATA
  * CATALOGS
  * TABLES
  * COLUMNS 
  * VIEWS

## SCHEMATA

The SCHEMATA table contains the CATALOG_NAME and SCHEMA_NAME columns. To allow
maximum flexibility inside BI tools, the only catalog that Drill supports is
`DRILL`.

    SELECT CATALOG_NAME, SCHEMA_NAME as all_my_data_sources FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME;
    +--------------+---------------------+
    | CATALOG_NAME | all_my_data_sources |
    +--------------+---------------------+
    | DRILL        | INFORMATION_SCHEMA  |
    | DRILL        | cp.default          |
    | DRILL        | dfs.default         |
    | DRILL        | dfs.root            |
    | DRILL        | dfs.tmp             |
    | DRILL        | HiveTest.SalesDB    |
    | DRILL        | maprfs.logs         |
    | DRILL        | sys                 |
    +--------------+---------------------+

The INFORMATION_SCHEMA name and associated keywords are case-insensitive. You
can also return a list of schemas by running the SHOW DATABASES command:

    SHOW DATABASES;
    +-------------+
    | SCHEMA_NAME |
    +-------------+
    | dfs.default |
    | dfs.root    |
    | dfs.tmp     |
    ...

## CATALOGS

The CATALOGS table returns only one row, with the hardcoded DRILL catalog name
and description.

## TABLES

The TABLES table returns the table name and type for each table or view in
your databases. (Type means TABLE or VIEW.) Note that Drill does not return
files available for querying in file-based data sources. Instead, use SHOW
FILES to explore these data sources.

## COLUMNS

The COLUMNS table returns the column name and other metadata (such as the data
type) for each column in each table or view.

## VIEWS

The VIEWS table returns the name and definition for each view in your
databases. Note that file schemas are the canonical repository for views in
Drill. Depending on how you create a view, the may only be displayed in Drill
after it has been used.

## Useful Queries

Run an ``INFORMATION_SCHEMA.`TABLES` ``query to view all of the tables and views
within a database. TABLES is a reserved word in Drill and requires back ticks
(`).

For example, the following query identifies all of the tables and views that
Drill can access:

    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.`TABLES`
    ORDER BY TABLE_NAME DESC;
    ----------------------------------------------------------------
    TABLE_SCHEMA             TABLE_NAME            TABLE_TYPE
    ----------------------------------------------------------------
    HiveTest.CustomersDB     Customers             TABLE
    HiveTest.SalesDB         Orders                TABLE
    HiveTest.SalesDB         OrderLines            TABLE
    HiveTest.SalesDB         USOrders              VIEW
    dfs.default              CustomerSocialProfile VIEW
    ----------------------------------------------------------------

{% include startnote.html %}Currently, Drill only supports querying Drill views; Hive views are not yet supported.{% include endnote.html %}

You can run a similar query to identify columns in tables and the data types
of those columns:

    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'Orders' AND TABLE_SCHEMA = 'HiveTest.SalesDB' AND COLUMN_NAME LIKE '%Total';
    +-------------+------------+
    | COLUMN_NAME | DATA_TYPE  |
    +-------------+------------+
    | OrderTotal  | Decimal    |
    +-------------+------------+  

