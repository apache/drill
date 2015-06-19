---
title: "PARTITION BY Clause"
parent: "SQL Commands"
---
You can take advantage of automatic partitioning in Drill 1.1 using the PARTITION BY CLAUSE in the CTAS command:

	CREATE TABLE table_name [ (column_name, . . .) ] 
    [ PARTITION_BY (column_name, . . .) ] 
    AS SELECT_statement;

The CTAS statement that uses the PARTITION BY clause must store the data in Parquet format. The CTAS statement needs to meet one of the following requirements:

* The column list in the PARTITION by clause are included in the column list following the table_name
* The SELECT statement has to use a * column if the base table in the SELECT statement is schema-less, and when the partition column is resolved to * column in a schema-less query, this * column cannot be a result of a join operation. 


To create and verify the contents of a table that contains this row:

  1. Set the workspace to a writable workspace.
  2. Set the `store.format` option to Parquet
  3. Run a CTAS statement with the PARTITION BY clause.
  4. Go to the directory where the table is stored and check the contents of the file.
  5. Run a query against the new table.

Examples:

	CREATE TABLE mytable1 PARTITION BY (r_regionkey) AS 
	  SELECT r_regionkey, r_name FROM cp.`tpch/region.parquet`
	CREATE TABLE mytable2 PARTITION BY (r_regionkey) AS 
	  SELECT * FROM cp.`tpch/region.parquet`
	CREATE TABLE mytable3 PARTITION BY (r_regionkey) AS
	  SELECT r.r_regionkey, r.r_name, n.n_nationkey, n.n_name 
	  FROM cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r
	  WHERE n.n_regionkey = r.r_regionkey



