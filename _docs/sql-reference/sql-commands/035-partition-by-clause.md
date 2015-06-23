---
title: "PARTITION BY Clause"
parent: "SQL Commands"
---
You can take advantage of automatic partitioning in Drill 1.1 by using the PARTITION BY clause in the CTAS command:

## Syntax

	CREATE TABLE table_name [ (column_name, . . .) ] 
    [ PARTITION_BY (column_name, . . .) ] 
    AS SELECT_statement;

The CTAS statement that uses the PARTITION BY clause must store the data in Parquet format and meet one of the following requirements:

* The columns in the column list in the PARTITION BY clause are included in the column list following the table_name
* The SELECT statement has to use a * column (SELECT *) if the base table in the SELECT statement is schema-less, and when the partition column is resolved to a * column in a schema-less query, this * column cannot be a result of a join operation. 

The output of using the PARTITION BY clause creates separate files. Each file contains one partition value, and Drill can create multiple files for the same partition value.

Partition pruning uses the parquet column stats to determine which which columns can be used to prune.

Examples:

    USE cp;
	CREATE TABLE mytable1 PARTITION BY (r_regionkey) AS 
	  SELECT r_regionkey, r_name FROM cp.`tpch/region.parquet`;
	CREATE TABLE mytable2 PARTITION BY (r_regionkey) AS 
	  SELECT * FROM cp.`tpch/region.parquet`;
	CREATE TABLE mytable3 PARTITION BY (r_regionkey) AS
	  SELECT r.r_regionkey, r.r_name, n.n_nationkey, n.n_name 
	  FROM cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r
	  WHERE n.n_regionkey = r.r_regionkey;



