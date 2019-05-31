---
title: "ANALYZE TABLE"
date: 2019-05-31
parent: "SQL Commands"
---  

Drill 1.16 and later supports the ANALYZE TABLE statement. The ANALYZE TABLE statement computes statistics on Parquet data stored in tables and directories. The optimizer in Drill uses statistics to estimate filter, aggregation, and join cardinalities and create an optimal query plan. 
ANALYZE TABLE writes statistics to a JSON file in the `.stats.drill` directory, for example `/user/table1/.stats.drill/0_0.json`. 

Drill will not use the statistics for query planning unless you enable the `planner.statistics.use` option, as shown:

	SET `planner.statistics.use` = true;

Alternatively, you can enable the option in the Drill Web UI at `http://<drill-hostname-or-ip-address>:8047/options`.

## Syntax

The ANALYZE TABLE statement supports the following syntax:  

	ANALYZE TABLE [workspace.]table_name COMPUTE STATISTICS [(column1, column2,...)] [SAMPLE number PERCENT]
        

## Parameters

*workspace*  
Optional. A configured storage plugin and workspace, like `dfs.samples`. For example, in `dfs.samples`, `dfs` is the file system storage plugin and samples is the `workspace` configured to point to a directory on the file system. 

*table_name*  
The name of the table or directory for which Drill will generate statistics. 

*COMPUTE STATISTICS*  
Generates statistics for the table, columns, or directory specified.   

*column*  
The name of the column(s) for which Drill will generate statistics.  

*SAMPLE*    
Optional. Indicates that compute statistics should run on a subset of the data.

*number PERCENT*  
An integer that specifies the percentage of data on which to compute statistics. For example, if a table has 100 rows, `SAMPLE 50 PERCENT` indicates that statistics should be computed on 50 rows. The optimizer selects the rows at random. 

## Related Command  

If you drop a table that you have already run ANALYZE TABLE against, the statistics are automatically removed with the table:  

	DROP TABLE [IF EXISTS] [workspace.]name  

To remove statistics for a table you want to keep, you must remove the directory in which Drill stores the statistics:  

	DROP TABLE [IF EXISTS] [workspace.]name/.stats.drill  

If you have already issued the ANALYZE TABLE statement against specific columns, table, or directory, you must run the DROP TABLE statement with `/.stats.drill` before you can successfully run the ANALYZE TABLE statement against the data source again, for example:

	DROP TABLE `table_stats/Tpch0.01/parquet/customer/.stats.drill`;


Note that `/.stats.drill` is the directory to which the JSON file with statistics is written.   

## Usage Notes  


- The ANALYZE TABLE statement can compute statistics for Parquet data stored in tables, columns, and directories within dfs storage plugins only.  
- The user running the ANALYZE TABLE statement must have read and write permissions on the data source.  
- The optimizer in Drill computes the following types of statistics for each column:  
	- Rowcount (total number of entries in the table)  
	- Nonnullrowcount (total number of non-null entries in the table)  
	- NDV (total distinct values in the table)  
	- Avgwidth (average width, in bytes, of a column)  
	- Majortype (data type and data mode (OPTIONAL, REQUIRED, REPEATED) of the column values)  
	- Histogram (represents the frequency distribution of values (numeric data) in a column) See [Histograms]({{site.baseurl}}/docs/analyze-table/#histograms).  
	- When you look at the statistics file, statistics for each column display in the following format (c_nationkey is used as an example column):  
	
			{"column":"`c_nationkey`","majortype":{"type":"INT","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":25,"avgwidth":4.0,"histogram":{"category":"numeric-equi-depth","numRowsPerBucket":150,"buckets":[0.0,2.0,4.0,7.0,9.0,12.0,15.199999999999978,17.0,19.0,22.0,24.0]}}  

- ANALYZE TABLE can compute statistics on nested scalar columns; however, you must explicitly state the columns, for example:    
		 `ANALYZE TABLE employee_table COMPUTE STATISTICS (name.firstname, name.lastname);`  
- ANALYZE TABLE can compute statistics at the root directory level, but not at the partition level. 
Drill does not compute statistics for complex types (maps, arrays).
 

## Related Options
You can set the following options related to the ANALYZE TABLE statement at the system or session level with the SET (session level) or ALTER SYSTEM SET (system level) statements, or through the Drill Web UI at `http://<drill-hostname-or-ip>:8047/options`:  

- **planner.statistics.use**  
Enables the query planner to use statistics. When disabled, ANALYZE TABLE generates statistics, but the query planner will not use the statistics unless this option is enabled. Disabled (false) by default. 
- **exec.statistics.ndv_accuracy**  
Controls the trade-off between NDV statistic computation memory cost and accuracy. Controls the amount of memory for estimates. More memory produces more accurate estimates. The default value should suffice for most scenarios. Default is 20. Range is 0- 30.  
- **exec.statistics.ndv_extrapolation_bf_elements**  
Controls the trade-off between NDV statistics computation memory cost and sampling extrapolation accuracy. Relates specifically to SAMPLE. The default value should suffice for most scenarios. Increasing the value requires additional memory. Default is 1000000.  
- **exec.statistics.ndv_extrapolation_bf_fpprobability**  
Controls the trade-off between NDV statistics computation memory cost and sampling extrapolation accuracy. Controls the overall accuracy of statistics when using sampling. Default is 10 percent. Range is 0-100.  
- **exec.statistics.deterministic_sampling**  
Turns deterministic sampling on and off. Relates specifically to SAMPLE. Default is false.  
- **exec.statistics.tdigest_compression**  
Controls the 'compression' factor for the TDigest algorithm used for histogram statistics. Controls trade-off between t-digest quantile statistic storage cost and accuracy. Higher values use more groups (clusters) for the t-digest and improve accuracy at the expense of extra storage. Positive integer values in the range [1, 10000].  Default is 100.  
 


## Reserved Keywords

The ANALYZE TABLE statement introduces the following reserved keywords:  
 
	Analyze  
	Compute  
	Estimate  
	Statistics  
	Sample  

If you use any of these words in a Drill query, you must enclose the word in backticks. For example, if you query a table named “estimate,” you would enclose the word "estimate" in backticks, as shown:  

	SELECT * FROM `estimate`;

 
## ANALYZE TABLE Performance

- After you run the ANALYZE TABLE statement, you can view the profile for ANALYZE in the Drill Web UI. Go to `http://<drill-hostname-or-ip>:8047/profiles`, and click the ANALYZE TABLE statement for which you want to view the profile.  
- Should you notice any performance issues, you may want to decrease the value of the `planner.slice_target` option.   
- Generating statistics on large data sets can consume time and resources, such as memory and CPU. ANALYZE TABLE can compute statistics on a sample (subset of the data indicated as a percentage) to limit the amount of resources needed for computation. Drill still scans the entire data set, but only computes on the rows selected for sampling. Rows are randomly selected for the sample. Note that the quality of statistics increases with the sample size.    
 
## Queries that Benefit from Statistics
Typically, the types of queries that benefit from statistics are those that include:

- Grouping  
- Multi-table joins  
- Equality predicates on scalar columns   
- Range predicates (filters) on numeric columns
  
## Histograms

**Note:** Currently, histograms are supported for numeric columns only.  
 
Histograms show the distribution of data to determine if data is skewed or normally distributed. Histogram statistics improve the selectivity estimates used by the optimizer to create the most efficient query plans possible. Histogram statistics are useful for range predicates to help determine how many rows belong to a particular range.   
 
Running the ANALYZE TABLE statement generates equi-depth histogram statistics on each column in a table. Equi-depth histograms distribute distinct column values across buckets of varying widths, with all buckets having approximately the same number of rows. The fixed number of rows per bucket is predetermined by `ceil(number_rows/n)`, where `n` is the number of buckets. The number of distinct values in each bucket depends on the distribution of the values in a column. Equi-depth histograms are better suited for skewed data because the more frequent column values have their own bucket or span more than one bucket instead of being lumped together with less frequent values.
 
The following diagram shows the column values on the horizontal axis and the individual frequencies (dark blue) and total frequency of a bucket (light blue). In this example, the total number of rows = 64, hence the number of rows per bucket = `ceil(64/4)  = 16`.  

![](https://i.imgur.com/imchEyg.png)  

The following steps are used to determine bucket boundaries:  
1. Determine the number of rows per bucket: ceil(N/m) where m = num buckets.  
2. Sort the data on the column.  
3. Determine bucket boundaries: The start of bucket 0  = min(column), then continue adding individual frequencies until the row limit is reached, which is the end point of the bucket. Continue to the next bucket and repeat the process. The same column value can potentially be at the end point of one bucket and the start point of the next bucket. Also, the last bucket could have slightly fewer values than other buckets.  

For the predicate `"WHERE a = 5"`, in the example histogram above, you can see that 5 is in the first bucket, which has a range of [1, 7], Using the ‘continuous variable’ nature of histograms, and assuming a uniform distribution within a bucket, we get 16/7 = 2 (approximately).  This is closer to the actual value of 1.
 
Next, consider the range predicate `"WHERE a > 5 AND a <= 16"`.  The range spans part of bucket [1, 7] and entire buckets [8, 9], [10, 11] and [12, 16].  The total estimate = (7-5)/7 * 16 + 16 + 16 + 16 = 53 (approximately).  The actual count is 59.

**Viewing Histogram Statistics for a Column**  
Histogram statistics are generated for each column, as shown:  

	qhistogram":{"category":"numeric-equi-depth","numRowsPerBucket":150,"buckets":[0.0,2.0,4.0,7.0,9.0,12.0,15.199999999999978,17.0,19.0,22.0,24.0]

In this example, there are 10 buckets. Each bucket contains 150 rows, which is calculated as the number of rows (1500)/number of buckets (10). The list of numbers for the “buckets” property indicates bucket boundaries, with the first bucket starting at 0.0 and ending at 2.0. The end of the first bucket is the start point for the second bucket, such that the second bucket starts at 2.0 and ends at 4.0, and so on for the remainder of the buckets. 
  

## Limitations  

- Drill does not cache statistics. 
- ANALYZE TABLE runs only on directory-based Parquet tables. 
- ANALYZE TABLE cannot do the following:  
	- compute statistics on schema-less file formats, such as text and CSV
	- provide up-to-date statistics for operational data due to potential mismatches that can occur between operational updates and manually running ANALYZE TABLE  
- Running the ANALYZE TABLE statement against multiple files in which some of the files have null values and others have no null values may return the following generic Drill error, which is not specific to the ANALYZE command:  
 
		Error: SYSTEM ERROR: IllegalStateException: Failure while reading vector. 
		 Expected vector class of org.apache.drill.exec.vector.NullableBigIntVector
		but was holding vector class org.apache.drill.exec.vector.IntVector, field= [`o_custkey` (INT:REQUIRED)] 
 
		//If you encounter this error, run the ANALYZE TABLE statement on each file with null values individually instead of running the statement against all the files at once.  

-   Running the ANALYZE TABLE statement creates the stats file, which changes the directory timestamp. The change of the timestamp automatically  triggers the REFRESH TABLE METADATA command, even when the underlying data has not changed.  

## EXAMPLES  

These examples use a schema, `dfs.drilltestdir`, which points to the `/drill/testdata` directory in the MapR File System. The `/drill/testdata` directory has the following subdirectories: 

    /drill/testdata/table_stats/Tpch0.01/parquet

The `/parquet`directory contains a table named “customer.”

Switch schema to `dfs.drilltestdir`:
 
	use dfs.drilltestdir;
	+------+----------------------------------------------+
	|  ok  |               	summary                	      |
	+------+----------------------------------------------+
	| true | Default schema changed to [dfs.drilltestdir] |
	+------+----------------------------------------------+
 
The following query shows the columns and types of data in the “customer” table:  

	apache drill (dfs.drilltestdir)> select * from `table_stats/Tpch0.01/parquet/customer` limit 2;
	+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+-----------------------------------------------------------------+
	| c_custkey |       c_name       |           c_address            | c_nationkey |     c_phone     | c_acctbal | c_mktsegment |                            c_comment                            |
	+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+-----------------------------------------------------------------+
	| 1         | Customer#000000001 | IVhzIApeRb ot,c,E              | 15          | 25-989-741-2988 | 711.56    | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e  |
	| 2         | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak | 13          | 23-768-687-3665 | 121.65    | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref |
	+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+-----------------------------------------------------------------+

 
###Enabling Statistics for Query Planning
You can run the ANALYZE TABLE statement at any time to compute statistics; however, you must enable the following option if you want Drill to use statistics during query planning:
 
	set `planner.statistics.use`=true;
	+------+---------------------------------+
	|  ok  |             summary         	 |
	+------+---------------------------------+
	| true | planner.statistics.use updated. |
	+------+---------------------------------+
 
###Computing Statistics
You can compute statistics on directories with Parquet data or on Parquet tables.
 
You can run the ANALYZE TABLE statement on a subset of columns to generate statistics for those columns only, as shown:
 
	analyze table `table_stats/Tpch0.01/parquet/customer` compute statistics (c_custkey, c_nationkey, c_acctbal);
	+----------+---------------------------+
	| Fragment | Number of records written |
	+----------+---------------------------+
	| 0_0      | 3                         |
	+----------+---------------------------+
 
Or, you can run the ANALYZE TABLE statement on the entire table/directory if you want statistics generated for all the columns:
 
	analyze table `table_stats/Tpch0.01/parquet/customer` compute statistics;
	+----------+---------------------------+
	| Fragment | Number of records written |
	+----------+---------------------------+
	| 0_0      | 8                         |
	+----------+---------------------------+


 
###Computing Statistics on a SAMPLE
You can also run ANALYZE TABLE on a percentage of the data using the SAMPLE command, as shown:
 
	ANALYZE TABLE `table_stats/Tpch0.01/parquet/customer` COMPUTE STATISTICS SAMPLE 50 PERCENT;
	+----------+---------------------------+
	| Fragment | Number of records written |
	+----------+---------------------------+
	| 0_0      | 8                         |
	+----------+---------------------------+

 
###Storing Statistics
When you generate statistics, a statistics directory (`.stats.drill`) is created with a JSON file that contains the statistical data.
 
For tables, the `.stats.drill` directory is nested within the table directory. For example, if you ran ANALYZE TABLE against a table named “customer,” you could access the statistic file in `/customer/.stats.drill`. The JSON file is stored in the `.stats.drill` directory.
 
For directories, a new directory is written with the same name as the directory on which you ran ANALYZE TABLE, appended by `.stats.drill`. For example, if you ran ANALYZE TABLE against a directory named “customer,” you could access the JSON statistics file in the new `customer.stats.drill` directory.
 
You can query the statistics file to see the statistics generated for each column, as shown in the following two examples:

 
	select * from `table_stats/Tpch0.01/parquet/customer/.stats.drill`;
	+--------------------+----------------------------------------------------------------------------------+
	| statistics_version |                                   directories                                    |
	+--------------------+----------------------------------------------------------------------------------+
	| v1                 | [{"computed":"2019-04-30","columns":[{"column":"`c_custkey`","majortype":{"type":"INT","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":1500,"avgwidth":4.0,"histogram":{"category":"numeric-equi-depth","numRowsPerBucket":150,"buckets":[2.0,149.0,299.0,450.99999999999994,599.0,749.0,900.9999999999999,1049.0,1199.0,1349.0,1500.0]}},{"column":"`c_name`","majortype":{"type":"VARCHAR","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":1500,"avgwidth":18.0,"histogram":{"buckets":[]}},{"column":"`c_address`","majortype":{"type":"VARCHAR","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":1500,"avgwidth":24.726666666666667,"histogram":{"buckets":[]}},{"column":"`c_nationkey`","majortype":{"type":"INT","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":25,"avgwidth":4.0,"histogram":{"category":"numeric-equi-depth","numRowsPerBucket":150,"buckets":[0.0,2.0,4.0,7.0,9.0,12.0,15.199999999999978,17.0,19.0,22.0,24.0]}},{"column":"`c_phone`","majortype":{"type":"VARCHAR","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":1500,"avgwidth":15.0,"histogram":{"buckets":[]}},{"column":"`c_acctbal`","majortype":{"type":"FLOAT8","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":1499,"avgwidth":8.0,"histogram":{"category":"numeric-equi-depth","numRowsPerBucket":150,"buckets":[-986.9599781036377,70.38335235292713,1315.592873527358,2308.3286817409094,3224.126201901585,4309.92251900211,5536.811312470584,6568.307274272932,7763.991209015995,8865.61001733318,9987.710018221289]}},{"column":"`c_mktsegment`","majortype":{"type":"VARCHAR","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":5,"avgwidth":8.976666666666667,"histogram":{"buckets":[]}},{"column":"`c_comment`","majortype":{"type":"VARCHAR","mode":"REQUIRED"},"schema":1.0,"rowcount":1500.0,"nonnullrowcount":1500.0,"ndv":1500,"avgwidth":73.2,"histogram":{"buckets":[]}}]}] |
	+--------------------+--------------------------------------------------------------------------------------+  

	SELECT t.directories.columns[0].ndv as ndv, t.directories.columns[0].rowcount as rc, t.directories.columns[0].nonnullrowcount AS nnrc, t.directories.columns[0].histogram as histogram FROM `table_stats/Tpch0.01/parquet/customer/.stats.drill` t;
	+------+--------+--------+----------------------------------------------------------------------------------+
	| ndv  |   rc   |  nnrc  |                                    histogram                                     |
	+------+--------+--------+----------------------------------------------------------------------------------+
	| 1500 | 1500.0 | 1500.0 | {"category":"numeric-equi-depth","numRowsPerBucket":150,"buckets":[2.0,149.0,299.0,450.99999999999994,599.0,749.0,900.9999999999999,1049.0,1199.0,1349.0,1500.0]}             |
	+------+--------+--------+----------------------------------------------------------------------------------+




###Dropping Statistics
If you want to compute statistics on a table or directory that you have already run the ANALYZE TABLE statement against, you must first drop the statistics before you can run ANALYZE TABLE statement on the table again.
 
The following example demonstrates how to drop statistics on a table:
 
	DROP TABLE `table_stats/Tpch0.01/parquet/customer/.stats.drill`;
	+------+--------------------------------------------------------------------+
	|  ok  |                              summary                               |
	+------+--------------------------------------------------------------------+
	| true | Table [table_stats/Tpch0.01/parquet/customer/.stats.drill] dropped |
	+------+--------------------------------------------------------------------+


The following example demonstrates how to drop statistics on a directory, assuming that “customer” is a directory that contains Parquet files:
 
	DROP TABLE `table_stats/Tpch0.01/parquet/customer.stats.drill`;
	+-------+------------------------------------+
	|  ok   | 	         summary     	         |
	+-------+------------------------------------+
	| true  | Table [customer.stats.drill] dropped|
	+-------+------------------------------------+
 
When you drop statistics, the statistics directory no longer exists for the table:

	select * from `table_stats/Tpch0.01/parquet/customer/.stats.drill`;

	Error: VALIDATION ERROR: From line 1, column 15 to line 1, column 66: Object 'table_stats/Tpch0.01/parquet/customer/.stats.drill' not found  
	[Error Id: 886003ca-c64f-4e7d-b4c5-26ee1ca617b8 ] (state=,code=0)


## Troubleshooting  

Typical errors you may get when running ANALYZE TABLE result from running the statement against an individual file or against a data source other than Parquet, as shown in the following examples:

**Running ANALYZE TABLE on a file.**  
  
	ANALYZE TABLE `/parquet/nation.parquet` COMPUTE STATISTICS;
	+--------+----------------------------------------------------------------------------------+
	|   ok   |                                     summary                                      |
	+--------+----------------------------------------------------------------------------------+
	| false  | Table /parquet/nation.parquet is not supported by ANALYZE. Support is currently limited to directory-based Parquet tables. |
	+--------+----------------------------------------------------------------------------------+


**Running ANALYZE TABLE on a data source other than Parquet.**

	ANALYZE TABLE nation1_json COMPUTE STATISTICS;
	+--------+----------------------------------------------------------------------------------+
	|   ok   |                                     summary                                      |
	+--------+----------------------------------------------------------------------------------+
	| false  | Table nation1_json is not supported by ANALYZE. Support is currently limited to directory-based Parquet tables. |
	+--------+----------------------------------------------------------------------------------+








  






