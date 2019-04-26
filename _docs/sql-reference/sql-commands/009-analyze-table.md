---
title: "ANALYZE TABLE"
date: 2019-04-23
parent: "SQL Commands"
---  

Starting in Drill 1.16, Drill supports the ANALYZE TABLE statement. The ANALYZE TABLE statement computes statistics on Parquet data stored in tables and directories. ANALYZE TABLE writes statistics to a JSON file in the `.stats.drill` directory, for example `/user/table1/.stats.drill/0_0.json`. The optimizer in Drill uses these statistics to estimate filter, aggregation, and join cardinalities to create more efficient query plans. 

You can run the ANALYZE TABLE statement to calculate statistics for tables, columns, and directories with Parquet data; however, Drill will not use the statistics for query planning unless you enable the `planner.statistics.use` option, as shown:  

	SET `planner.statistics.use` = true;

Alternatively, you can enable the option in the Drill Web UI at `http://<drill-hostname-or-ip>:8047/options`.

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

If you drop a table on which you have run ANALYZE TABLE, the statistics are automatically removed with the table:  

	DROP TABLE [IF EXISTS] [workspace.]name  

If you want to remove statistics for a table (and keep the table), you must remove the directory in which Drill stores the statistics:  

	DROP TABLE [IF EXISTS] [workspace.]name/.stats.drill  

If you have already issued the ANALYZE TABLE statement against specific columns, a table, or directory, you must run the DROP TABLE statement with `/.stats.drill` before you can successfully run the ANALYZE TABLE statement against the data source again:  

	DROP TABLE dfs.samples.`nation1/.stats.drill`;

Note that `/.stats.drill` is the directory to which the JSON file with statistics is written.   

## Usage Notes  
- The ANALYZE TABLE statement can compute statistics for Parquet data stored in tables, columns, and directories.  
- The user running the ANALYZE TABLE statement must have read and write permissions on the data source.  
- The optimizer in Drill computes the following types of statistics for each column: 
	- Rowcount (total number of entries in the table)  
	- Nonnullrowcount (total number of non-null entries in the table)  
	- NDV (total distinct values in the table)  
	- Avgwidth (average width of columns/average number of characters in a column)   
	- Majortype (data type of the column values)  
	- Histogram (represents the frequency distribution of values (numeric data) in a column; designed for estimations on data with skewed distribution; sorts data into “buckets” such that each bucket contains the same number of rows determined by ceiling(num_rows/n) where n is the number of buckets; the number of distinct values in each bucket depends on the distribution of the column's values)  

- ANALYZE TABLE can compute statistics on nested scalar columns; however, you must explicitly state the columns, for example:  

		ANALYZE TABLE employee_table COMPUTE STATISTICS (name.firstname, name.lastname);  
- ANALYZE TABLE can compute statistics at the root directory level, but not at the partition level.  
- Drill does not compute statistics for complex types (maps, arrays).   

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
- Generating statistics on large data sets can unnecessarily consume time and resources, such as memory and CPU. ANALYZE TABLE can compute statistics on a sample (subset of the data indicated as a percentage) to limit the amount of resources needed for computation. Drill still scans the entire data set, but only computes on the rows selected for sampling. Rows are randomly selected for the sample. Note that the quality of statistics increases with the sample size.  

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

-  Running the ANALYZE TABLE statement against a table with a metadata cache file inadvertently updates the timestamp on the metadata cache file, which automatically triggers the REFRESH TABLE METADATA command.  

## EXAMPLES  

These examples use a schema, `dfs.samples`, which points to the `/home` directory. The `/home` directory contains a subdirectory, `parquet`, which contains the `nation.parquet` and `region.parquet` files. You can access these Parquet files in the `sample-data` directory of your Drill installation.  

	[root@doc23 parquet]# pwd
	/home/parquet
	
	[root@doc23 parquet]# ls
	nation.parquet  region.parquet  

Change schemas to use `dfs.samples`:

	use dfs.samples;
	+-------+------------------------------------------+
	|  ok   |                 summary                  |
	+-------+------------------------------------------+
	| true  | Default schema changed to [dfs.samples]  |
	+-------+------------------------------------------+  

### Enabling Statistics for Query Planning 

You can run the ANALYZE TABLE statement at any time to compute statistics; however, you must enable the following option if you want Drill to use statistics during query planning:

	set `planner.statistics.use`=true;
	+-------+----------------------------------+
	|  ok   |             summary              |
	+-------+----------------------------------+
	| true  | planner.statistics.use updated.  |
	+-------+----------------------------------+  

### Computing Statistics on a Directory 

If you want to compute statistics for all Parquet data in a directory, you can run the ANALYZE TABLE statement against the directory, as shown:

	ANALYZE TABLE `/parquet` COMPUTE STATISTICS;
	+-----------+----------------------------+
	| Fragment  | Number of records written  |
	+-----------+----------------------------+
	| 0_0       | 4                          |
	+-----------+----------------------------+

### Computing Statistics on a Table 

You can create a table from the data in the `nation.parquet` file, as shown:

	CREATE TABLE nation1 AS SELECT * from `parquet/nation.parquet`;
	+-----------+----------------------------+
	| Fragment  | Number of records written  |
	+-----------+----------------------------+
	| 0_0       | 25                         |
	+-----------+----------------------------+

Drill writes the table to the `/home` directory, which is where the `dfs.samples` workspace points: 

	[root@doc23 home]# ls
	nation1  parquet  

Changing to the `nation1` directory, you can see that the table is written as a parquet file:  

	[root@doc23 home]# cd nation1
	[root@doc23 nation1]# ls
	0_0_0.parquet

You can run the ANALYZE TABLE statement on a subset of columns in the table to generate statistics for those columns only, as shown:

	ANALYZE TABLE dfs.samples.nation1 COMPUTE STATISTICS (N_NATIONKEY, N_REGIONKEY);
	+-----------+----------------------------+
	| Fragment  | Number of records written  |
	+-----------+----------------------------+
	| 0_0       | 2                          |
	+-----------+----------------------------+

Or, you can run the ANALYZE TABLE statement on the entire table if you want statistics generated for all columns in the table:

	ANALYZE TABLE dfs.samples.nation1 COMPUTE STATISTICS;
	+-----------+----------------------------+
	| Fragment  | Number of records written  |
	+-----------+----------------------------+
	| 0_0       | 4                          |
	+-----------+----------------------------+  

### Computing Statistics on a SAMPLE
You can also run ANALYZE TABLE on a percentage of the data in a table using the SAMPLE command, as shown:

	ANALYZE TABLE dfs.samples.nation1 COMPUTE STATISTICS SAMPLE 50 PERCENT;
	+-----------+----------------------------+
	| Fragment  | Number of records written  |
	+-----------+----------------------------+
	| 0_0       | 4                          |
	+-----------+----------------------------+  

### Storing Statistics
When you generate statistics, a statistics directory (`.stats.drill`) is created with a JSON file that contains the statistical data.

For tables, the `.stats.drill` directory is nested within the table directory. For example, if you ran ANALYZE TABLE against a table named “nation1,” you could access the statistic file in:  
	
	[root@doc23 home]# cd nation1/.stats.drill
	[root@doc23 .stats.drill]# ls
	0_0.json

For directories, a new directory is written with the same name as the directory on which you ran ANALYZE TABLE and appended by `.stats.drill`. For example, if you ran ANALYZE TABLE against a directory named “parquet,” you could access the statistic file in:

	[root@doc23 home]# cd parquet.stats.drill
	[root@doc23 parquet.stats.drill]# ls
	0_0.json

You can query the statistics file, as shown in the following two examples:

	SELECT * FROM dfs.samples.`parquet.stats.drill`;
	+--------------------+----------------------------------------------------------------------------------+
	| statistics_version |                                   directories                                    |
	+--------------------+----------------------------------------------------------------------------------+
	| v1                 | [{"computed":"2019-04-23","columns":[{"column":"`R_REGIONKEY`","majortype":{"type":"BIGINT","mode":"REQUIRED"},"schema":1.0,"rowcount":5.0,"nonnullrowcount":5.0,"ndv":5,"avgwidth":8.0,"histogram":{"category":"numeric-equi-depth","numRowsPerBucket":1,"buckets":[1.0,0.0,0.0,2.9999999999999996,2.0,4.0]}},{"column":"`R_NAME`","majortype":{"type":"VARCHAR","mode":"REQUIRED"},"schema":1.0,"rowcount":5.0,"nonnullrowcount":5.0,"ndv":5,"avgwidth":6.8,"histogram":{"buckets":[]}},{"column":"`R_COMMENT`","majortype":{"type":"VARCHAR","mode":"REQUIRED"},"schema":1.0,"rowcount":5.0,"nonnullrowcount":5.0,"ndv":5,"avgwidth":20.0,"histogram":{"buckets":[]}}]}] |
	+--------------------+----------------------------------------------------------------------------------+



	SELECT t.directories.columns[0].ndv as ndv, t.directories.columns[0].rowcount as rc, t.directories.columns[0].non                                                                                               nullrowcount AS nnrc, t.directories.columns[0].histogram as histogram FROM dfs.samples.`parquet.stats.drill` t;
	+-----+-----+------+----------------------------------------------------------------------------------+
	| ndv | rc  | nnrc |                                    histogram                                     |
	+-----+-----+------+----------------------------------------------------------------------------------+
	| 5   | 5.0 | 5.0  | {"category":"numeric-equi-depth","numRowsPerBucket":1,"buckets":[1.0,0.0,0.0,2.9999999999999996,2.0,4.0]} |
	+-----+-----+------+----------------------------------------------------------------------------------+  

### Dropping Statistics 

If you want to compute statistics on a table or directory that you have already run the ANALYZE TABLE statement against, you must first drop the statistics before you can run ANALYZE TABLE statement on the table again.

The following example demonstrates how to drop statistics on a table:

	DROP TABLE dfs.samples.`parquet/.stats.drill`;
	+-------+-------------------------------------+
	|  ok   |               summary               |
	+-------+-------------------------------------+
	| true  | Table [parquet/.stats.drill] dropped  |
	+-------+-------------------------------------+

The following example demonstrates how to drop statistics on a directory:

	DROP TABLE dfs.samples.`/parquet.stats.drill`;
	+-------+------------------------------------+
	|  ok   |              summary               |
	+-------+------------------------------------+
	| true  | Table [parquet.stats.drill] dropped  |
	+-------+------------------------------------+

When you drop statistics, the statistics directory no longer exists for the table: 

	[root@doc23 home]# cd parquet/.stats.drill
	-bash: cd: parquet/.stats.drill: No such file or directory
	
	SELECT * FROM dfs.samples.`parquet/.stats.drill`;
	Error: VALIDATION ERROR: From line 1, column 15 to line 1, column 17: Object 'parquet/.stats.drill' not found within 'dfs.samples'
	[Error Id: 0b9a0c35-f058-4e0a-91d5-034d095393d7 on doc23.lab:31010] (state=,code=0)  

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








  






