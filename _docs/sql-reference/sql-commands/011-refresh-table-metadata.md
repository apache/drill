---
title: "REFRESH TABLE METADATA"
date: 2019-05-31
parent: "SQL Commands"
---
Run the REFRESH TABLE METADATA command on Parquet tables and directories to generate a metadata cache file. REFRESH TABLE METADATA collects metadata from the footers of Parquet files and writes the metadata to a metadata file (`.drill.parquet_file_metadata.v4`) and a summary file (`.drill.parquet_summary_metadata.v4`). The planner uses the metadata cache file to prune extraneous data during the query planning phase. Run the REFRESH TABLE METADATA command if planning time is a significant percentage of the total elapsed time of the query.   

Starting in Drill 1.11, Drill stores the paths to Parquet files as relative paths instead of absolute paths. You can move partitioned Parquet directories from one location in the distributed file system to another without issuing the REFRESH TABLE METADATA command to rebuild the Parquet metadata cache files; the metadata remains valid in the new location.  

## Syntax   

The REFRESH TABLE METADATA command supports the following syntax: 

	REFRESH TABLE METADATA  [ COLUMNS ( column1, column2...) | NONE ]  table_path


## Parameters
*COLUMNS*  
Optional. Introduced in Drill 1.16. Use to indicate the columns on which the command should refresh metadata. When omitted, metadata is collected for all the columns.

*column*  
Required when using the COLUMNS clause. The columns for which metadata will be stored.

*NONE*  
Optional. Introduced in Drill 1.16. When used, the REFRESH command does not refresh any metadata; however, a summary file is generated and contains a summary section (ColumnTypeInfo) that lists all the columns with their data types.  

*table_path*  
Required. The name of the table or directory for which Drill will refresh metadata.  

## Related Command  
Run the [EXPLAIN]({{site.baseurl}}/docs/explain/) command to determine the query execution time. If the query execution time is the most significant time factor, running REFRESH TABLE METADATA will not improve query performance.


## Usage Notes  

### Metadata Storage  
-  Drill traverses directories for Parquet files and gathers the metadata from the footer of the files. Drill stores the collected metadata in a metadata cache file, `.drill.parquet_file_metadata.v4`, a summary file, `.drill.parquet_summary_metadata.v4`, and a directories file, `.drill.parquet_metadata_directories` file at each directory level.  
-  Introduced in Drill 1.16, the summary file, `.drill.parquet_summary_metadata.v4`, optimizes planning for certain queries, like COUNT(*) queries, such that the planner can use the summary file instead of the larger metadata cache file.       
- The metadata cache file stores metadata for files in the current directory, as well as the metadata for the files in subdirectories.  
- For each row group in a Parquet file, the metadata cache file stores the column names in the row group and the column statistics, such as the min/max values and null count.  
- If the Parquet data is updated, for example data is added to a file, Drill automatically  refreshes the Parquet metadata when you issue the next query against the Parquet data.  

### Refreshing Columns
- Starting in Drill 1.16, you can run the REFRESH TABLE METADATA command on specific columns. When you run the command on specific columns, metadata is refreshed for the indicated columns only.  
- Refreshing the metadata for sorted or partitioned columns improves the planning time for queries that filter on these columns. The query planner uses the min and max column statistics to determine which data meets the filter criteria. The planner can prune files and row groups that do not meet the filter criteria, which reduces the amount of time it takes the query planner to scan the metadata during query planning. Refreshing column metadata (versus entire tables) also reduces the size of the metadata cache file.  

### Automatic Refresh

- You only have to run the REFRESH TABLE METADATA command against a table once to generate the initial metadata cache file. Thereafter, Drill automatically refreshes stale cache files when you issue queries against the table. An automatic refresh is triggered when data is modified.The query planner uses the timestamp of the cache file and table to determine if the cache file is stale.   
- An automatic refresh updates the metadata cache file the same way the REFRESH TABLE METADATA command did when it was last issued against a table. For example, if you ran REFRESH TABLE METADATA on col1 and col2 in table t1, the next time a query is issued against table t1, the query planner checks the timestamp on the cache file and the table, t1. If the timestamp on the data is later than the timestamp on the metadata cache file, automatic refresh is triggered and metadata is refreshed for col1 and col2. 
- Note that the elapsed time of a query that triggers an automatic refresh can be greater than that of subsequent queries that use the metadata. To avoid this, you can manually run the REFRESH TABLE METADATA command.

### Query Planning and Execution  

- Drill reads the metadata cache file during query planning, which improves the query planning time.  
- Parquet metadata caching has no effect on query execution time. At execution time, Drill reads the files. Metadata caching will not improve query performance if the time it takes to execute the query is greater than the time used to plan a query.  
- Run [EXPLAIN]({{site.baseurl}}/docs/explain/) for the query to determine if query execution time is the most significant time factor for the query. Compare the query execution time to the overall time for the query to determine whether metadata caching would be useful.  

### Not Supported  
- Parquet metadata caching does not benefit queries on Hive tables, HBase tables, or text files. Drill only uses the Hive metastore to query Parquet files when a query is issued against the Hive storage plugin.


## Related Options  
You can set the following options related to the REFRESH TABLE METADATA command at the system or session level with the SET (session level) or ALTER SYSTEM SET (system level) statements, or through the Drill Web UI at `http://<drill-hostname-or-ip>:8047/options`:  

- **planner.store.parquet.rowgroup.filter.pushdown.enabled**    
Enables filter pushdown optimization for Parquet files. Drill reads the file metadata, stored in the footer, to eliminate row groups based on the filter condition. Default is true. (Drill 1.9+)   
- **planner.store.parquet.rowgroup.filter.pushdown.threshold**  
Sets the number of row groups that a table can have. You can increase the threshold if the filter can prune many row groups. However, if this setting is too high, the filter evaluation overhead increases. Base this setting on the data set. Reduce this setting if the planning time is significant or you do not see any benefit at runtime. Default is 10000.  (Drill 1.9+)  

## Limitations


- Drill does not support runtime rowgroup pruning.  
- REFRESH TABLE METADATA does not count null values for decimal, varchar, and interval data types.


## Examples  
These examples use a schema, `dfs.samples`, which points to the `/tmp` directory. The `/tmp` directory contains the following subdirectories and files used in the examples:  

	[root@doc23 parquet1]# pwd
	/tmp/parquet1
	
	[root@doc23 parquet1]# ls
	Parquet
	
	[root@doc23 parquet1]# cd parquet
	
	[root@doc23 parquet]# ls
	nation.parquet  test
	
	[root@doc23 parquet]# cd test
	
	[root@doc23 test]# ls
	nation.parquet

**Note:** You can access the sample `nation.parquet` file in the `sample-data` directory of your Drill installation.

 
Change to the `dfs.samples` schema: 

	use dfs.samples;
	+-------+------------------------------------------+
	|  ok   |                 summary        	      |
	+-------+------------------------------------------+
	| true  | Default schema changed to [dfs.samples]  |
	+-------+------------------------------------------+  

### Running REFRESH TABLE METADATA on a Directory
Running the REFRESH TABLE METADATA command on the “parquet1” directory generates metadata cache files at each directory level.

	apache drill (dfs.samples)> REFRESH TABLE METADATA parquet1;
	+------+---------------------------------------------------+
	|  ok  |                      summary                      |
	+------+---------------------------------------------------+
	| true | Successfully updated metadata for table parquet1. |
	+------+---------------------------------------------------+

When looking at the “parquet1” directory and subdirectories, you can see that a metadata cache and summary (hidden) files were created at each level:

**Note:** The CRC files are Cyclical Redundancy Check checksum files used to verify the data integrity of other files. 

	[root@doc23 parquet1]# ls -la
	total 36
	drwxr-xr-x   3 root root  284 Apr 29 11:46 .
	drwxrwxrwt. 51 root root 8192 Apr 29 11:44 ..
	-rw-r--r--   1 root root 1037 Apr 29 11:46 .drill.parquet_file_metadata.v4
	-rw-r--r--   1 root root   20 Apr 29 11:46 ..drill.parquet_file_metadata.v4.crc
	-rw-r--r--   1 root root   51 Apr 29 11:46 .drill.parquet_metadata_directories
	-rw-r--r--   1 root root   12 Apr 29 11:46 ..drill.parquet_metadata_directories.crc
	-rw-r--r--   1 root root 1334 Apr 29 11:46 .drill.parquet_summary_metadata.v4
	-rw-r--r--   1 root root   20 Apr 29 11:46 ..drill.parquet_summary_metadata.v4.crc
	drwxr-xr-x   3 root root  212 Apr 29 11:30 parquet  
	
	[root@doc23 parquet1]# cd parquet
	[root@doc23 parquet]# ls -la
	total 20
	drwxr-xr-x 3 root root  212 Apr 29 11:30 .
	drwxr-xr-x 3 root root  284 Apr 29 11:46 ..
	-rw-r--r-- 1 root root 1021 Apr 29 11:46 .drill.parquet_file_metadata.v4
	-rw-r--r-- 1 root root   16 Apr 29 11:46 ..drill.parquet_file_metadata.v4.crc
	-rw-r--r-- 1 root root 1315 Apr 29 11:46 .drill.parquet_summary_metadata.v4
	-rw-r--r-- 1 root root   20 Apr 29 11:46 ..drill.parquet_summary_metadata.v4.crc
	-rwxr-xr-x 1 root root 1210 Apr 29 11:23 nation.parquet
	drwxr-xr-x 2 root root  200 Apr 29 11:46 test
	
	[root@doc23 test]# ls -la
	total 20
	drwxr-xr-x 2 root root  200 Apr 29 11:46 .
	drwxr-xr-x 3 root root  212 Apr 29 11:30 ..
	-rw-r--r-- 1 root root  517 Apr 29 11:46 .drill.parquet_file_metadata.v4
	-rw-r--r-- 1 root root   16 Apr 29 11:46 ..drill.parquet_file_metadata.v4.crc
	-rw-r--r-- 1 root root 1308 Apr 29 11:46 .drill.parquet_summary_metadata.v4
	-rw-r--r-- 1 root root   20 Apr 29 11:46 ..drill.parquet_summary_metadata.v4.crc
	-rwxr-xr-x 1 root root 1210 Apr 29 11:23 nation.parquet  

Looking at the `.drill.parquet_file_metadata.v4` file in the `/tmp/parquet1` directory, you can see that the file contains the paths to the Parquet files in the subdirectories, as well as metadata for those files: 

	[root@doc23 parquet1]# cat .drill.parquet_file_metadata.v4
	{
	  "files" : [ {
	    "path" : "parquet/test/nation.parquet",
	    "length" : 1210,
	    "rowGroups" : [ {
	      "start" : 4,
	      "length" : 944,
	      "rowCount" : 25,
	      "hostAffinity" : {
	        "localhost" : 1.0
	      },
	      "columns" : [ {
	        "name" : [ "N_NATIONKEY" ],
	        "nulls" : -1
	      }, {
	        "name" : [ "N_NAME" ],
	        "nulls" : -1
	      }, {
	        "name" : [ "N_REGIONKEY" ],
	        "nulls" : -1
	      }, {
	        "name" : [ "N_COMMENT" ],
	        "nulls" : -1
	      } ]
	    } ]
	  }, {
	    "path" : "parquet/nation.parquet",
	    "length" : 1210,
	    "rowGroups" : [ {
	      "start" : 4,
	      "length" : 944,
	      "rowCount" : 25,
	      "hostAffinity" : {
	        "localhost" : 1.0
	      },
	      "columns" : [ {
	        "name" : [ "N_NATIONKEY" ],
	        "nulls" : -1
	      }, {
	        "name" : [ "N_NAME" ],
	        "nulls" : -1
	      }, {
	        "name" : [ "N_REGIONKEY" ],
	        "nulls" : -1
	      }, {
	        "name" : [ "N_COMMENT" ],
	        "nulls" : -1
	      } ]
	    } ]
	  } ]


Looking at the `.drill.parquet_summary_metadata.v4` file in the `parquet1` directory, you can see information about each of the columns in the files and the list of subdirectories and interesting columns (useful when indicating columns in the REFRESH TABLE METADATA command):  

	[root@doc23 parquet1]# cat .drill.parquet_summary_metadata.v4
	{
	  "columnTypeInfo" : {
	    "`N_COMMENT`" : {
	      "name" : [ "N_COMMENT" ],
	      "primitiveType" : "BINARY",
	      "originalType" : "UTF8",
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0,
	      "totalNullCount" : -1,
	      "isInteresting" : true
	    },
	    "`N_NATIONKEY`" : {
	      "name" : [ "N_NATIONKEY" ],
	      "primitiveType" : "INT64",
	      "originalType" : null,
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0,
	      "totalNullCount" : -1,
	      "isInteresting" : true
	    },
	    "`N_REGIONKEY`" : {
	      "name" : [ "N_REGIONKEY" ],
	      "primitiveType" : "INT64",
	      "originalType" : null,
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0,
	      "totalNullCount" : -1,
	      "isInteresting" : true
	    },
	    "`N_NAME`" : {
	      "name" : [ "N_NAME" ],
	      "primitiveType" : "BINARY",
	      "originalType" : "UTF8",
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0,
	      "totalNullCount" : -1,
	      "isInteresting" : true
	    }
	  },
	  "directories" : [ "parquet/test", "parquet" ],
	  "drillVersion" : "1.16.0-SNAPSHOT",
	  "totalRowCount" : 50,
	  "allColumnsInteresting" : true,
	  "metadata_version" : "4"  

###Verifying that the Planner is Using the Metadata Cache or Summary Files

When the planner uses metadata cache files, the query plan includes the `usedMetadataFile` flag. You can access the query plan in the Drill Web UI, by clicking on the query in the Profiles page, or by running the EXPLAIN PLAN FOR command, as shown:

	apache drill (dfs.samples)> explain plan for select * from parquet1;
	+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
	|                                       text                                       |                                       json                                       |
	+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
	| 00-00    Screen
	00-01      Project(**=[$0])
	00-02        Scan(table=[[dfs, samples, parquet1]], groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=/tmp/parquet1]], selectionRoot=/tmp/parquet1, numFiles=1, numRowGroups=2, usedMetadataFile=true, cacheFileRoot=/tmp/parquet1, columns=[`**`]]])  
	 |   

When you run the EXPLAIN command with a COUNT() query, as shown, you can see that the query planner uses the summary cache file and avoids reading the larger metadata cache file. The query plan includes the `usedMetadataSummaryFile` flag.

	apache drill (dfs.samples)> explain plan for select count(*) from parquet1;
	+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
	|                                       text                                       |                                       json                                       |
	+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------+
	| 00-00    Screen
	00-01      Project(EXPR$0=[$0])
	00-02        DirectScan(groupscan=[files = [file:/tmp/parquet1/.drill.parquet_summary_metadata.v4], numFiles = 1, usedMetadataSummaryFile = true, DynamicPojoRecordReader{records = [[50]]}])
	 | 

	
	




