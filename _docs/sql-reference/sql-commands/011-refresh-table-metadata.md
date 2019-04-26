---
title: "REFRESH TABLE METADATA"
date: 2019-04-23
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
- Drill traverses directories for Parquet files and gathers the metadata from the footer of the files. Drill stores the collected metadata in a metadata cache file, `.drill.parquet_metadata`, at each directory level.  
- The metadata cache file stores metadata for files in that directory, as well as the metadata for the files in the subdirectories.  
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
Currently, Drill does not support runtime rowgroup pruning. 

<!--
## Examples  
These examples use a schema, `dfs.samples`, which points to the `/home` directory. The `/home` directory contains a subdirectory, `parquet`, which
contains the `nation.parquet` and a subdirectory, `dir1` with the `region.parquet` file. You can access the `nation.parquet` and `region.parquet` Parquet files in the `sample-data` directory of your Drill installation.  

	[root@doc23 dir1]# pwd
	/home/parquet/dir1
	 
	[root@doc23 parquet]# ls
	dir1  nation.parquet
	 
	[root@doc23 dir1]# ls
	region.parquet  

Change schemas to use `dfs.samples`:
 
	use dfs.samples;
	+-------+------------------------------------------+
	|  ok   |                 summary        	      |
	+-------+------------------------------------------+
	| true  | Default schema changed to [dfs.samples]  |
	+-------+------------------------------------------+  

### Running REFRESH TABLE METADATA on a Directory  
Running the REFRESH TABLE METADATA command on the `parquet` directory generates metadata cache files at each directory level.  

	REFRESH TABLE METADATA parquet;  
	+-------+---------------------------------------------------+
	|  ok   |                  	summary                  	|
	+-------+---------------------------------------------------+
	| true  | Successfully updated metadata for table parquet.  |
	+-------+---------------------------------------------------+  

When looking at the `parquet` directory and `dir1` subdirectory, you can see that a metadata cache file was created at each level:

	[root@doc23 parquet]# ls -la
	drwxr-xr-x   2 root root   95 Mar 18 17:49 dir1
	-rw-r--r--   1 root root 2642 Mar 18 17:52 .drill.parquet_metadata
	-rw-r--r--   1 root root   32 Mar 18 17:52 ..drill.parquet_metadata.crc
	-rwxr-xr-x   1 root root 1210 Mar 13 13:32 nation.parquet
	 
	[root@doc23 dir1]# ls -la
	-rw-r--r-- 1 root root 1235 Mar 18 17:52 .drill.parquet_metadata
	-rw-r--r-- 1 root root   20 Mar 18 17:52 ..drill.parquet_metadata.crc
	-rwxr-xr-x 1 root root  455 Mar 18 17:41 region.parquet  

The following sections compare the content of the metadata cache file in  the `parquet` and `dir1` directories:  

**Content of the metadata cache file in the directory named `parquet` that contains the nation.parquet file and subdirectory `dir1`.**  


	[root@doc23 parquet]# cat .drill.parquet_metadata
	{
	  "metadata_version" : "3.3",
	  "columnTypeInfo" : {
	    "`N_COMMENT`" : {
	      "name" : [ "N_COMMENT" ],
	      "primitiveType" : "BINARY",
	      "originalType" : "UTF8",
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0
	    },
	    "`N_NATIONKEY`" : {
	      "name" : [ "N_NATIONKEY" ],
	      "primitiveType" : "INT64",
	      "originalType" : null,
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0
	    },
	    "`R_REGIONKEY`" : {
	      "name" : [ "R_REGIONKEY" ],
	      "primitiveType" : "INT64",
	      "originalType" : null,
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0
	    },
	    "`R_COMMENT`" : {
	      "name" : [ "R_COMMENT" ],
	      "primitiveType" : "BINARY",
	      "originalType" : "UTF8",
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0
	    },
	    "`N_REGIONKEY`" : {
	      "name" : [ "N_REGIONKEY" ],
	      "primitiveType" : "INT64",
	      "originalType" : null,
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0
	    },
	    "`R_NAME`" : {
	      "name" : [ "R_NAME" ],
	      "primitiveType" : "BINARY",
	      "originalType" : "UTF8",
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0
	    },
	    "`N_NAME`" : {
	      "name" : [ "N_NAME" ],
	      "primitiveType" : "BINARY",
	      "originalType" : "UTF8",
	      "precision" : 0,
	      "scale" : 0,
	      "repetitionLevel" : 0,
	      "definitionLevel" : 0
	    }
	  },
	  "files" : [ {
	    "path" : "dir1/region.parquet",
	    "length" : 455,
	    "rowGroups" : [ {
	      "start" : 4,
	      "length" : 250,
	      "rowCount" : 5,
	      "hostAffinity" : {
	        "localhost" : 1.0
	      },
	      "columns" : [ ]
	    } ]
	  }, {
	    "path" : "nation.parquet",
	    "length" : 1210,
	    "rowGroups" : [ {
	      "start" : 4,
	      "length" : 944,
	      "rowCount" : 25,
	      "hostAffinity" : {
	        "localhost" : 1.0
	      },
	      "columns" : [ ]
	    } ]
	  } ],
	  "directories" : [ "dir1" ],
	  "drillVersion" : "1.16.0-SNAPSHOT"  

**Content of the directory named `dir1` that contains the `region.parquet` file and no subdirectories.**  

	[root@doc23 dir1]# cat .drill.parquet_metadata
	{
	  "metadata_version" : "3.3",
	  "columnTypeInfo" : {
	   	"`R_REGIONKEY`" : {
	   	"name" : [ "R_REGIONKEY" ],
	   	"primitiveType" : "INT64",
	   	"originalType" : null,
	   	"precision" : 0,
	   	"scale" : 0,
	   	"repetitionLevel" : 0,
	   	"definitionLevel" : 0
	   	},
	   	"`R_COMMENT`" : {
	   	"name" : [ "R_COMMENT" ],
	   	"primitiveType" : "BINARY",
	   	"originalType" : "UTF8",
	   	"precision" : 0,
	   	"scale" : 0,
	   	"repetitionLevel" : 0,
	   	"definitionLevel" : 0
	   	},
	   	"`R_NAME`" : {
	   	"name" : [ "R_NAME" ],
	   	"primitiveType" : "BINARY",
	      "originalType" : "UTF8",
	   	"precision" : 0,
	   	"scale" : 0,
	   	"repetitionLevel" : 0,
	   	"definitionLevel" : 0
	   	}
	  },
	  "files" : [ {
	   	"path" : "region.parquet",
	   	"length" : 455,
	   	"rowGroups" : [ {
	   	"start" : 4,
	   	"length" : 250,
	   	"rowCount" : 5,
	   	"hostAffinity" : {
	   	"localhost" : 1.0
	   	},
	   	"columns" : [ ]
	   	} ]
	  } ],
	  "directories" : [ ],
	  "drillVersion" : "1.16.0-SNAPSHOT"
	}  

### Verifying that the Planner is Using the Metadata Cache File 

When the planner uses metadata cache files, the query plan includes the `usedMetadataFile` flag. You can access the query plan in the Drill Web UI, by clicking on the query in the Profiles page, or by running the EXPLAIN PLAN FOR command, as shown:

	EXPLAIN PLAN FOR SELECT * FROM parquet;  
 
	| 00-00    Screen
	00-01      Project(**=[$0])
	00-02      Scan(table=[[dfs, samples, parquet]], groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=/home/parquet]], selectionRoot=/home/parquet, numFiles=1, numRowGroups=2, usedMetadataFile=true, cacheFileRoot=/home/parquet, columns=[`**`]]])
	|... 

-->	
