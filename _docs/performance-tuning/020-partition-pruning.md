---
title: "Partition Pruning"
parent: "Performance Tuning"
--- 

Partition pruning is a performance optimization that limits the number of files and partitions that Drill reads when querying file systems and Hive tables. When you partition data, Drill only reads a subset of the files that reside in a file system or a subset of the partitions in a Hive table when a query matches certain filter criteria.
 
The query planner in Drill performs partition pruning by evaluating the filters. If no partition filters are present, the underlying Scan operator reads all files in all directories and then sends the data to operators, such as Filter, downstream. When partition filters are present, the query planner pushes the filters down to the Scan if possible. The Scan reads only the directories that match the partition filters, thus reducing disk I/O.

## How to Partition Data

You can partition data manually or automatically to take advantage of partition pruning in Drill. In Drill 1.0 and earlier, you need to organize your data in such a way to take advantage of partition pruning. In Drill 1.1.0 and later, if the data source is Parquet, you can partition data automatically using CTAS--no data organization tasks required. 

## Automatic Partitioning
Automatic partitioning in Drill 1.1 and later occurs when you write Parquet data using the [PARTITION BY]({{site.baseurl}}/docs/partition-by-clause/) clause in the CTAS statement. Unlike manual partitioning, no view is required, nor is it necessary to use the [dir* variables]({{site.baseurl}}/docs/querying-directories). The Parquet writer first sorts by the partition keys, and then creates a new file when it encounters a new value for the partition columns.

Automatic partitioning creates separate files, but not separate directories, for different partitions. Each file contains exactly one partition value, but there can be multiple files for the same partition value.

Partition pruning uses the Parquet column statistics to determine which columns to use to prune. 

## Manual Partitioning

Manual partitioning is directory-based. You perform the following steps to manually partition data.   
 
1. Devise a logical way to store the data in a hierarchy of directories. 
2. Use CTAS to create Parquet files from the original data, specifying filter conditions.
3. Move the files into directories in the hierarchy. 

After partitioning the data, you need to create a view of the partitioned data to query the data. You can use the [dir* variables]({{site.baseurl}}/docs/querying-directories) in queries to refer to subdirectories in your workspace path.
 
### Manual Partitioning Example

Suppose you have text files containing several years of log data. To partition the data by year and quarter, create the following hierarchy of directories:  
       
       …/logs/1994/Q1  
       …/logs/1994/Q2  
       …/logs/1994/Q3  
       …/logs/1994/Q4  
       …/logs/1995/Q1  
       …/logs/1995/Q2  
       …/logs/1995/Q3  
       …/logs/1995/Q4  
       …/logs/1996/Q1  
       …/logs/1996/Q2  
       …/logs/1996/Q3  
       …/logs/1996/Q4  

Run the following CTAS statement, filtering on the Q1 1994 data.
 
          CREATE TABLE TT_1994_Q1 
              AS SELECT * FROM <raw table data in text format >
              WHERE columns[1] = 1994 AND columns[2] = 'Q1'
 
This creates a Parquet file with the log data for Q1 1994 in the current workspace.  You can then move the file into the correlating directory, and repeat the process until all of the files are stored in their respective directories.

Now you can define views on the parquet files and query the views.  

       0: jdbc:drill:zk=local> create view vv1 as select `dir0` as `year`, `dir1` as `qtr` from dfs.`/Users/max/data/multilevel/parquet`;
       +------------+------------+
       |     ok     |  summary   |
       +------------+------------+
       | true       | View 'vv1' created successfully in 'dfs.tmp' schema |
       +------------+------------+
       1 row selected (0.16 seconds)  

Query the view to see all of the logs.  

       0: jdbc:drill:zk=local> select * from dfs.tmp.vv1;
       +------------+------------+
       |    year    |    qtr     |
       +------------+------------+
       | 1994       | Q1         |
       | 1994       | Q3         |
       | 1994       | Q3         |
       | 1994       | Q4         |
       | 1994       | Q4         |
       | 1994       | Q4         |
       | 1994       | Q4         |
       | 1995       | Q2         |
       | 1995       | Q2         |
       | 1995       | Q2         |
       | 1995       | Q2         |
       | 1995       | Q4         |
       | 1995       | Q4         |
       | 1995       | Q4         |
       | 1995       | Q4         |
       | 1995       | Q4         |
       | 1995       | Q4         |
       | 1995       | Q4         |
       | 1996       | Q1         |
       | 1996       | Q1         |
       | 1996       | Q1         |
       | 1996       | Q1         |
       | 1996       | Q1         |
       | 1996       | Q2         |
       | 1996       | Q3         |
       | 1996       | Q3         |
       | 1996       | Q3         |
       +------------+------------+
       ...


When you query the view, Drill can apply partition pruning and read only the files and directories required to return query results.

       0: jdbc:drill:zk=local> explain plan for select * from dfs.tmp.vv1 where `year` = 1996 and qtr = 'Q2';
       +------------+------------+
       |    text    |    json    |
       +------------+------------+
       | 00-00    Screen
       00-01      Project(year=[$0], qtr=[$1])
       00-02        Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/Users/maxdata/multilevel/parquet/1996/Q2/orders_96_q2.parquet]], selectionRoot=/Users/max/data/multilevel/parquet, numFiles=1, columns=[`dir0`, `dir1`]]])
       


