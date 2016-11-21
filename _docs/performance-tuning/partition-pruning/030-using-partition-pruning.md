---
title: "How to Partition Data"
date: 2016-11-21 22:14:42 UTC
parent: "Partition Pruning"
--- 

In Drill 1.1.0 and later, if the data source is Parquet, no data organization tasks are required to take advantage of partition pruning. To partition and query Parquet files generated from other tools, use Drill to read and rewrite the files and metadata using the CTAS command with the [PARTITION BY]({{site.baseurl}}/docs/partition-by-clause/) clause in the CTAS statement. 

The Parquet writer first sorts data by the partition keys, and then creates a new file when it encounters a new value for the partition columns. During partitioning, Drill creates separate files, but not separate directories, for different partitions. Each file contains exactly one partition value, but there can be multiple files for the same partition value. 

Partition pruning uses the Parquet column statistics to determine which columns to use to prune. 

Unlike using the Drill 1.0 partitioning, no view query is subsequently required, nor is it necessary to use the [dir* variables]({{site.baseurl}}/docs/querying-directories) after you use the PARTITION BY clause in a CTAS statement. 

## Drill 1.0 Partitioning

Drill 1.0 does not support the PARTITION BY clause of the CTAS command supported by later versions. Partitioning Drill 1.0-generated data involves performing the following steps.   
 
1. Devise a logical way to store the data in a hierarchy of directories. 
2. Use CTAS to create Parquet files from the original data, specifying filter conditions.
3. Move the files into directories in the hierarchy. 

After partitioning the data, you need to create a view of the partitioned data to query the data. You can use the [dir* variables]({{site.baseurl}}/docs/querying-directories) in queries to refer to subdirectories in your workspace path.
 
### Drill 1.0 Partitioning Example

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
       


