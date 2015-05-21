---
title: "Partition Pruning"
parent: "Performance Tuning"
--- 

Partition pruning is a performance optimization that limits the number of files and partitions that Drill reads when querying file systems and Hive tables. When you partition data, Drill only reads a subset of the files that reside in a file system or a subset of the partitions in a Hive table when a query matches certain filter criteria.
 
The query planner in Drill evaluates the filters as part of a Filter operator. If no partition filters are present, the underlying Scan operator reads all files in all directories and then sends the data to operators downstream, such as Filter. When partition filters are present, the query planner determines if it can push the filters down to the Scan such that the Scan only reads the directories that match the partition filters, thus reducing disk I/O.

## Determining a Partitioning Scheme  

You can organize your data in such a way that maximizes partition pruning in Drill to optimize performance. Currently, you must partition data manually for a query to take advantage of partition pruning in Drill.
 
Partitioning data requires you to determine a partitioning scheme, or a logical way to store the data in a hierarchy of directories. You can then use CTAS to create Parquet files from the original data, specifying filter conditions, and then move the files into the correlating directories in the hierarchy. Once you have partitioned the data, you can create and query views on the data.
 
### Partitioning Example  

If you have several text files with log data which span multiple years, and you want to partition the data by year and quarter, you could create the following hierarchy of directories:  
       
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

Once the directory structure is in place, run CTAS with a filter condition in the year and quarter for Q1 1994.
 
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
       
