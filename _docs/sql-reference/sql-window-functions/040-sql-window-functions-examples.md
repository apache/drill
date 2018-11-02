---
title: "SQL Window Functions Examples"
date: 2018-11-02
parent: "SQL Window Functions"
---

The window function examples use a view named q1\_sales that was created from a CSV file named emp_sales and stored in a directory on the local file system.
 
The emp_sales.csv file contains the following information:  

       +-----------------+-----------------+------------+--------+
       |    emp_name     |     emp_mgr     | dealer_id  | sales  |
       +-----------------+-----------------+------------+--------+
       | Beverly Lang    | Mike Palomino   | 2          | 16233  |
       | Kameko French   | Mike Palomino   | 2          | 16233  |
       | Ursa George     | Rich Hernandez  | 3          | 15427  |
       | Ferris Brown    | Dan Brodi       | 1          | 19745  |
       | Noel Meyer      | Kari Phelps     | 1          | 19745  |
       | Abel Kim        | Rich Hernandez  | 3          | 12369  |
       | Raphael Hull    | Kari Phelps     | 1          | 8227   |
       | Jack Salazar    | Kari Phelps     | 1          | 9710   |
       | May Stout       | Rich Hernandez  | 3          | 9308   |
       | Haviva Montoya  | Mike Palomino   | 2          | 9308   |
       +-----------------+-----------------+------------+--------+
You can create a CSV file named emp_sales with this data.

Drill was installed locally and a workspace was created in the `dfs` storage plugin configuration for the directory where the emp_sales.csv file is located. See [Installing Drill](https://drill.apache.org/docs/embedded-mode-prerequisites/).
 
If you create a CSV file with the data provided, you can create workspace that points to the directory where you store the emp_sales.csv file. See [Configuring Storage Plugins](https://drill.apache.org/docs/file-system-storage-plugin/).
 
When you run show schemas, Drill lists the workspace that you configured as a schema. In the following example, you can see dfs.emp listed. This is the workspace that points to the directory where the emp_sales.csv file is stored.

       0: jdbc:drill:zk=local> show schemas;
       +---------------------+
       |     SCHEMA_NAME	 |
       +---------------------+
       | INFORMATION_SCHEMA  |
       | cp.default     	 |
       | dfs.default    	 |
       | dfs.emp        	 |
       | dfs.root       	 |
       | dfs.tmp        	 |
       | sys            	 |
       +---------------------+  

You can then run the USE command to change to the schema with the file. All queries are executed against the schema that you use.
 
       0: jdbc:drill:zk=local> use dfs.emp;
       +-------+--------------------------------------+
       |  ok   |               summary           	 |
       +-------+--------------------------------------+
       | true  | Default schema changed to [dfs.emp]  |
       +-------+--------------------------------------+
 
To create the q1_sales view used in the examples, issue the following query with the CREATE VIEW command.

Note: You must use column numbers when querying CSV files. Also, CAST the columns to a specific data type to avoid incorrect implicit casting by Drill. This can affect the accuracy of window function results. In Drill, the column array starts with 0 as the first column.
 
       0: jdbc:drill:zk=local> create view q1_sales as select cast(columns[0] as varchar(30)) as emp_name, cast(columns[1] as varchar(30)) as emp_mgr, cast(columns[2] as int) as dealer_id, cast(columns[3] as int) as sales from `q1_sales.csv`;
       +-------+-----------------------------------------------------------+
       |  ok   |                          summary                     	 |
       +-------+-----------------------------------------------------------+
       | true  | View 'q1_sales' created successfully in 'dfs.emp' schema  |
       +-------+-----------------------------------------------------------+
       1 row selected (0.134 seconds)  

Query the view to verify that all of the data appears correctly:  

       select * from q1_sales; 
       +-----------------+-----------------+------------+--------+
       |    emp_name     |     emp_mgr     | dealer_id  | sales  |
       +-----------------+-----------------+------------+--------+
       | Beverly Lang    | Mike Palomino   | 2          | 16233  |
       | Kameko French   | Mike Palomino   | 2          | 16233  |
       | Ursa George     | Rich Hernandez  | 3          | 15427  |
       | Ferris Brown    | Dan Brodi       | 1          | 19745  |
       | Noel Meyer      | Kari Phelps     | 1          | 19745  |
       | Abel Kim        | Rich Hernandez  | 3          | 12369  |
       | Raphael Hull    | Kari Phelps     | 1          | 8227   |
       | Jack Salazar    | Kari Phelps     | 1          | 9710   |
       | May Stout       | Rich Hernandez  | 3          | 9308   |
       | Haviva Montoya  | Mike Palomino   | 2          | 9308   |
       +-----------------+-----------------+------------+--------+
       10 rows selected (0.112 seconds)  

Now, you can run the window function example queries on your machine.

       
       



                                                                                                                                       
