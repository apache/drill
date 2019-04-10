---
title: "Starting Drill on Windows"
date: 2019-04-10
parent: "Installing Drill in Embedded Mode"
---
Complete the following steps to launch the Drill:

1. Open the Windows command prompt.  
2. Navigate to the Drill installation folder, for example:  
`cd \Users\user1\drill_repo\apache-drill-1.16.0-SNAPSHOT` 
3. Go to the `bin` directory, for example:  
`cd bin`
4. Enter either of the following commands to start Drill:     
	- `sqlline.bat -u "jdbc:drill:zk=local"`  
    - `drill-embedded.bat` (Supported in Drill 1.16 and later.)  

			C:\Users\user1\drill_repo\apache-drill-1.16.0-SNAPSHOT\bin>drill-embedded.bat
		
			DRILL_ARGS - " -u jdbc:drill:zk=local"
			HADOOP_HOME not detected...
			HBASE_HOME not detected...
			Calculating Drill classpath...
			Apache Drill 1.16.0-SNAPSHOT
			"Drill never goes out of style."
			apache drill>

You can run a test query to verify that Drill is running, for example:  

	//Drill's classpath contains sample data, including an employee.json file that you can query. Switch schema to cp, for classpath.  
 
	apache drill>use cp;
	+------+--------------------------------+
	|  ok  |            summary             |
	+------+--------------------------------+
	| true | Default schema changed to [cp] |
	+------+--------------------------------+  

	//Query the employee.json file in the classpath.

	apache drill (cp)>SELECT * FROM cp.`employee.json` LIMIT 1;
	+-------------+--------------+------------+-----------+-------------+----------------+----------+---------------+------------+-----------------------+---------+---------------+-----------------+----------------+--------+-------------------+
	| employee_id |  full_name   | first_name | last_name | position_id | position_title | store_id | department_id | birth_date |       hire_date       | salary  | supervisor_id | education_level | marital_status | gender |  management_role  |
	+-------------+--------------+------------+-----------+-------------+----------------+----------+---------------+------------+-----------------------+---------+---------------+-----------------+----------------+--------+-------------------+
	| 1           | Sheri Nowmer | Sheri      | Nowmer    | 1           | President      | 0        | 1             | 1961-08-26 | 1994-12-01 00:00:00.0 | 80000.0 | 0             | Graduate Degree | S              | F      | Senior Management |
	+-------------+--------------+------------+-----------+-------------+----------------+----------+---------------+------------+-----------------------+---------+---------------+-----------------+----------------+--------+-------------------+


For a short tutorial that you can run in Drill, see [Drill in 10 Minutes]({{ site.baseurl }}/docs/drill-in-10-minutes/#query-sample-data).

Note that you can use the schema option in the **sqlline** command to specify a storage plugin. Specifying the storage plugin when you start up eliminates the need to specify the storage plugin in the query. For example, this command specifies the `dfs` storage plugin:

	sqlline.bat –u "jdbc:drill:zk=local;schema=dfs"

If you start Drill on one network, and then want to use Drill on another network, such as your home network, restart Drill.

## Exiting the Drill Shell

To exit the Drill shell, issue the following command:

	!quit	

