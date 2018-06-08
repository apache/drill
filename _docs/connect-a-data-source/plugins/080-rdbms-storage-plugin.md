---
title: "RDBMS Storage Plugin"
date: 2018-06-08 02:01:23 UTC
parent: "Connect a Data Source"
---
Apache Drill supports querying a number of RDBMS instances. This allows you to connect your traditional databases to your Drill cluster so you can have a single view of both your relational and NoSQL datasources in a single system. 

As with any source, Drill supports joins within and between all systems. Drill additionally has powerful pushdown capabilities with RDBMS sources. This includes support to push down join, where, group by, intersect and other SQL operations into a particular RDBMS source (as appropriate).

## Using the RDBMS Storage Plugin

Drill is designed to work with any relational datastore that provides a JDBC driver. Drill is actively tested with Postgres, MySQL, Oracle, MSSQL and Apache Derby. For each system, you will follow three basic steps for setup:

  1. [Install Drill]({{ site.baseurl }}/docs/installing-drill-in-embedded-mode), if you do not already have it installed.
  2. Copy your database's JDBC driver into the jars/3rdparty directory. (You'll need to do this on every node.)  
  3. Restart Drill. See [Starting Drill in Distributed Mode]({{site.baseurl}}/docs/starting-drill-in-distributed-mode/).
  4. Add a new storage configuration to Drill through the Web Console. Example configurations for [Oracle](#Example-Oracle-Configuration), [SQL Server](#Example-SQL-Server-Configuration), [MySQL](#Example-MySQL-Configuration) and [Postgres](#Example-Postgres-Configuration) are provided below.
  
**Example: Working with MySQL**

Drill communicates with MySQL through the JDBC driver using the configuration that you specify in the Web Console or through the [REST API]({{site.baseurl}}/docs/plugin-configuration-basics/#storage-plugin-rest-api).  

{% include startnote.html %}Verify that MySQL is running and the MySQL driver is in place before you configure the JDBC storage plugin.{% include endnote.html %}  

To configure the JDBC storage plugin:

1. [Start the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).  
1. [Start the Web Console]({{site.baseurl}}/docs/starting-the-web-console/).  
1. On the Storage tab, enter a name in **New Storage Plugin**. For example, enter `myplugin`.
   Each configuration registered with Drill must have a distinct name. Names are case-sensitive.  

    {% include startnote.html %}The URL differs depending on your installation and configuration. See the [example configurations](#Example-Configurations) below for examples.{% include endnote.html %}  
1. Click **Create**.  
1. In Configuration, set the required properties using JSON formatting as shown in the following example. Change the properties to match your environment.  

        {
          "type": "jdbc",
          "driver": "com.mysql.jdbc.Driver",
          "url": "jdbc:mysql://localhost:3306",
          "username": "root",
          "password": "mypassword",
          "enabled": true
        }  

7. Click **Create**.  

You can use the performance_schema database, which is installed with MySQL to query your MySQL performance_schema database. Include the names of the storage plugin configuration, the database, and table in dot notation the FROM clause as follows:

      0: jdbc:drill:zk=local> select * from myplugin.performance_schema.accounts;
      +--------+------------+----------------------+--------------------+
      |  USER  |    HOST    | CURRENT_CONNECTIONS  | TOTAL_CONNECTIONS  |
      +--------+------------+----------------------+--------------------+
      | null   | null       | 18                   | 20                 |
      | jdoe   | localhost  | 0                    | 813                |
      | root   | localhost  | 3                    | 5                  |
      +--------+------------+----------------------+--------------------+
      3 rows selected (0.171 seconds)




## Example Configurations

  
**Example Oracle Configuration**

Download and install Oracle's Thin [ojdbc7.12.1.0.2.jar](http://www.oracle.com/technetwork/database/features/jdbc/default-2280470.html) driver and copy it to all nodes in your cluster.

    {
      type: "jdbc",
      enabled: true,
      driver: "oracle.jdbc.OracleDriver",
      url:"jdbc:oracle:thin:user/password@1.2.3.4:1521/ORCL"
    }

**Example SQL Server Configuration**

For SQL Server, Drill has been tested with Microsoft's  [sqljdbc41.4.2.6420.100.jar](https://www.microsoft.com/en-US/download/details.aspx?id=11774) driver. Copy this jar file to all Drillbits. 

{% include startnote.html %}You'll need to provide a database name as part of your JDBC connection string for Drill to correctly expose MSSQL schemas.{% include endnote.html %}

    {
      type: "jdbc",
      enabled: true,
      driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver",
      url:"jdbc:sqlserver://1.2.3.4:1433;databaseName=mydatabase",
      username:"user",
      password:"password"
    }

**Example MySQL Configuration**

For MySQL, Drill has been tested with MySQL's [mysql-connector-java-5.1.37-bin.jar](http://dev.mysql.com/downloads/connector/j/) driver. Copy this to all nodes.

    {
      type: "jdbc",
      enabled: true,
      driver: "com.mysql.jdbc.Driver",
      url:"jdbc:mysql://1.2.3.4",
      username:"user",
      password:"password"
    }  

**Example Postgres Configuration**

For Postgres, Drill has been tested with Postgres's [9.1-901-1.jdbc4](http://central.maven.org/maven2/org/postgresql/postgresql/) driver (any recent driver should work). Copy this driver file to all nodes.

{% include startnote.html %}You'll need to provide a database name as part of your JDBC connection string for Drill to correctly expose Postgres tables.{% include endnote.html %}

    {
      type: "jdbc",
      enabled: true,
      driver: "org.postgresql.Driver",
      url:"jdbc:postgresql://1.2.3.4/mydatabase",
      username:"user",
      password:"password"
    }  

You may need to qualify a table name with a schema name for Drill to return data. For example, when querying a table named ips, you must issue the query against public.ips, as shown in the following example:  

       0: jdbc:drill:zk=local> use pgdb;          
       +-------+-----------------------------------+
       |  ok   |          	summary          	|
       +-------+-----------------------------------+
       | true  | Default schema changed to [pgdb]  |
       +-------+-----------------------------------+
        
       0: jdbc:drill:zk=local> show tables;          
       +---------------+--------------------------+
       | TABLE_SCHEMA  |        TABLE_NAME    	|
       +---------------+--------------------------+
       | pgdb.test 	| ips                  	|
       | pgdb.test 	| pg_aggregate         	|
       | pgdb.test 	| pg_am                	| 
       …  

       0: jdbc:drill:zk=local> select * from public.ips;          
       +-------+----------+
       | ipid  | ipv4dot  |
       +-------+----------+
       | 1 	| 1.2.3.4  |
       | 2 	| 1.2.3.5  |
       +-------+----------+


