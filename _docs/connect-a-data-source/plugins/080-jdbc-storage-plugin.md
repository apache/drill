---
title: "JDBC Storage Plugin"
parent: "Connect a Data Source"
---
Drill 1.2 includes a JDBC storage plugin for connecting to data sources such as MySQL over JDBC. 

## Example MySQL Setup
To configure the JDBC storage plugin for MySQL, set up MySQL and the compatible MySQL driver:

1. Download and install a current version MySQL.  
2. Set up a MySQL password for the user account you plan to use.
3. Download and unpack the compatible JDBC driver.  
4. Put a copy of the driver JAR in the following location:  
   `<drill_installation_directory>/jars/3rdparty`  

## JDBC Storage Plugin Configuration

Drill communicates with MySQL through the JDBC driver using the configuration that you specify in the Web Console or through the [REST API]({{site.baseurl}}/docs/plugin-configuration-basics/#storage-plugin-rest-api).  

{% include startnote.html %}Verify that MySQL is running and the MySQL driver is in place before you configure the JDBC storage plugin.{% include endnote.html %}  

To configure the JDBC storage plugin:

1. To save your storage plugin configurations from one session to the next, set the following option in the `drill-override.conf` file if you are running Drill in embedded mode.

    `drill.exec.sys.store.provider.local.path = "/mypath"`

2. [Start the Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/).  
3. [Start the Web Console]({{site.baseurl}}/docs/starting-the-web-console/).  
4. On the Storage tab, enter a name in **New Storage Plugin**. For example, enter myplugin.
   Each configuration registered with Drill must have a distinct
name. Names are case-sensitive.  

    {% include startnote.html %}The URL differs depending on your installation and configuration.{% include endnote.html %}  
5. Click **Create**.  
6. In Configuration, set the required properties using JSON formatting as shown in the following example. Change the properties to match your environment.  

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

For information about related, unresolved issues in Drill 1.2, see the [release notes]({{site.baseurl}}/docs/apache-drill-1-2-0-release-notes/#important-unresolved-issues).