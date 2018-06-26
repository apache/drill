---
title: "Querying Hive"
date: 2018-06-26 00:42:18 UTC
parent: "Query Data"
---
This is a simple exercise that provides steps for creating a Hive table and
inserting data that you can query using Drill. Before you perform the steps,
download the [customers.csv](http://doc.mapr.com/download/attachments/28868943/customers.csv?version=1&modificationDate=1426874930765&api=v2) file.  

{% include startnote.html %}Drill 1.8 implements the IF EXISTS parameter for the DROP TABLE and DROP VIEW commands, making IF a reserved word in Drill. As a result, you must include backticks around the Hive \``IF`` conditional function when you use it in a query on Hive tables. Alternatively, you can use the CASE statement instead of the IF function.{% include endnote.html %}

To create a Hive table and query it with Drill, complete the following steps:

  1. Issue the following command to start the Hive shell:
  
      `hive`
  2. Issue the following command from the Hive shell create a table schema:
  
        hive> create table customers(FirstName string, LastName string, Company string, Address string, City string, County string, State string, Zip string, Phone string, Fax string, Email string, Web string) row format delimited fields terminated by ',' stored as textfile;
  3. Issue the following command to load the customer data into the customers table:  

        hive> load data local inpath '/<directory path>/customers.csv' overwrite into table customers;
  4. Issue `quit` or `exit` to leave the Hive shell.
  5. Start the Drill shell. 
  6. Issue the following query to Drill to get the first and last names of the first ten customers in the Hive table:  

        0: jdbc:drill:schema=hiveremote> SELECT firstname,lastname FROM hiveremote.`customers` limit 10;`

     The query returns the following results:
     
        +------------+------------+
        | firstname  |  lastname  |
        +------------+------------+
        | Essie      | Vaill      |
        | Cruz       | Roudabush  |
        | Billie     | Tinnes     |
        | Zackary    | Mockus     |
        | Rosemarie  | Fifield    |
        | Bernard    | Laboy      |
        | Sue        | Haakinson  |
        | Valerie    | Pou        |
        | Lashawn    | Hasty      |
        | Marianne   | Earman     |
        +------------+------------+
        10 rows selected (1.5 seconds)
        0: jdbc:drill:schema=hiveremote>

## Optimizing Reads of Parquet-Backed Tables

Use the `store.hive.parquet.optimize_scan_with_native_reader` option to optimize reads of Parquet-backed external tables from Hive. When set to TRUE, this option uses Drill native readers instead of the Hive Serde interface, resulting in more performant queries of Parquet-backed external tables. (Drill 1.2 and later)

Set the `store.hive.parquet.optimize_scan_with_native_reader` option as described in the section, ["Planning and Execution Options"]({{site.baseurl}}/docs/planning-and-execution-options/).
