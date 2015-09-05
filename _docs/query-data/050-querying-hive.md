---
title: "Querying Hive"
parent: "Query Data"
---
This is a simple exercise that provides steps for creating a Hive table and
inserting data that you can query using Drill. Before you perform the steps,
download the [customers.csv](http://doc.mapr.com/download/attachments/28868943/customers.csv?version=1&modificationDate=1426874930765&api=v2) file.

To create a Hive table and query it with Drill, complete the following steps:

  1. Issue the following command to start the Hive shell:
  
        hive
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

