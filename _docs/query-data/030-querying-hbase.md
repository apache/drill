---
title: "Querying HBase"
parent: "Query Data"
---
This exercise creates two tables in HBase, students and clicks, that you can query with Drill. You can use the Drill Sandbox to step through the exercise. As an HBase user, you most likely are running Drill in  distributed mode. In this case, the warden starts Drill as a service. If you are not an HBase user and just kicking the tires, you are most likely running Drill in embedded mode. In embedded mode, you need to [start Drill](/docs/starting-stopping-drill/) before performing step 5. On the Drill Sandbox, HBase tables you create will be located in: /mapr/demo.mapr.com/tables

You use the CONVERT_TO and CONVERT_FROM functions to convert binary text to readable output. You use the CAST function to convert the binary INT to readable output in the last step. It is a best practice to use CAST for INT and BIGINT conversions from binary and to use CONVERT_TO and CONVERT_FROM for other conversions.

## Create the HBase tables

To create the HBase tables and start Drill, complete the following
steps:

1. Pipe the following commands to the HBase shell to create students and  clicks tables in HBase:
  
          echo "create 'students','account','address'" | hbase shell
          echo "create 'clicks','clickinfo','iteminfo'" | hbase shell

2. Issue the following command to create a `testdata.txt` file:

      cat > testdata.txt

3. Copy and paste the following `put` commands on the line below the **cat** command. Press Return, and then CTRL Z to close the file.

        put 'students','student1','account:name','Alice'
        put 'students','student1','address:street','123 Ballmer Av'
        put 'students','student1','address:zipcode','12345'
        put 'students','student1','address:state','CA'
        put 'students','student2','account:name','Bob'
        put 'students','student2','address:street','1 Infinite Loop'
        put 'students','student2','address:zipcode','12345'
        put 'students','student2','address:state','CA'
        put 'students','student3','account:name','Frank'
        put 'students','student3','address:street','435 Walker Ct'
        put 'students','student3','address:zipcode','12345'
        put 'students','student3','address:state','CA'
        put 'students','student4','account:name','Mary'
        put 'students','student4','address:street','56 Southern Pkwy'
        put 'students','student4','address:zipcode','12345'
        put 'students','student4','address:state','CA'
        put 'clicks','click1','clickinfo:studentid','student1'
        put 'clicks','click1','clickinfo:url','http://www.google.com'
        put 'clicks','click1','clickinfo:time','2014-01-01 12:01:01.0001'
        put 'clicks','click1','iteminfo:itemtype','image'
        put 'clicks','click1','iteminfo:quantity','1'
        put 'clicks','click2','clickinfo:studentid','student1'
        put 'clicks','click2','clickinfo:url','http://www.amazon.com'
        put 'clicks','click2','clickinfo:time','2014-01-01 01:01:01.0001'
        put 'clicks','click2','iteminfo:itemtype','image'
        put 'clicks','click2','iteminfo:quantity','1'
        put 'clicks','click3','clickinfo:studentid','student2'
        put 'clicks','click3','clickinfo:url','http://www.google.com'
        put 'clicks','click3','clickinfo:time','2014-01-01 01:02:01.0001'
        put 'clicks','click3','iteminfo:itemtype','text'
        put 'clicks','click3','iteminfo:quantity','2'
        put 'clicks','click4','clickinfo:studentid','student2'
        put 'clicks','click4','clickinfo:url','http://www.ask.com'
        put 'clicks','click4','clickinfo:time','2013-02-01 12:01:01.0001'
        put 'clicks','click4','iteminfo:itemtype','text'
        put 'clicks','click4','iteminfo:quantity','5'
        put 'clicks','click5','clickinfo:studentid','student2'
        put 'clicks','click5','clickinfo:url','http://www.reuters.com'
        put 'clicks','click5','clickinfo:time','2013-02-01 12:01:01.0001'
        put 'clicks','click5','iteminfo:itemtype','text'
        put 'clicks','click5','iteminfo:quantity','100'
        put 'clicks','click6','clickinfo:studentid','student3'
        put 'clicks','click6','clickinfo:url','http://www.google.com'
        put 'clicks','click6','clickinfo:time','2013-02-01 12:01:01.0001'
        put 'clicks','click6','iteminfo:itemtype','image'
        put 'clicks','click6','iteminfo:quantity','1'
        put 'clicks','click7','clickinfo:studentid','student3'
        put 'clicks','click7','clickinfo:url','http://www.ask.com'
        put 'clicks','click7','clickinfo:time','2013-02-01 12:45:01.0001'
        put 'clicks','click7','iteminfo:itemtype','image'
        put 'clicks','click7','iteminfo:quantity','10'
        put 'clicks','click8','clickinfo:studentid','student4'
        put 'clicks','click8','clickinfo:url','http://www.amazon.com'
        put 'clicks','click8','clickinfo:time','2013-02-01 22:01:01.0001'
        put 'clicks','click8','iteminfo:itemtype','image'
        put 'clicks','click8','iteminfo:quantity','1'
        put 'clicks','click9','clickinfo:studentid','student4'
        put 'clicks','click9','clickinfo:url','http://www.amazon.com'
        put 'clicks','click9','clickinfo:time','2013-02-01 22:01:01.0001'
        put 'clicks','click9','iteminfo:itemtype','image'
        put 'clicks','click9','iteminfo:quantity','10'

4. Issue the following command to put the data into hbase:  
  
        cat testdata.txt | hbase shell
5. In Drill, use the [MapR-DB format plugin](/docs/mapr-db-format), if you are using the Drill Sandbox; otherwise, enable and use the hbase storage plugin on a system having HBase services.  
   * USE hbase; /* If you have installed HBase services. */  
   * USE maprdb; /* If you are using the Drill Sandbox */

The `maprdb` format plugin provides access to the `/tables` directory. Use Drill to query the students and clicks tables on the Drill Sandbox.  

## Query HBase Tables
1. Issue the following query to see the data in the students table:  

       SELECT * FROM students;
   The query returns binary results:
  
        +------------+------------+------------+
        |  row_key   |  account   |  address   |
        +------------+------------+------------+
        | [B@e6d9eb7 | {"name":"QWxpY2U="} | {"state":"Q0E=","street":"MTIzIEJhbGxtZXIgQXY=","zipcode":"MTIzNDU="} |
        | [B@2823a2b4 | {"name":"Qm9i"} | {"state":"Q0E=","street":"MSBJbmZpbml0ZSBMb29w","zipcode":"MTIzNDU="} |
        | [B@3b8eec02 | {"name":"RnJhbms="} | {"state":"Q0E=","street":"NDM1IFdhbGtlciBDdA==","zipcode":"MTIzNDU="} |
        | [B@242895da | {"name":"TWFyeQ=="} | {"state":"Q0E=","street":"NTYgU291dGhlcm4gUGt3eQ==","zipcode":"MTIzNDU="} |
        +------------+------------+------------+
        4 rows selected (1.335 seconds)
   The Drill output reflects the actual data type of the HBase data, which is binary.

2. Issue the following query, that includes the CONVERT_FROM function, to convert the `students` table to readable data:

         SELECT CONVERT_FROM(row_key, 'UTF8') AS studentid, 
                CONVERT_FROM(students.account.name, 'UTF8') AS name, 
                CONVERT_FROM(students.address.state, 'UTF8') AS state, 
                CONVERT_FROM(students.address.street, 'UTF8') AS street, 
                CONVERT_FROM(t.students.address.zipcode, 'UTF8') AS zipcode 
         FROM students;

    **Note:** Use dot notation to drill down to a column in an HBase table:
    
        tablename.columnfamilyname.columnnname

    The query returns readable data:

        +------------+------------+------------+------------+------------+
        | studentid  |    name    |   state    |   street   |  zipcode   |
        +------------+------------+------------+------------+------------+
        | student1   | Alice      | CA         | 123 Ballmer Av | 12345      |
        | student2   | Bob        | CA         | 1 Infinite Loop | 12345      |
        | student3   | Frank      | CA         | 435 Walker Ct | 12345      |
        | student4   | Mary       | CA         | 56 Southern Pkwy | 12345      |
        +------------+------------+------------+------------+------------+
        4 rows selected (0.504 seconds)

3. Query the clicks table to see which students visited google.com:
  
        SELECT CONVERT_FROM(row_key, 'UTF8') AS clickid, 
               CONVERT_FROM(clicks.clickinfo.studentid, 'UTF8') AS studentid, 
               CONVERT_FROM(clicks.clickinfo.`time`, 'UTF8') AS `time`,
               CONVERT_FROM(clicks.clickinfo.url, 'UTF8') AS url 
        FROM clicks WHERE clicks.clickinfo.url LIKE '%google%'; 

        +------------+------------+------------+------------+
        |  clickid   | studentid  |    time    |    url     |
        +------------+------------+------------+------------+
        | click1     | student1   | 2014-01-01 12:01:01.0001 | http://www.google.com |
        | click3     | student2   | 2014-01-01 01:02:01.0001 | http://www.google.com |
        | click6     | student3   | 2013-02-01 12:01:01.0001 | http://www.google.com |
        +------------+------------+------------+------------+
        3 rows selected (0.294 seconds)

4. Query the clicks table to get the studentid of the student having 100 items.

        SELECT CONVERT_FROM(tbl.clickinfo.studentid, 'UTF8') AS studentid, CONVERT_FROM(tbl.iteminfo.itemtype, 'UTF8'), CAST(tbl.iteminfo.quantity AS INT) AS items FROM clicks tbl WHERE tbl.iteminfo.quantity=100;

        +------------+------------+------------+
        | studentid  |   EXPR$1   |   items    |
        +------------+------------+------------+
        | student2   | text       | 100        |
        +------------+------------+------------+
        1 row selected (0.656 seconds)