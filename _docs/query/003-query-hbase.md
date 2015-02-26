---
title: "Querying HBase"
parent: "Query Data"
---
This is a simple exercise that provides steps for creating a “students” table
and a “clicks” table in HBase that you can query with Drill.

To create the HBase tables and query them with Drill, complete the following
steps:

  1. Issue the following command to start the HBase shell:
  
        hbase shell
  2. Issue the following commands to create a ‘students’ table and a ‘clicks’ table with column families in HBase:
    
        echo "create 'students','account','address'" | hbase shell
    
        echo "create 'clicks','clickinfo','iteminfo'" | hbase shell
  3. Issue the following command with the provided data to create a `testdata.txt` file:

        cat > testdata.txt

     **Sample Data**

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

  4. Issue the following command to verify that the data is in the `testdata.txt` file:  
    
         cat testdata.txt | hbase shell
  5. Issue `exit` to leave the `hbase shell`.
  6. Start Drill. Refer to [Starting/Stopping Drill](/drill/docs/starting-stopping-drill) for instructions.
  7. Use Drill to issue the following SQL queries on the “students” and “clicks” tables:  
  
     1. Issue the following query to see the data in the “students” table:  

            SELECT * FROM hbase.`students`;
        The query returns binary results:
        
            Query finished, fetching results ...
            +----------+----------+----------+-----------+----------+----------+----------+-----------+
            |id    | name        | state       | street      | zipcode |`
            +----------+----------+----------+-----------+----------+-----------+----------+-----------
            | [B@1ee37126 | [B@661985a1 | [B@15944165 | [B@385158f4 |[B@3e08d131 |
            | [B@64a7180e | [B@161c72c2 | [B@25b229e5 | [B@53dc8cb8 |[B@1d11c878 |
            | [B@349aaf0b | [B@175a1628 | [B@1b64a812 | [B@6d5643ca |[B@147db06f |
            | [B@3a7cbada | [B@52cf5c35 | [B@2baec60c | [B@5f4c543b |[B@2ec515d6 |

        Since Drill does not require metadata, you must use the SQL `CAST` function in
some queries to get readable query results.

     2. Issue the following query, that includes the `CAST` function, to see the data in the “`students`” table:

            SELECT CAST(students.clickinfo.studentid as VarChar(20)),
            CAST(students.account.name as VarChar(20)), CAST (students.address.state as
            VarChar(20)), CAST (students.address.street as VarChar(20)), CAST
            (students.address.zipcode as VarChar(20)), FROM hbase.students;

        **Note:** Use the following format when you query a column in an HBase table:
          
             tablename.columnfamilyname.columnname
            
        For more information about column families, refer to [5.6. Column
Family](http://hbase.apache.org/book/columnfamily.html).

        The query returns the data:

            Query finished, fetching results ...
            +----------+-------+-------+------------------+---------+`
            | studentid | name  | state | street           | zipcode |`
            +----------+-------+-------+------------------+---------+`
            | student1 | Alice | CA    | 123 Ballmer Av   | 12345   |`
            | student2 | Bob   | CA    | 1 Infinite Loop  | 12345   |`
            | student3 | Frank | CA    | 435 Walker Ct    | 12345   |`
            | student4 | Mary  | CA    | 56 Southern Pkwy | 12345   |`
            +----------+-------+-------+------------------+---------+`

     3. Issue the following query on the “clicks” table to find out which students clicked on google.com:
        
              SELECT CAST(clicks.clickinfo.studentid as VarChar(200)), CAST(clicks.clickinfo.url as VarChar(200)) FROM hbase.`clicks` WHERE URL LIKE '%google%';  

        The query returns the data:
        
            Query finished, fetching results ...`
        
            +---------+-----------+-------------------------------+-----------------------+----------+----------+
            | clickid | studentid | time                          | url                   | itemtype | quantity |
            +---------+-----------+-------------------------------+-----------------------+----------+----------+
            | click1  | student1  | 2014-01-01 12:01:01.000100000 | http://www.google.com | image    | 1        |
            | click3  | student2  | 2014-01-01 01:02:01.000100000 | http://www.google.com | text     | 2        |
            | click6  | student3  | 2013-02-01 12:01:01.000100000 | http://www.google.com | image    | 1        |
            +---------+-----------+-------------------------------+-----------------------+----------+----------+