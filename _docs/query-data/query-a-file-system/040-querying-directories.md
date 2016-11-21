---
title: "Querying Directories"
date: 2016-11-21 22:14:46 UTC
parent: "Querying a File System"
---
You can store multiple files in a directory and query them as if they were a
single entity. You do not have to explicitly join the files. The files must be
compatible, in the sense that they must have comparable data types and columns
in the same order. Hidden files that do not have comparable data types can cause a [Table Not Found]({{site.baseurl}}/docs/troubleshooting/#table-not-found) error. You can query directories of files that have formats supported by Drill, such as JSON, Parquet, or text files. 

For example, assume that a `testdata` directory contains two files with the
same structure: `plays.csv` and `moreplays.csv`. The first file contains 7
records and the second file contains 3 records. The following query returns
the "union" of the two files, ordered by the first column:

    0: jdbc:drill:zk=local> SELECT COLUMNS[0] AS `Year`, COLUMNS[1] AS Play 
    FROM dfs.`/Users/brumsby/drill/testdata` order by 1;
 
    +------------+------------------------+
    |    Year    |          Play          |
    +------------+------------------------+
    | 1594       | Comedy of Errors       |
    | 1595       | Romeo and Juliet       |
    | 1596       | The Merchant of Venice |
    | 1599       | As You Like It         |
    | 1599       | Hamlet                 |
    | 1601       | Twelfth Night          |
    | 1606       | Macbeth                |
    | 1606       | King Lear              |
    | 1609       | The Winter's Tale      |
    | 1610       | The Tempest            |
    +------------+------------------------+
    10 rows selected (0.296 seconds)

You can drill down further and automatically query subdirectories as well. For
example, assume that you have a logs directory that contains a subdirectory
for each year and subdirectories for each month (1 through 12). The month
directories contain JSON files.

    [root@ip-172-16-1-200 logs]# pwd
    /mapr/drilldemo/labs/clicks/logs
    [root@ip-172-16-1-200 logs]# ls
    2012  2013  2014
    [root@ip-172-16-1-200 logs]# cd 2013
    [root@ip-172-16-1-200 2013]# ls
    1  10  11  12  2  3  4  5  6  7  8  9

You can query all of these files, or a subset, by referencing the file system
once in a Drill query. For example, the following query counts the number of
records in all of the files inside the `2013` directory:

    0: jdbc:drill:> SELECT COUNT(*) FROM MFS.`/mapr/drilldemo/labs/clicks/logs/2013` ;
    +------------+
    |   EXPR$0   |
    +------------+
    | 24000      |
    +------------+
    1 row selected (2.607 seconds)

You can also use variables `dir0`, `dir1`, and so on, to refer to
subdirectories in your workspace path. For example, assume that `bob.logdata`
is a workspace that points to the `logs` directory, which contains multiple
subdirectories: `2012`, `2013`, and `2014`. The following query constrains
files inside the subdirectory named `2013`. The variable `dir0` refers to the
first level down from logs, `dir1` to the next level, and so on.

    0: jdbc:drill:> USE bob.logdata;
    +------------+-----------------------------------------+
    |     ok     |              summary                    |
    +------------+-----------------------------------------+
    | true       | Default schema changed to 'bob.logdata' |
    +------------+-----------------------------------------+
    1 row selected (0.305 seconds)
 
    0: jdbc:drill:> SELECT * FROM logs WHERE dir0='2013' LIMIT 10;
    +------------+------------+------------+------------+------------+------------+------------+------------+------------+-------------+
    |    dir0    |    dir1    |  trans_id  |    date    |    time    |  cust_id   |   device   |   state    |  camp_id   |  keywords   |
    +------------+------------+------------+------------+------------+------------+------------+------------+------------+-------------+
    | 2013       | 2          | 12115      | 02/23/2013 | 19:48:24   | 3          | IOS5       | az         | 5          | who's       |
    | 2013       | 2          | 12127      | 02/26/2013 | 19:42:03   | 11459      | IOS5       | wa         | 10         | for         |
    | 2013       | 2          | 12138      | 02/09/2013 | 05:49:01   | 1          | IOS6       | ca         | 7          | minutes     |
    | 2013       | 2          | 12139      | 02/23/2013 | 06:58:20   | 1          | AOS4.4     | ms         | 7          | i           |
    | 2013       | 2          | 12145      | 02/10/2013 | 10:14:56   | 10         | IOS5       | mi         | 6          | wrong       |
    | 2013       | 2          | 12157      | 02/15/2013 | 02:49:22   | 102        | IOS5       | ny         | 5          | want        |
    | 2013       | 2          | 12176      | 02/19/2013 | 08:39:02   | 28         | IOS5       | or         | 0          | and         |
    | 2013       | 2          | 12194      | 02/24/2013 | 08:26:17   | 125445     | IOS5       | ar         | 0          | say         |
    | 2013       | 2          | 12236      | 02/05/2013 | 01:40:05   | 10         | IOS5       | nj         | 2          | sir         |
    | 2013       | 2          | 12249      | 02/03/2013 | 04:45:47   | 21725      | IOS5       | nj         | 5          | no          |
    +------------+------------+------------+------------+------------+------------+------------+------------+------------+-------------+
    10 rows selected (0.583 seconds)

You can use [query directory functions]({{site.baseurl}}/docs/query-directory-functions/) to restrict a query to one of a number of subdirectories and to prevent Drill from scanning all data in directories.

