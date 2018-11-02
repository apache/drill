---
title: "Lesson 2: Run Queries with ANSI SQL"
date: 2018-11-02
parent: "Learn Drill with the MapR Sandbox"
---
## Goal

This lesson shows how to do some standard SQL analysis in Apache Drill: for
example, summarizing data by using simple aggregate functions and connecting
data sources by using joins. Note that Apache Drill provides ANSI SQL support,
not a “SQL-like” interface.

## Queries in This Lesson

Now that you know what the data sources look like in their raw form, using
select * queries, try running some simple but more useful queries on each data
source. These queries demonstrate how Drill supports ANSI SQL constructs and
also how you can combine data from different data sources in a single SELECT
statement.

  * Show an aggregate query on a single file or table. Use GROUP BY, WHERE, HAVING, and ORDER BY clauses.
  * Perform joins between Hive, MapR-DB, and file system data sources.
  * Use table and column aliases.
  * Create a Drill view.

## Aggregation


### Set the schema to hive:

    0: jdbc:drill:> use hive.`default`;
    +-------+-------------------------------------------+
    |  ok   |                  summary                  |
    +-------+-------------------------------------------+
    | true  | Default schema changed to [hive.default]  |
    +-------+-------------------------------------------+
    1 row selected 

### Return sales totals by month:

    0: jdbc:drill:> select `month`, sum(order_total)
    from orders group by `month` order by 2 desc;
    +------------+---------+
    |   month    | EXPR$1  |
    +------------+---------+
    | June       | 950481  |
    | May        | 947796  |
    | March      | 836809  |
    | April      | 807291  |
    | July       | 757395  |
    | October    | 676236  |
    | August     | 572269  |
    | February   | 532901  |
    | September  | 373100  |
    | January    | 346536  |
    +------------+---------+
    10 rows selected 

Drill supports SQL aggregate functions such as SUM, MAX, AVG, and MIN.
Standard SQL clauses work in the same way in Drill queries as in relational
database queries.

Note that back ticks are required for the “month” column only because “month”
is a reserved word in SQL.

### Return the top 20 sales totals by month and state:

    0: jdbc:drill:> select `month`, state, sum(order_total) as sales from orders group by `month`, state
    order by 3 desc limit 20;
    +-----------+--------+---------+
    |   month   | state  |  sales  |
    +-----------+--------+---------+
    | May       | ca     | 119586  |
    | June      | ca     | 116322  |
    | April     | ca     | 101363  |
    | March     | ca     | 99540   |
    | July      | ca     | 90285   |
    | October   | ca     | 80090   |
    | June      | tx     | 78363   |
    | May       | tx     | 77247   |
    | March     | tx     | 73815   |
    | August    | ca     | 71255   |
    | April     | tx     | 68385   |
    | July      | tx     | 63858   |
    | February  | ca     | 63527   |
    | June      | fl     | 62199   |
    | June      | ny     | 62052   |
    | May       | fl     | 61651   |
    | May       | ny     | 59369   |
    | October   | tx     | 55076   |
    | March     | fl     | 54867   |
    | March     | ny     | 52101   |
    +-----------+--------+---------+
    20 rows selected 

Note the alias for the result of the SUM function. Drill supports column
aliases and table aliases.

## HAVING Clause

This query uses the HAVING clause to constrain an aggregate result.

### Set the workspace to dfs.clicks

    0: jdbc:drill:> use dfs.clicks;
    +-------+-----------------------------------------+
    |  ok   |                 summary                 |
    +-------+-----------------------------------------+
    | true  | Default schema changed to [dfs.clicks]  |
    +-------+-----------------------------------------+
    1 row selected

### Return total number of clicks for devices that indicate high click-throughs:

    0: jdbc:drill:> select t.user_info.device, count(*) from `clicks/clicks.json` t 
    group by t.user_info.device
    having count(*) > 1000;
    +---------+---------+
    | EXPR$0  | EXPR$1  |
    +---------+---------+
    | IOS5    | 11814   |
    | AOS4.2  | 5986    |
    | IOS6    | 4464    |
    | IOS7    | 3135    |
    | AOS4.4  | 1562    |
    | AOS4.3  | 3039    |
    +---------+---------+
    6 rows selected

The aggregate is a count of the records for each different mobile device in
the clickstream data. Only the activity for the devices that registered more
than 1000 transactions qualify for the result set.

## UNION Operator

Use the same workspace as before (dfs.clicks).

### Combine clicks activity from before and after the marketing campaign

    0: jdbc:drill:> select t.trans_id transaction, t.user_info.cust_id customer from `clicks/clicks.campaign.json` t 
    union all 
    select u.trans_id, u.user_info.cust_id  from `clicks/clicks.json` u limit 5;
    +-------------+------------+
    | transaction |  customer  |
    +-------------+------------+
    | 35232       | 18520      |
    | 31995       | 17182      |
    | 35760       | 18228      |
    | 37090       | 17015      |
    | 37838       | 18737      |
    +-------------+------------+

This UNION ALL query returns rows that exist in two files (and includes any
duplicate rows from those files): `clicks.campaign.json` and `clicks.json`.

## Subqueries

### Set the workspace to hive:

    0: jdbc:drill:> use hive.`default`;
    +-------+-------------------------------------------+
    |  ok   |                  summary                  |
    +-------+-------------------------------------------+
    | true  | Default schema changed to [hive.default]  |
    +-------+-------------------------------------------+
    1 row selected
    
### Compare order totals across states:

    0: jdbc:drill:> select ny_sales.cust_id, ny_sales.total_orders, ca_sales.total_orders
    from
    (select o.cust_id, sum(o.order_total) as total_orders from hive.orders o where state = 'ny' group by o.cust_id) ny_sales
    left outer join
    (select o.cust_id, sum(o.order_total) as total_orders from hive.orders o where state = 'ca' group by o.cust_id) ca_sales
    on ny_sales.cust_id = ca_sales.cust_id
    order by ny_sales.cust_id
    limit 20;
    +------------+------------+------------+
    |  cust_id   |  ny_sales  |  ca_sales  |
    +------------+------------+------------+
    | 1001       | 72         | 47         |
    | 1002       | 108        | 198        |
    | 1003       | 83         | null       |
    | 1004       | 86         | 210        |
    | 1005       | 168        | 153        |
    | 1006       | 29         | 326        |
    | 1008       | 105        | 168        |
    | 1009       | 443        | 127        |
    | 1010       | 75         | 18         |
    | 1012       | 110        | null       |
    | 1013       | 19         | null       |
    | 1014       | 106        | 162        |
    | 1015       | 220        | 153        |
    | 1016       | 85         | 159        |
    | 1017       | 82         | 56         |
    | 1019       | 37         | 196        |
    | 1020       | 193        | 165        |
    | 1022       | 124        | null       |
    | 1023       | 166        | 149        |
    | 1024       | 233        | null       |
    +------------+------------+------------+

This example demonstrates Drill support for subqueries. 

## CAST Function

### Use the maprdb workspace:

    0: jdbc:drill:> use maprdb;
    +-------+-------------------------------------+
    |  ok   |               summary               |
    +-------+-------------------------------------+
    | true  | Default schema changed to [maprdb]  |
    +-------+-------------------------------------+
    1 row selected (0.088 seconds)

### Return customer data with appropriate data types

    0: jdbc:drill:> select cast(row_key as int) as cust_id, cast(t.personal.name as varchar(20)) as name, 
    cast(t.personal.gender as varchar(10)) as gender, cast(t.personal.age as varchar(10)) as age,
    cast(t.address.state as varchar(4)) as state, cast(t.loyalty.agg_rev as dec(7,2)) as agg_rev, 
    cast(t.loyalty.membership as varchar(20)) as membership
    from customers t limit 5;
    +----------+----------------------+-----------+-----------+--------+----------+-------------+
    | cust_id  |         name         |  gender   |    age    | state  | agg_rev  | membership  |
    +----------+----------------------+-----------+-----------+--------+----------+-------------+
    | 10001    | "Corrine Mecham"     | "FEMALE"  | "15-20"   | "va"   | 197.00   | "silver"    |
    | 10005    | "Brittany Park"      | "MALE"    | "26-35"   | "in"   | 230.00   | "silver"    |
    | 10006    | "Rose Lokey"         | "MALE"    | "26-35"   | "ca"   | 250.00   | "silver"    |
    | 10007    | "James Fowler"       | "FEMALE"  | "51-100"  | "me"   | 263.00   | "silver"    |
    | 10010    | "Guillermo Koehler"  | "OTHER"   | "51-100"  | "mn"   | 202.00   | "silver"    |
    +----------+----------------------+-----------+-----------+--------+----------+-------------+

Note the following features of this query:

  * The CAST function is required for every column in the table. This function returns the MapR-DB/HBase binary data as readable integers and strings. Alternatively, you can use CONVERT_TO/CONVERT_FROM functions to decode the string columns. CONVERT_TO/CONVERT_FROM are more efficient than CAST in most cases. Use only CONVERT_TO to convert binary types to any type other than VARCHAR.
  * The row_key column functions as the primary key of the table (a customer ID in this case).
  * The table alias t is required; otherwise the column family names would be parsed as table names and the query would return an error.

### Remove the quotes from the strings:

You can use the regexp_replace function to remove the quotes around the
strings in the query results. For example, to return a state name va instead
of “va”:

    0: jdbc:drill:> select cast(row_key as int), regexp_replace(cast(t.address.state as varchar(10)),'"','')
    from customers t limit 1;
    +------------+------------+
    |   EXPR$0   |   EXPR$1   |
    +------------+------------+
    | 10001      | va         |
    +------------+------------+
    1 row selected

## CREATE VIEW Command

    0: jdbc:drill:> use dfs.views;
    +-------+----------------------------------------+
    |  ok   |                summary                 |
    +-------+----------------------------------------+
    | true  | Default schema changed to [dfs.views]  |
    +-------+----------------------------------------+
    1 row selected

### Use a mutable workspace:

A mutable (or writable) workspace is a workspace that is enabled for “write”
operations. This attribute is part of the storage plugin configuration. You
can create Drill views and tables in mutable workspaces.

### Create a view on a MapR-DB table

    0: jdbc:drill:> create or replace view custview as select cast(row_key as int) as cust_id,
    cast(t.personal.name as varchar(20)) as name, 
    cast(t.personal.gender as varchar(10)) as gender, 
    cast(t.personal.age as varchar(10)) as age, 
    cast(t.address.state as varchar(4)) as state,
    cast(t.loyalty.agg_rev as dec(7,2)) as agg_rev,
    cast(t.loyalty.membership as varchar(20)) as membership
    from maprdb.customers t;
    +-------+-------------------------------------------------------------+
    |  ok   |                           summary                           |
    +-------+-------------------------------------------------------------+
    | true  | View 'custview' created successfully in 'dfs.views' schema  |
    +-------+-------------------------------------------------------------+
    1 row selected

Drill provides CREATE OR REPLACE VIEW syntax similar to relational databases
to create views. Use the OR REPLACE option to make it easier to update the
view later without having to remove it first. Note that the FROM clause in
this example must refer to maprdb.customers. The MapR-DB tables are not
directly visible to the dfs.views workspace.

Unlike a traditional database where views typically are DBA/developer-driven
operations, file system-based views in Drill are very lightweight. A view is
simply a special file with a specific extension (.drill). You can store views
even in your local file system or point to a specific workspace. You can
specify any query against any Drill data source in the body of the CREATE VIEW
statement.

Drill provides a decentralized metadata model. Drill is able to query metadata
defined in data sources such as Hive, HBase, and the file system. Drill also
supports the creation of metadata in the file system.

### Query data from the view:

    0: jdbc:drill:> select * from custview limit 1;
    +----------+-------------------+-----------+----------+--------+----------+-------------+
    | cust_id  |       name        |  gender   |   age    | state  | agg_rev  | membership  |
    +----------+-------------------+-----------+----------+--------+----------+-------------+
    | 10001    | "Corrine Mecham"  | "FEMALE"  | "15-20"  | "va"   | 197.00   | "silver"    |
    +----------+-------------------+-----------+----------+--------+----------+-------------+
    1 row selected

Once the users know what data is available by exploring it directly from the file system, views can be used as a way to read the data into downstream tools such as Tableau and MicroStrategy for analysis and visualization. For these tools, a view appears simply as a “table” with selectable “columns” in it.

## Query Across Data Sources

Continue using `dfs.views` for this query.

### Join the customers view and the orders table:

    0: jdbc:drill:> select membership, sum(order_total) as sales from hive.orders, custview
    where orders.cust_id=custview.cust_id
    group by membership order by 2;
    +------------+------------+
    | membership |   sales    |
    +------------+------------+
    | "basic"    | 380665     |
    | "silver"   | 708438     |
    | "gold"     | 2787682    |
    +------------+------------+
    3 rows selected

In this query, we are reading data from a MapR-DB table (represented by
custview) and combining it with the order information in Hive. When doing
cross data source queries such as this, you need to use fully qualified
table/view names. For example, the orders table is prefixed by “hive,” which
is the storage plugin name registered with Drill. We are not using any prefix
for “custview” because we explicitly switched the dfs.views workspace where
custview is stored.

Note: If the results of any of your queries appear to be truncated because the
rows are wide, set the maximum width of the display to 10000:

Do not use a semicolon for this SET command.

### Join the customers, orders, and clickstream data:

    0: jdbc:drill:> select custview.membership, sum(orders.order_total) as sales from hive.orders, custview,
    dfs.`/mapr/demo.mapr.com/data/nested/clicks/clicks.json` c 
    where orders.cust_id=custview.cust_id and orders.cust_id=c.user_info.cust_id 
    group by custview.membership order by 2;
    +------------+------------+
    | membership |   sales    |
    +------------+------------+
    | "basic"    | 372866     |
    | "silver"   | 728424     |
    | "gold"     | 7050198    |
    +------------+------------+
    3 rows selected

This three-way join selects from three different data sources in one query:

  * hive.orders table
  * custview (a view of the HBase customers table)
  * clicks.json file

The join column for both sets of join conditions is the cust_id column. The
views workspace is used for this query so that custview can be accessed. The
hive.orders table is also visible to the query.

However, note that the JSON file is not directly visible from the views
workspace, so the query specifies the full path to the file:

    dfs.`/mapr/demo.mapr.com/data/nested/clicks/clicks.json`


## What's Next

Go to [Lesson 3: Run Queries on Complex Data Types]({{ site.baseurl }}/docs/lesson-3-run-queries-on-complex-data-types). 



