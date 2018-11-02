---
title: "Using SQL Functions, Clauses, and Joins"
date: 2018-11-02
parent: "Querying Complex Data"
---
You can use standard SQL clauses, such as WHERE and ORDER BY, to elaborate on
this kind of simple query:

    0: jdbc:drill:zk=local> select id, type from dfs.`/Users/brumsby/drill/donuts.json`
    where id>0
    order by id limit 1;
  
    +------------+------------+
    |     id     |    type    |
    +------------+------------+
    | 0001       | donut      |
    +------------+------------+
  
    1 row selected (0.318 seconds)

You can also join files (or tables, or files and tables) by using standard
syntax:

    0: jdbc:drill:zk=local> select tbl1.id, tbl1.type from dfs.`/Users/brumsby/drill/donuts.json` as tbl1
    join
    dfs.`/Users/brumsby/drill/moredonuts.json` as tbl2
    on tbl1.id=tbl2.id;
  
    +------------+------------+
    |     id     |    type    |
    +------------+------------+
    | 0001       | donut      |
    +------------+------------+
  
    1 row selected (0.395 seconds)

Equivalent USING syntax and joins in the WHERE clause are also supported.

Standard aggregate functions work against JSON data. For example:

    0: jdbc:drill:zk=local> select type, avg(ppu) as ppu_sum from dfs.`/Users/brumsby/drill/donuts.json` group by type;
  
    +------------+------------+
    |    type    |  ppu_sum   |
    +------------+------------+
    | donut      | 0.55       |
    +------------+------------+
  
    1 row selected (0.216 seconds)
  
    0: jdbc:drill:zk=local> select type, sum(sales) as sum_by_type from dfs.`/Users/brumsby/drill/moredonuts.json` group by type;
  
    +------------+-------------+
    |    type    | sum_by_type |
    +------------+-------------+
    | donut      | 1194        |
    +------------+-------------+
  
    1 row selected (0.389 seconds)
