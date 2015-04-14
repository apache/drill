---
title: "Selecting Flat Data"
parent: "Querying Complex Data"
---
A very simple query against the `donuts.json` file returns the values for the
four "flat" columns (the columns that contain data at the top level only: no
nested data):

    0: jdbc:drill:zk=local> select id, type, name, ppu
    from dfs.`/Users/brumsby/drill/donuts.json`;
    +------------+------------+------------+------------+
    |     id     |    type    |    name    |    ppu     |
    +------------+------------+------------+------------+
    | 0001       | donut      | Cake       | 0.55       |
    +------------+------------+------------+------------+
    1 row selected (0.248 seconds)

Note that `dfs` is the schema name, the path to the file is enclosed by
backticks, and the query must end with a semicolon.