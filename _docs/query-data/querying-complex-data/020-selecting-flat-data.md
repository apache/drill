---
title: "Selecting Flat Data"
date: 2018-11-02
parent: "Querying Complex Data"
---
A very simple query against the `donuts.json` file returns the values for the
five "flat" columns (the columns that contain data at the top level only: no
nested data):

    0: jdbc:drill:zk=local> select id, type, name, ppu
    from dfs.`/Users/brumsby/drill/donuts.json`;
    +-------+--------+----------------+-------+
    |  id   |  type  |      name      |  ppu  |
    +-------+--------+----------------+-------+
    | 0001  | donut  | Cake           | 0.55  |
    | 0002  | donut  | Raised         | 0.69  |
    | 0003  | donut  | Old Fashioned  | 0.55  |
    | 0004  | donut  | Filled         | 0.69  |
    | 0005  | donut  | Apple Fritter  | 1.0   |
    +-------+--------+----------------+-------+
    5 rows selected (0.18 seconds)

Note that `dfs` is the schema name, the path to the file is enclosed by
backticks, and the query must end with a semicolon.
