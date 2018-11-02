---
title: "Selecting Multiple Columns Within Nested Data"
date: 2018-11-02
parent: "Querying Complex Data"
---
The following query goes one step further to extract the JSON data, selecting
specific `id` and `type` data values _as individual columns_ from inside the
`topping` array. This query is similar to the previous query, but it returns
the `id` and `type` values as separate columns.

    0: jdbc:drill:zk=local> select tbl.topping[3].id as record, tbl.topping[3].type as first_topping
    from dfs.`/Users/brumsby/drill/donuts.json` as tbl;
    +------------+---------------+
    |   record   | first_topping |
    +------------+---------------+
    | 5007       | Powdered Sugar |
    +------------+---------------+
    1 row selected (0.133 seconds)

This query also introduces a typical requirement for queries against nested
data: the use of a table alias (named tbl in this example). Without the table
alias, the query would return an error because the parser would assume that id
is a column inside a table named topping. As in all standard SQL queries,
select tbl.col means that tbl is the name of an existing table (at least for
the duration of the query) and col is a column that exists in that table.
