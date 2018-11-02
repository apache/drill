---
title: "Selecting Nested Data for a Column"
date: 2018-11-02
parent: "Querying Complex Data"
---
The following queries show how to access the nested data inside the parts of
the record that are not flat (such as `topping`). To isolate and return nested
data, use the `[n]` notation, where `n` is a number that points to a specific
position in an array. Arrays use a 0-based index, so `topping[3]` points to
the _fourth_ element in the array under `topping`, not the third.

    0: jdbc:drill:zk=local> select topping[3] as top from dfs.`/Users/brumsby/drill/donuts.json`;
  
    +------------+
    |    top     |
    +------------+
    | {"id":"5007","type":"Powdered Sugar"} |
    +------------
    1 row selected (0.137 seconds)

Note that this query produces _one column for all of the data_ that is nested
inside the `topping` segment of the file. The query as written does not unpack
the `id` and `type` name/value pairs. Also note the use of an alias for the
column name. (Without the alias, the default column name would be `EXPR$0`.)

Some JSON files store arrays within arrays. If your data has this
characteristic, you can probe into the inner array by using the following
notation: `[n][n]`

For example, assume that a segment of the JSON file looks like this:

    ...
    group:
    [
      [1,2,3],
  
      [4,5,6],
  
      [7,8,9]
    ]
    ...

The following query would return `6` (the _third_ value of the _second_ inner
array).

`select group[1][2]`
