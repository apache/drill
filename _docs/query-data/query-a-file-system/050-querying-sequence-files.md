---
title: "Querying Sequence Files"
date: 2018-02-09 00:16:04 UTC
parent: "Querying a File System"
---

Sequence files are flat files that store binary key value pairs.
Drill projects sequence files as a table with two columns 'binary_key', 'binary_value'.


## Querying a Sequence File

Start the Drill shell and enter your query.

        SELECT * FROM dfs.tmp.`simple.seq` LIMIT 1;
        +--------------+---------------+
        |  binary_key  | binary_value  |
        +--------------+---------------+
        | [B@70828f46  | [B@b8c765f    |
        +--------------+---------------+

Since simple.seq contains byte serialized strings as keys and values, you can convert them to strings.

        SELECT CONVERT_FROM(binary_key, 'UTF8'), CONVERT_FROM(binary_value, 'UTF8') FROM dfs.tmp.`simple.seq` LIMIT 1;
        +-----------+-------------+
        |  EXPR$0   |   EXPR$1    |
        +-----------+-------------+
        | key0      |   value0    |
        +-----------+-------------+
