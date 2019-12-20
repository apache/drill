---
title: "Connect a Data Source Introduction"
date: 2019-12-20
parent: "Connect a Data Source"
---
Drill includes several storage plugins than you can configure for your environment. A storage plugin is a software module for connecting Drill to data sources, such as databases and local or distributed file systems. A storage plugin typically optimizes Drill queries, provides the location of the data, and stores the workspace and file formats for reading data. Storage plugins also perform scanner and writer functions and informs the execution engine of any native capabilities, such as predicate pushdown, joins, and SQL. You can modify the default storage plugin configurations or add new storage plugin configurations. 

When you run a query, Drill gets the storage plugin configuration name in one of several ways:

* The FROM clause of the query can identify the plugin to use.
* The USE <plugin name> command can precede the query.
* You can specify the storage plugin when starting Drill.
