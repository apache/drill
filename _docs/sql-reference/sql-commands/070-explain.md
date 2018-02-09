---
title: "EXPLAIN"
date: 2018-02-09 00:16:06 UTC
parent: "SQL Commands"
---
EXPLAIN is a useful tool for examining the steps that a query goes through
when it is executed. You can use the EXPLAIN output to gain a deeper
understanding of the parallel processing that Drill queries exploit. You can
also look at costing information, troubleshoot performance issues, and
diagnose routine errors that may occur when you run queries.

Drill provides two variations on the EXPLAIN command, one that returns the
physical plan and one that returns the logical plan. A logical plan takes the
SQL query (as written by the user and accepted by the parser) and translates
it into a logical series of operations that correspond to SQL language
constructs (without defining the specific algorithms that will be implemented
to run the query). A physical plan translates the logical plan into a specific
series of steps that will be used when the query runs. For example, a logical
plan may indicate a join step in general and classify it as inner or outer,
but the corresponding physical plan will indicate the specific type of join
operator that will run, such as a merge join or a hash join. The physical plan
is operational and reveals the specific _access methods_ that will be used for
the query.

An EXPLAIN command for a query that is run repeatedly under the exact same
conditions against the same data will return the same plan. However, if you
change a configuration option, for example, or update the tables or files that
you are selecting from, you are likely to see plan changes.

## EXPLAIN Syntax

The EXPLAIN command supports the following syntax:

    explain plan [ including all attributes ] [ with implementation | without implementation ] for <query> ;

where `query` is any valid SELECT statement supported by Drill.

**INCLUDING ALL ATTRIBUTES**

This option returns costing information. You can use this option for both
physical and logical plans.  

**WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION**

These options return the physical and logical plan information, respectively.
The default is physical (WITH IMPLEMENTATION).

### EXPLAIN for Physical Plans

The EXPLAIN PLAN FOR <query> command returns the chosen physical execution
plan for a query statement without running the query. You can use this command
to see what kind of execution operators Drill implements. For example, you can
find out what kind of join algorithm is chosen when tables or files are
joined. You can also use this command to analyze errors and troubleshoot
queries that do not run. For example, if you run into a casting error, the
query plan text may help you isolate the problem.

Use the following syntax:

    explain plan for <query> ;

The following set command increases the default text display (number of
characters). By default, most of the plan output is not displayed.

    0: jdbc:drill:zk=local> !set maxwidth 10000

Do not use a semicolon to terminate set commands.

For example, here is the top portion of the explain output for a
COUNT(DISTINCT) query on a JSON file:

    0: jdbc:drill:zk=local> !set maxwidth 10000
	0: jdbc:drill:zk=local> explain plan for select type t, count(distinct id) from dfs.`/home/donuts/donuts.json` where type='donut' group by type;
	+------------+------------+
	|   text    |   json    |
	+------------+------------+
	| 00-00 Screen
	00-01   Project(t=[$0], EXPR$1=[$1])
	00-02       Project(t=[$0], EXPR$1=[$1])
	00-03       HashAgg(group=[{0}], EXPR$1=[COUNT($1)])
	00-04           HashAgg(group=[{0, 1}])
	00-05           SelectionVectorRemover
	00-06               Filter(condition=[=($0, 'donut')])
	00-07               Scan(groupscan=[EasyGroupScan [selectionRoot=/home/donuts/donuts.json, numFiles=1, columns=[`type`, `id`], files=[file:/home/donuts/donuts.json]]])...
	...

Read the text output from bottom to top to understand the sequence of
operators that will execute the query. Note that the physical plan starts with
a scan of the JSON file that is being queried. The selected columns are
projected and filtered, then the aggregate function is applied.

The EXPLAIN text output is followed by detailed JSON output, which is reusable
for submitting the query via Drill APIs.

	| {
	  "head" : {
	    "version" : 1,
	    "generator" : {
	      "type" : "ExplainHandler",
	      "info" : ""
	    },
	    "type" : "APACHE_DRILL_PHYSICAL",
	    "options" : [ ],
	    "queue" : 0,
	    "resultMode" : "EXEC"
	  },
	....

**Costing Information**

Add the INCLUDING ALL ATTRIBUTES option to the EXPLAIN command to see cost
estimates for the query plan. For example:

	0: jdbc:drill:zk=local> !set maxwidth 10000
	0: jdbc:drill:zk=local> explain plan including all attributes for select * from dfs.`/home/donuts/donuts.json` where type='donut';
	+------------+------------+
	|   text    |   json    |
	+------------+------------+
	| 00-00 Screen: rowcount = 1.0, cumulative cost = {5.1 rows, 21.1 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 889
	00-01   Project(*=[$0]): rowcount = 1.0, cumulative cost = {5.0 rows, 21.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 888
	00-02       Project(T1¦¦*=[$0]): rowcount = 1.0, cumulative cost = {4.0 rows, 17.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 887
	00-03       SelectionVectorRemover: rowcount = 1.0, cumulative cost = {3.0 rows, 13.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 886
	00-04           Filter(condition=[=($1, 'donut')]): rowcount = 1.0, cumulative cost = {2.0 rows, 12.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 885
	00-05           Project(T1¦¦*=[$0], type=[$1]): rowcount = 1.0, cumulative cost = {1.0 rows, 8.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 884
	00-06               Scan(groupscan=[EasyGroupScan [selectionRoot=/home/donuts/donuts.json, numFiles=1, columns=[`*`], files=[file:/home/donuts/donuts.json]]]): rowcount = 1.0, cumulative cost = {0.0 rows, 0.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 883

### EXPLAIN for Logical Plans

To return the logical plan for a query (again, without actually running the
query), use the EXPLAIN PLAN WITHOUT IMPLEMENTATION syntax:

    explain plan without implementation for <query> ;

For example:

	0: jdbc:drill:zk=local> explain plan without implementation for select type t, count(distinct id) from dfs.`/home/donuts/donuts.json` where type='donut' group by type;
	+------------+------------+
	|   text    |   json    |
	+------------+------------+
	| DrillScreenRel
	  DrillProjectRel(t=[$0], EXPR$1=[$1])
	    DrillAggregateRel(group=[{0}], EXPR$1=[COUNT($1)])
	    DrillAggregateRel(group=[{0, 1}])
	        DrillFilterRel(condition=[=($0, 'donut')])
	        DrillScanRel(table=[[dfs, /home/donuts/donuts.json]], groupscan=[EasyGroupScan [selectionRoot=/home/donuts/donuts.json, numFiles=1, columns=[`type`, `id`], files=[file:/home/donuts/donuts.json]]]) | {
	  | {
	  "head" : {
	    "version" : 1,
	    "generator" : {
	    "type" : "org.apache.drill.exec.planner.logical.DrillImplementor",
	    "info" : ""
	    },
	    "type" : "APACHE_DRILL_LOGICAL",
	    "options" : null,
	    "queue" : 0,
	    "resultMode" : "LOGICAL"
	  },...
