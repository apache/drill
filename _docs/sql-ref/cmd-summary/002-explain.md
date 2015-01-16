---
title: "EXPLAIN commands"
parent: "SQL Commands Summary"
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

##### INCLUDING ALL ATTRIBUTES

This option returns costing information. You can use this option for both
physical and logical plans.

#### WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION

These options return the physical and logical plan information, respectively.
The default is physical (WITH IMPLEMENTATION).

## EXPLAIN for Physical Plans

The EXPLAIN PLAN FOR <query> command returns the chosen physical execution
plan for a query statement without running the query. You can use this command
to see what kind of execution operators Drill implements. For example, you can
find out what kind of join algorithm is chosen when tables or files are
joined. You can also use this command to analyze errors and troubleshoot
queries that do not run. For example, if you run into a casting error, the
query plan text may help you isolate the problem.

Use the following syntax:

    explain plan for <query> ;
    explain plan with implementation for <query> ;

The following set command increases the default text display (number of
characters). By default, most of the plan output is not displayed.

    0: jdbc:drill:zk=local> !set maxwidth 10000

Do not use a semicolon to terminate set commands.

For example, here is the top portion of the explain output for a
COUNT(DISTINCT) query on a JSON file:

	0: jdbc:drill:zk=local> !set maxwidth 10000
	 
	0: jdbc:drill:zk=local> explain plan for 
	select type t, count(distinct id) 
	from dfs.`/Users/brumsby/drill/donuts.json` 
	where type='donut' group by type;
	 
	+------------+------------+
	|    text    |    json    |
	+------------+------------+
	| 00-00    Screen
	00-01      Project(t=[$0], EXPR$1=[$1])
	00-02        Project(t=[$0], EXPR$1=[$1])
	00-03          HashAgg(group=[{0}], EXPR$1=[COUNT($1)])
	00-04            HashAgg(group=[{0, 1}])
	00-05              SelectionVectorRemover
	00-06                Filter(condition=[=(CAST($0):CHAR(5) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary", 'donut')])
	00-07                  Project(type=[$1], id=[$2])
	00-08                    ProducerConsumer
	00-09                      Scan(groupscan=[EasyGroupScan [selectionRoot=/Users/brumsby/drill/donuts.json, columns = null]])
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

## Costing Information

Add the INCLUDING ALL ATTRIBUTES option to the EXPLAIN command to see cost
estimates for the query plan. For example:

	0: jdbc:drill:zk=local> !set maxwidth 10000
	0: jdbc:drill:zk=local> explain plan including all attributes for 
	select * from dfs.`/Users/brumsby/drill/donuts.json` where type='donut';
	 
	+------------+------------+
	|    text    |    json    |
	+------------+------------+
	 
	| 00-00    Screen: rowcount = 1.0, cumulative cost = {4.1 rows, 14.1 cpu, 0.0 io, 0.0 network}, id = 3110
	00-01      Project(*=[$0], type=[$1]): rowcount = 1.0, cumulative cost = {4.0 rows, 14.0 cpu, 0.0 io, 0.0 network}, id = 3109
	00-02        SelectionVectorRemover: rowcount = 1.0, cumulative cost = {3.0 rows, 6.0 cpu, 0.0 io, 0.0 network}, id = 3108
	00-03          Filter(condition=[=(CAST($1):CHAR(5) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary", 'donut')]): rowcount = 1.0, cumulative cost = {2.0 rows, 5.0 cpu, 0.0 io, 0.0 network}, id = 3107
	00-04            ProducerConsumer: rowcount = 1.0, cumulative cost = {1.0 rows, 1.0 cpu, 0.0 io, 0.0 network}, id = 3106
	00-05              Scan(groupscan=[EasyGroupScan [selectionRoot=/Users/brumsby/drill/donuts.json, columns = null]]): rowcount = 1.0, cumulative cost = {0.0 rows, 0.0 cpu, 0.0 io, 0.0 network}, id = 3101

## EXPLAIN for Logical Plans

To return the logical plan for a query (again, without actually running the
query), use the EXPLAIN PLAN WITHOUT IMPLEMENTATION syntax:

    explain plan without implementation for <query> ;

For example:

	0: jdbc:drill:zk=local> explain plan without implementation for 
	select a.id 
	from dfs.`/Users/brumsby/drill/donuts.json` a, dfs.`/Users/brumsby/drill/moredonuts.json` b 
	where a.id=b.id;
	 
	+------------+------------+
	|    text    |    json    |
	+------------+------------+
	| DrillScreenRel
	  DrillProjectRel(id=[$1])
	    DrillJoinRel(condition=[=($1, $3)], joinType=[inner])
	      DrillScanRel(table=[[dfs, /Users/brumsby/drill/donuts.json]], groupscan=[EasyGroupScan [selectionRoot=/Users/brumsby/drill/donuts.json, columns = null]])
	      DrillScanRel(table=[[dfs, /Users/brumsby/drill/moredonuts.json]], groupscan=[EasyGroupScan [selectionRoot=/Users/brumsby/drill/moredonuts.json, columns = null]])
	 | {
	  "head" : {
	    "version" : 1,
	    "generator" : {
	      "type" : "org.apache.drill.exec.planner.logical.DrillImplementor",
	      "info" : ""
	    },
	...

