---
title: "Verifying Index Use"
date: 2018-09-28 21:35:21 UTC
parent: "Querying Indexes"
---  

Evaluate query plans to analyze query performance and determine if Drill used qualifying indexes. You can view query plans in the Drill Web UI or through the command line using the EXPLAIN PLAN FOR command. Alternatively, you can disable the `planner.enable_index_planning` option in Drill to see the query plan with a full table scan and then compare the plan with the index-based plan.  

The following examples demonstrate how to view the query profile in the Drill Web UI and the output of the EXPLAIN PLAN FOR command to determine if the query planner in Drill selected an index-based query plan. There is also an example that shows you how to disable index planning to compare a full table scan plan with an index-based query plan.  

##Examples  
The subsequent sections assume that an index exists on a table named "lineitem." The index, l_single_c_5, is a single column index created on the L_QUANTITY column. The index also covers the L_SUPPKEY, L_DISCOUNT, L_SHIPDate, and L_SHIPMODE columns. If a query contains columns covered by the index, the query is a covering query. If a query contains columns not covered by the index, the query is non-covering and requires a lookup back into the primary table to retrieve data.  

The following list summarizes the assumptions:  

- **Table name**: `lineitem`
- **Index name**: `l_single_c_5`
- **Indexed column**: `L_QUANTITY`
- **Included columns**: `L_SUPPKEY`, `L_DISCOUNT`, `L_SHIPDate`, `L_SHIPMODE`  

###Query Profile
You can view the query plan on the Profiles page of the Drill Web UI, by selecting the query you want to evaluate and then selecting the Physical Plan page. The page displays the physical plan that Drill used to execute the query.  
 
The following image shows the physical plan that Drill used to execute this simple equality query:  

	SELECT L_SHIPDate FROM lineitem WHERE L_QUANTITY = 5; 

![](https://i.imgur.com/DkZPAYJ.png)   

In the plan, you can see that Drill scanned the index, `l_single_c_5`, instead of the primary table. The query was completely covered by the index because the index contains all the columns referenced in the query and the query filtered on the indexed column.  

###EXPLAIN Command  
Alternatively, you can issue the [EXPLAIN command]({{site.baseurl}}/docs/explain/) to see how Drill executes a query. You can see the chosen physical execution plan for a query without running the query, by issuing the [EXPLAIN PLAN FOR command]({{site.baseurl}}/docs/explain/#explain-for-physical-plans). The output of the command shows you if Drill plans to use the index when executing the query, as shown:    

	EXPLAIN PLAN FOR SELECT L_SHIPDate FROM lineitem WHERE L_QUANTITY = 5 LIMIT 10;
	+------+------+
	| text | json |
	+------+------+
	| 00-00    Screen
	00-01      Project(L_SHIPDate=[$0])
	00-02        SelectionVectorRemover
	00-03          Limit(fetch=[10])
	00-04            Limit(fetch=[10])
	00-05              Project(L_SHIPDate=[$1])
	00-06                Scan(groupscan=[JsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=hdfs:///drill/testdata/tpch/sf1/data/json/lineitem, condition=(L_QUANTITY = {"$numberLong":5}), indexName=l_single_c_5], columns=[`L_QUANTITY`, `L_SHIPDate`]]])  

In the plan, you can see that Drill plans to use the index, `l_single_c_5`, instead of performing a full table scan. The query is completely covered by the index because the index contains all columns referenced in the query and the query filters on the indexed column.  

###Comparing an Index-Based Plan to a Full Table Scan Plan 
If you want to compare an index-based plan against a plan with a full table scan, disable the `planner.enable_index_planning` option in Drill, and run the EXPLAIN PLAN FOR command with the query. Running this command with the `planner.enable_index_planning` option disabled forces Drill to generate a plan that includes a full table scan. 

You can compare the full table scan plan against the index-based plan to compare the costs and resource consumption of each plan.
 
In the following example, the indexing feature is enabled, and Drill generated a plan using the index:  

	EXPLAIN PLAN FOR SELECT L_SHIPDate FROM lineitem WHERE L_QUANTITY = 5 LIMIT 10;
	+------+------+
	| text | json |
	+------+------+
	| 00-00    Screen
	00-01      Project(L_SHIPDate=[$0])
	00-02        SelectionVectorRemover
	00-03          Limit(fetch=[10])
	00-04            Limit(fetch=[10])
	00-05              Project(L_SHIPDate=[$1])
	00-06                Scan(groupscan=[JsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=hdfs:///drill/testdata/tpch/sf1/data/json/lineitem, condition=(L_QUANTITY = {"$numberLong":5}), indexName=l_single_c_5], columns=[`L_QUANTITY`, `L_SHIPDate`]]])  

Turning the option off, as shown:  

	ALTER SESSION SET planner.enable_index_planning = false   
 
And running the EXPLAIN PLAN FOR command again shows the plan with a full table scan:  

	EXPLAIN PLAN FOR SELECT L_SHIPDate FROM lineitem WHERE L_QUANTITY = 5 LIMIT 10;
	+------+------+
	| text | json |
	+------+------+
	| 00-00    Screen
	00-01      Project(L_SHIPDate=[$0])
	00-02        SelectionVectorRemover
	00-03          Limit(fetch=[10])
	00-04            UnionExchange
	01-01              SelectionVectorRemover
	01-02                Limit(fetch=[10])
	01-03                  Project(L_SHIPDate=[$1])
	01-04                    Scan(groupscan=[JsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=hdfs:///drill/testdata/tpch/sf1/data/json/lineitem, condition=(L_QUANTITY = {"$numberLong":5})], columns=[`L_QUANTITY`, `L_SHIPDate`]]])
	 |  

**Note:** To see the cost of each plan, go to the Drill Web UI and view the query profile for each EXPLAIN PLAN FOR command that you issue through the command line.





 







   