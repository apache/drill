---
title: "Types of Indexes"
date: 2018-09-28 21:35:21 UTC
parent: "Querying Indexes"
---  
  
The query planner in Drill can leverage indexes to create index-based query plans for improved query performance. An index is either covering or non-covering based on the columns referenced in the query and the columns in the index. Indexes for queries with ORDER BY may be hashed or non-hashed, affecting the sort order of the indexes. Drill also supports indexes on the CAST function.  

The following sections describe these indexes in more detail and show examples of query plans generated for each.  

{% include startnote.html %}Currently, Drill works with the types of indexes supported by MapR Database. The following sections demonstrate how Drill works with indexes in the MapR Database.{% include endnote.html %}  

##Covering Index  
A covering index is an index that “covers” all the columns referenced in a query. Only the index is needed to process the query. Covering indexes avoid the overhead of fetching data from the primary table. A covering index can include indexed and non-indexed (included) columns. 

For example, suppose an index is created and queried, as follows:  

	index keys:{a}, included columns: {b} and the query is SELECT a, b FROM T WHERE a > 10 AND b < 20.

Since columns a, b are present in the index, this is a covering index. The Drill planner generates a covering index plan (index-only plan) where all the columns are retrieved from the index after pushing down relevant filter conditions to the index scan.  

###Covering Index Example  

This example uses an index, l_comp_1, created on a table named lineitem with indexed columns L_LINENUMBER and L_ORDERKEY and also includes columns L_LINESTATUS and L_QUANTITY.  

The following query references the L_LINESTATUS, L_QUANTITY, L_LINENUMBER, and L_ORDERKEY columns in the lineitem table:  

	SELECT L_LINESTATUS, L_QUANTITY FROM lineitem WHERE L_LINENUMBER = 1 AND L_ORDERKEY BETWEEN 40 AND 75;  

Because the l_comp_1 index includes all columns referenced in the query, the query planner in Drill creates a covering index plan.

Running the EXPLAIN PLAN FOR command with the query shows that Drill created a query plan that only uses the index to process the query:  

	EXPLAIN PLAN FOR SELECT L_LINESTATUS, L_QUANTITY FROM lineitem WHERE L_LINENUMBER = 1 AND L_ORDERKEY BETWEEN 40 AND 75;
	
	00-00    Screen
	00-01      Project(L_LINESTATUS=[$0], L_QUANTITY=[$1])
	00-02        Scan(table=[[si, tpch_sf1_maprdb_range, lineitem]], groupscan=[JsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=maprfs:///drill/testdata/tpch/sf1/maprdb/json/range/lineitem, condition=(((L_LINENUMBER = {"$numberLong":1}) and (L_ORDERKEY >= {"$numberLong":40})) and (L_ORDERKEY <= {"$numberLong":75})), indexName=l_comp_1], columns=[`L_LINESTATUS`, `L_QUANTITY`]]])  

Reading the query plan, you can see that the plan includes an index scan, as indicated by `groupscan=[JsonTableGroupScan` and `indexName=l_comp_1`. Drill and the data source can process this query using only the index.  

##Non-Covering Index
A non-covering index is an index that does not “cover” all the columns referenced in a query.  A non-covering index has indexed and/or non-indexed (included) columns that only partially cover the columns referenced in a query. A non-covering query plan includes an index scan and a join back to the primary table. In some scenarios, a full table scan is more cost efficient than an index scan and Drill will not create an index-based query plan.

For example, suppose an index is created and queried, as follows:  

	index keys:{a, b}, included columns: {c} and the query is SELECT d, e FROM T WHERE a > 10 AND b < 20.

Since columns d, e are not present in the index, this is a non-covering index. For such indexes, the Drill planner generates a non-covering index plan where only the row ids are fetched from the index by pushing down the WHERE clause filters and the rest of the columns are fetched after a join-back to the primary table. The join-back is performed using the row ids.  

###Non-Covering Index Example  
This example uses an index, l_comp_1, created on a table named lineitem with indexed columns L_LINENUMBER and L_ORDERKEY and also included columns L_LINESTATUS and L_QUANTITY.  

The following query references the L_RETURNFLAG, L_LINESTATUS, L_QUANTITY L_LINENUMBER, and L_ORDERKEY columns in the lineitem table:  
	
	SELECT L_RETURNFLAG, L_LINESTATUS, L_QUANTITY FROM lineitem WHERE L_LINENUMBER = 1 AND L_ORDERKEY BETWEEN 40 AND 75;  

Because the l_comp_1 index does not include the L_RETURNFLAG column, the query planner in Drill creates a non-covering index plan that uses the index, but also includes a join on the primary table.  

Running the EXPLAIN PLAN FOR command with the query shows that Drill includes an index scan and a table scan with a rowkey join:  

	EXPLAIN PLAN FOR SELECT L_RETURNFLAG, L_LINESTATUS, L_QUANTITY FROM lineitem WHERE L_LINENUMBER = 1 AND L_ORDERKEY BETWEEN 40 AND 75;
	
	00-00    Screen
	00-01      Project(L_RETURNFLAG=[$0], L_LINESTATUS=[$1], L_QUANTITY=[$2])
	00-02        Project(L_RETURNFLAG=[$2], L_LINESTATUS=[$3], L_QUANTITY=[$4])
	00-03          Project(L_LINENUMBER=[$0], L_ORDERKEY=[$1], L_RETURNFLAG=[$2], L_LINESTATUS=[$3], L_QUANTITY=[$4])
	00-04            RowKeyJoin(condition=[=($5, $6)], joinType=[inner])
	00-06              Scan(table=[[si, tpch_sf1_maprdb_range, lineitem]], groupscan=[RestrictedJsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=maprfs:///drill/testdata/tpch/sf1/maprdb/json/range/lineitem, condition=(((L_LINENUMBER = {"$numberLong":1}) and (L_ORDERKEY >= {"$numberLong":40})) and (L_ORDERKEY <= {"$numberLong":75}))], columns=[`L_LINENUMBER`, `L_ORDERKEY`, `L_RETURNFLAG`, `L_LINESTATUS`, `L_QUANTITY`, `_id`], rowcount=60012.15000000001]])
	00-05              Scan(table=[[si, tpch_sf1_maprdb_range, lineitem]], groupscan=[JsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=maprfs:///drill/testdata/tpch/sf1/maprdb/json/range/lineitem, condition=(((L_LINENUMBER = {"$numberLong":1}) and (L_ORDERKEY >= {"$numberLong":40})) and (L_ORDERKEY <= {"$numberLong":75})), indexName=l_comp_1], columns=[`_id`]]])


Reading the non-covering index plan, you can see that the plan includes an index scan, as indicated by the `groupscan=[JsonTableGroupScan` and `indexName=l_comp_1`, and also a scan on the primary table, as indicated by the `groupscan=[RestrictedJsonTableGroupScan` and the `RowKeyJoin`. To process this query, Drill can use the index with the data source, but the data source must also use the rowkey join on the primary table to fetch data in the L_RETURNFLAG column. Note that this is not a real join, but more of a lookup based on the rowid. This is a random I/O operation from the primary table since each row id may access a separate data block from the table.  

If this query ran on a regular basis, you could remove the l_comp_1 index and create a new index that includes all columns referenced in the query, including the L_RETURNFLAG column, to improve query performance. However, running a query only once or a few times may not justify the overhead of removing the old index and creating a new index.  

##Non-Hashed Indexes 
Non-hashed indexes support conditional queries with an ORDER BY clause. When processing ORDER BY queries, Drill does not have to perform sort operations on the data.  

###Non-Hashed Index Plan Example  

A non-hashed index, l_comp_1, was created on a table, lineitem with indexed columns L_LINENUMBER and L_ORDERKEY and included columns L_LINESTATUSand L_QUANTITY.  

Running the example query with the EXPLAIN PLAN FOR command shows that Drill produces an index plan without the additional sort and merge operations when using the non-hashed index to process the query, as follows:  

	EXPLAIN PLAN FOR SELECT L_LINESTATUS, L_QUANTITY FROM lineitem WHERE L_LINENUMBER = 1 AND L_ORDERKEY BETWEEN 40 AND 75 ORDER BY L_LINENUMBER;
	
	00-00    Screen
	00-01      Project(L_LINESTATUS=[$0], L_QUANTITY=[$1])
	00-02        Project(L_LINESTATUS=[$2], L_QUANTITY=[$3], L_LINENUMBER=[$0])
	00-03          Scan(table=[[si, tpch_sf1_maprdb_range, lineitem]], groupscan=[JsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=maprfs:///drill/testdata/tpch/sf1/maprdb/json/range/lineitem, condition=(((L_LINENUMBER = {"$numberLong":1}) and (L_ORDERKEY >= {"$numberLong":40})) and (L_ORDERKEY <= {"$numberLong":75})), indexName=l_comp_1], columns=[`L_LINENUMBER`, `L_ORDERKEY`, `L_LINESTATUS`, `L_QUANTITY`]]])  

Reading the query plan, you can see that Drill uses the non-hashed index plan, as indicated by `indexName=l_comp_1`. To process the query, the data source uses the index and Drill does not have to perform sort and merge operations on the data, as indicated by the absence of the Sort and SingleMergeExchange operations in the query plan. The data source sorted the data in the index when the index was created.  

##Hashed Indexes
Hashed indexes support the same conditional queries as non-hashed indexes, but they do not have a guaranteed sort order. Hashed indexes enable the data source to evenly distribute new writes on an index across logical partitions to avoid hot spotting. Drill must perform a sort for ORDER BY queries that use hashed indexes. Sorting the data can increase the CPU costs and negatively impact performance.  

If you notice performance issues with ORDER BY queries that use hashed indexes, review the query plans to see if the plans include sort and merge operations. If this is the case, create non-hashed indexes to support the queries and achieve the best performance.  

###Hashed Index Plan Example
A hashed index, l_hash_comp_1, was created with indexed columns L_LINENUMBER and L_ORDERKEY and included columns L_LINESTATUS and L_QUANTITY.  

Running the example query with the EXPLAIN PLAN FOR command shows that Drill produces an index plan with sort and merge operations to process the query when using the hashed index, as follows:  

	EXPLAIN PLAN FOR SELECT L_LINESTATUS, L_QUANTITY FROM lineitem WHERE L_LINENUMBER = 1 AND L_ORDERKEY BETWEEN 40 AND 75 ORDER BY L_LINENUMBER;
	
	00-00    Screen
	00-01      Project(L_LINESTATUS=[$0], L_QUANTITY=[$1])
	00-02        SingleMergeExchange(sort0=[2])
	01-01          SelectionVectorRemover
	01-02            Sort(sort0=[$2], dir0=[ASC])
	01-03              Project(L_LINESTATUS=[$2], L_QUANTITY=[$3], L_LINENUMBER=[$0])
	01-04                Scan(table=[[si, tpch_sf1_maprdb_hash, lineitem]], groupscan=[JsonTableGroupScan [ScanSpec=JsonScanSpec [tableName=maprfs:///drill/testdata/tpch/sf1/maprdb/json/hash/lineitem, condition=(((L_LINENUMBER = {"$numberLong":1}) and (L_ORDERKEY >= {"$numberLong":40})) and (L_ORDERKEY <= {"$numberLong":75})), indexName=l_hash_comp_1], columns=[`L_LINENUMBER`, `L_ORDERKEY`, `L_LINESTATUS`, `L_QUANTITY`]]])  

Reading the query plan, you can see that Drill uses the hashed index in the plan, as indicated by `indexName=l_hash_comp_1`. To process the query, the data source can use the index, but Drill must sort and merge the data, as indicated by the `Sort` and `SingleMergeExchange` operations in the query plan.
Using the hashed index plan for this ORDER BY query requires additional processing and negatively impacts performance.  

##Functional Index  
A functional index is an index created on functions or expressions instead of columns in a table. CAST functions are most commonly used in Drill views. For example, if the filter condition is 'WHERE CAST(zip_code as BIGINT) = 95120' and a functional index exists on CAST(zip_code as BIGINT), the query planner will leverage the index.  

When issuing Drill queries through BI tools, you can include CAST functions in your queries to create [Drill views]({{site.baseurl}}/docs/create-view/). Including CAST functions provides the metadata needed to optimally process the queries. For more information about using the CAST function with Drill, see [Data Type Conversion]({{site.baseurl}}/docs/data-type-conversion/).  

{% include startnote.html %}MapR Database supports functional indexes. The query planner in Drill can use the functional index to optimize queries with CAST functions.{% include endnote.html %}  
 