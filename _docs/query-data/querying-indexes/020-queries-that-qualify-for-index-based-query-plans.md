---
title: "Queries that Qualify for Index-Based Query Plans"
date: 2018-09-28 21:35:21 UTC
parent: "Querying Indexes"
---  

In most cases, the planner chooses an index-based query plan for queries with [WHERE clause]({{site.baseurl}}/docs/where-clause/) filters and sort-based operations, such as [ORDER BY]({{site.baseurl}}/docs/order-by-clause/), [GROUP BY]({{site.baseurl}}/docs/group-by-clause/), and [joins]({{site.baseurl}}/docs/from-clause/#join-types).

The following types of queries and conditions typically qualify for index-based query plans:  


- Equality conditions (includes IN)
- Range conditions (includes LIKE pattern matching)
- ORDER BY queries
- GROUP BY queries
- JOIN queries
- Projection queries (including DISTINCT)

Drill can use indexes for queries that GROUP BY or ORDER BY the leading columns in an index. When a query contains GROUP BY and ORDER BY operations on the leading indexed column, Drill uses the sort order of the index to create index-based query plans with streaming aggregates and merge joins to improve query performance. Drill does not use indexes for queries that GROUP BY or ORDER BY the trailing or included columns in an index. 
If a query contains conditions on multiple columns (multi-index query), Drill can scan multiple indexes and use the index intersection to optimize the query.  

{% include startnote.html %}Each column added to an index increases the storage requirement for the index. As the storage size increases, the cost of reading the index also increases. Likewise, for the cost of adding and updating data. Consider the impact on storage and updates when including columns in an index.{% include endnote.html %}  

##Queries Without Filters  

Drill can create index-based query plans for the following types of queries without filters (queries that do not have a WHERE clause):  
   

- ORDER BY queries, as shown in the following example where L_LINENUMBER is an indexed column in the index selected for the query plan:    
  
		SELECT L_LINENUMBER FROM lineitem ORDER BY L_LINENUMBER;  
  
- GROUP BY queries, as shown in the following example where L_COMMITDate is an indexed column in the index selected for the query plan:  
  
		SELECT L_COMMITDate FROM lineitem GROUP BY L_COMMITDate;  

- JOIN queries, as shown in the following example where L_ORDERKEY and O_ORDERKEY are indexed columns and L_LINESTATUS is an included column in the index selected for the query plan:  

		SELECT L.L_LINESTATUS FROM lineitem L, orders O WHERE  L.L_ORDERKEY=O.O_ORDERKEY;  


	**Note:** If the planner picks two indexes, one for lineitem and one for orders, a sort merge join is used instead of a hash join.  

- Queries with DISTINCT projections, as shown in the following examples where L_LINENUMBER is an indexed column in the index selected for the query plan:  
 
		SELECT DISTINCT L_LINENUMBER FROM lineitem;
		SELECT COUNT(DISTINCT L_LINENUMBER) FROM lineitem;  








  


  