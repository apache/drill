---
title: "Designing Indexes for Your Queries"
date: 2018-09-28 21:35:21 UTC
parent: "Querying Indexes"
---   

Design indexes that support your queries for maximum performance benefits. Use common query patterns that involve filters and ordering to define indexes. Weigh the benefits of indexes against their update and storage costs and take into consideration any index limitations.  

##Identify Query Patterns  
Query patterns, such as queries with filter conditions and ORDER BY clauses, indicate where indexes can improve performance. If a query does not contain selective filters, the overhead of using an index may cost more than a full table scan. You should also define your indexes such that a single index benefits either multiple queries or individual queries that you run most often.  

###Determine Potential Indexes Based on Query Patterns  
The following table describes the types and characteristics of indexes you might want to create based on some example query patterns:  

| **Identified Query Pattern**                                                                    | **Potential Indexes to Create**                                                                                                                                                                                                                     |
|---------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Compares individual columns with selective filter conditions                                | Define single column indexes on the columns that   you compare against. Verify that the columns contain supported data types.                                                                                                                   |
| Filters against specific combinations of columns                                            | Define   composite column indexes instead of single column indexes. Specify the   sequence of the index keys so columns that appear in equality conditions are   the prefixes in the keys.                                                      |
| Accesses a subset of columns   in a document, but does not filter or sort on these columns  | Add those columns as included columns in   indexes.                                                                                                                                                                                             |
| Filters on a sub-column in a   nested document                                              | Define the index key on the sub-column.                                                                                                                                                                                                         |
| Filters on sub-columns in   nested documents that are array elements                        | Define the index key using a container column   path: for example, arraycolumn[].subcolumn.                                                                                                                                                     |
| Filters and projects using a   container column path                                        | Define the container column path as both an   indexed column and included column.                                                                                                                                                               |
| Filters on individual   elements of an array, which can appear in any position in the array | Define an index using a container column path:   for example, arraycolumn[].                                                                                                                                                                    |
| Issues Drill SQL queries with   filter conditions that contain CAST expressions             | Specify the CAST function when defining the   index key.                                                                                                                                                                                        |
| Sorts on columns                                                                            | Define the sequence and order direction of the   index keys to match the sequence and order direction of the columns your   query sorts. If the sort order of the index keys matches the insertion order   of documents, define hashed indexes. |
| Sorts on one set of columns   and filters on another set using equality conditions          | Define a composite index so that columns using   equality conditions are the prefixes in the index keys, followed by the sort   columns.                                                                                                        |   

##Evaluate Trade-Offs and Limitations  

When designing indexes for optimization, consider the following trade-offs and limitations:

###Synchronizing Indexes
When you design your indexes, remember that the data source must synchronize each index when you insert and update columns in the table. This impacts the throughput performance of inserts and updates because the data source must perform additional writes. The impact increases with each additional index.  

###Index Storage Requirements
Consider the storage costs when creating indexes and deciding on the columns to add to the index. Indexes increase your storage requirements. The storage size depends on the number of indexed and included columns in the index and the size of values stored in those columns. As the size of the index increases, the cost of reading the index also increases.  

###Index Restrictions
When designing your indexes, make sure the indexes support the functionality you need. 

**Examples**  

The following examples illustrate the concepts behind index design, though they do not account for sizing, storage, and updates. Always weigh the benefits of indexes against these other requirements.  

Suppose you have the following customer data in JSON format:  

	{
	   "_id": "10000",
	   "FullName": {
	      "LastName": "Smith",
	      "FirstName": "John"
	   },
	   "Address": {
	      "Street": "123 SE 22nd St.",
	      "City": "Oakland",
	      "State": "CA",
	      "Zipcode": "94601-1001"
	   },
	   "Gender": "M",
	   "AccountBalance": 999.99,
	   "Email": "john.smith@company.com",
	   "Phones": [
	      {"Type": "Home", "Number": "555-555-1234"},
	      {"Type": "Mobile", "Number": "555-555-5678"},
	      {"Type": "Work", "Number": "555-555-9012"}
	   ],
	   "Hobbies": ["Baseball", "Cooking", "Reading"],
	   "DateOfBirth": "10/1/1985"
	}   

The following table contains columns in the document that are candidates for indexing based on the sample queries:  

|    Query # | Query                                                                                                                     | Candidate   columns for Indexing                           |
|------------|---------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| 1          | Find all customers who were born in the 1970s.                                                                            | DateOfBirth                                                |
| 2          | Find all customers who have an account balance   greater than $10K. Order the information in descending order of balance. | AccountBalance                                             |
| 3          | List customers who live in   California, ordering the list by LastName, FirstName.                                        | Address.State     FullName.LastName     FullName.FirstName |
| 4          | Find the ids and emails of customers who live in   a specific zip code.                                                   | Address.Zip                                                |
| 5          | Find customers who live in a   specific set of states and have an account balance less than a specific   value.           | Address.State     AccountBalance                           |
| 6          | Find male customers with the   last name starting with the letter "S".                                                    | Gender     FullName.LastName                               |
| 7          | Find all customers who have a   mobile phone number with a prefix of "650".                                               | Phones[].Type     Phones[].Number                          |     

The following table contains indexes you might create to optimize the queries listed in the previous table and the reasons for doing so:  

|    Index                                                                                           | Rationale                                                                                                                                                                                                                                                                |
|----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Simple   index on DateOfBirth                                                                      | Optimizes the range condition on   DateOfBirth in query 1.     You need not create a hashed index, because it is unlikely that the order   of DateOfBirth correlates with the insert order of new data.                                                                  |
| Simple   index on AccountBalance, specified as a descending key                                    | Optimizes the range condition on   AccountBalance in query 2.     Descending order of key meets the ordering criteria in query 2.     Also optimizes the range condition on AccountBalance in query 5 in   combination with the index on Address.State.                  |
| Composite   index on:     •Address.State     •FullName.LastName     •FullName.FirstName            | Optimizes both the equality   condition on Address.State and ordering in query 3.     Inclusion of the name columns in the index meets query 3 ordering.     Also optimizes the IN condition in query 5 when used in combination with   the index on AccountBalance      |
| Simple   index with:     •Indexed column on Address.Zip     •Included columns on Id and Email      | Optimizes the equality condition   on Address.Zip in query 4.     Adding the included columns avoids reading the JSON table in query 4.                                                                                                                                  |
| Composite   index on:     •Gender     •FullName.LastName                                           | Optimizes equality condition on   Gender and pattern matching condition on FullName.LastName for query 6.     Specifying Gender as the leading key in combination with FullName.LastName   results in more selective index lookups for query 6.                          |
| Simple   index on Hobbies[]                                                                        | Optimizes the equality condition   on array elements of Hobbies in query 7:     {"$eq":{"Hobbies[]":"Reading"}}                                                                                                                                                          |
| Composite   index on:     •Phones[].Type     •Phones[].Number                                      | Optimizes the following two   conditions in query 8:     •Equality condition on the Type subcolumn in nested documents in the Phones   array.     •Pattern matching condition on the Number subcolumn in nested documents in   the Phones array.                         |




