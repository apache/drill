---
title: "Physical Operators"
slug: "Physical Operators"
parent: "Performance Tuning Reference"
--- 

This document describes the physical operators that Drill uses in query plans.

## Distribution Operators  

Drill uses the following operators to perform data distribution over the network:  

| Operator             | Description                                                                                                                                                                                                                                                                                                                                               |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| HashToRandomExchange | A HashToRandomExchange gets an   input row, computes a hash value on the distribution key, determines the   destination receiver based on the hash value, and sends the row in a batch   operation. The join key or aggregation group-by keys are examples of distribution   keys. The destination receiver is a minor fragment on a destination   node.  |
| HashToMergeExchange  | A HashToMergeExchange is similar   to the HashToRandomExchange operator, except that each destination receiver   merges incoming streams of sorted data received from a sender.                                                                                                                                                                          |
| UnionExchange        | A UnionExchange is a   serialization operator in which each sender sends to a single (common)   destination. The receiver “unions” the input streams from various senders.                                                                                                                                                                                |
| SingleMergeExchange  | A SingleMergeExchange is   distribution operator in which each sender sends a sorted stream of data to a   single receiver. The receiver performs a Merge operation to merge all of the   incoming streams. This operator is useful when performing an ORDER BY operation   that requires a final global ordering.                                        |
| BroadcastExchange    | A BroadcastExchange is a   distribution operation in which each sender sends its input data to all N   receivers via a broadcast.                                                                                                                                                                                                                          |
| UnorderedMuxExchange | An UnorderedMuxExchange is an   operation that multiplexes the data from all minor fragments on a node so the   data can be sent out on a single channel to a destination receiver. A sender   node only needs to maintain buffers for each receiving node instead of each   receiving minor fragment on every node.                                    |

## Join Operators  

Drill uses the following join operators:

| Operator         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hash Join        | A Hash Join is used for inner joins, left, right and full outer joins.  A hash table is built on the rows produced by the inner child of the Hash Join.  The outer child rows are used to probe the hash table and find matches. This operator Holds the entire dataset for the right hand side of the join in memory  which could be up to 2 billion records per minor fragment.                                                                          |
| Merge Join       | A Merge Join is used for inner join, left and right outer joins.  Inputs to the Merge Join must be sorted. It reads the sorted input streams from both sides and finds matching rows.  This operator holds the amount of memory of one incoming record batch from each side of the join.   In addition, if there are repeating values in the right hand side of the join, the Merge Join will hold record batches for as long as a repeated value extends. |
| Nested Loop Join | A Nested Loop Join is used for certain types of cartesian joins and inequality joins.                                                                                                                                                                                                                                                                                                                                                                      |  

## Aggregate Operators  

Drill uses the following aggregate operators:  

| Operator            | Description                                                                                                                                                                                                                                                                                                                                                                                                                           |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hash Aggregate      | A Hash Aggregate performs grouped aggregation on the input data by building a hash table on the GROUP-BY keys and computing the aggregate values within each group. This operator holds memory for each aggregation grouping and each aggregate value, up to 2 billion values per minor fragment.                                                                                                                                     |
| Streaming Aggregate | A Streaming Aggregate performs grouped aggregation and non-grouped aggregation.  For grouped aggregation, the data must be sorted on the GROUP-BY keys.  Aggregate values are computed within each group.  For non-grouped aggregation, data does not have to be sorted. This operator maintains a single aggregate grouping (keys and aggregate intermediate values) at a time in addition to the size of one incoming record batch. |  

## Sort and Limit Operators  

Drill uses the following sort and limiter operators:  

| Operator     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Sort         | A Sort operator is used to perform an ORDER BY and as an upstream operator for other  operations that require sorted data such as Merge Join, Streaming Aggregate.                                                                                                                                                                                                                                                                                                         |
| ExternalSort | The ExternalSort operator can potentially hold the entire dataset in memory.  This operator will also start spooling to the disk in the case that there is memory pressure.  In this case, the external sort will continue to try to use as much memory as available.  In all cases, external sort will hold at least one record batch in memory for each record spill.  Spills are currently sized based on the amount of memory available to the external sort operator. |
| TopN         | A TopN operator is used to perform an ORDER BY with LIMIT.                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Limit        | A Limit operator is used to restrict the number of rows to a value specified by the LIMIT clause.                                                                                                                                                                                                                                                                                                                                                                          |  

## Projection Operators  

Drill uses the following projection operators:  

| Operator | Description                                                                                                                                                                                                                                       |
|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Project  | A   Project operator projects columns and/or expressions involving columns and   constants. This operator holds one incoming record batch plus any additional   materialized projects for the same number of rows as the incoming record   batch. |

## Filter and Related Operators  

Drill uses the following filter and related operators:  

| Operator               | Description                                                                                                                                                                                                                                                                                                                                                                                      |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Filter                 | A Filter operator is used to evaluate the WHERE clause and HAVING clause predicates.  These predicates may consist of join predicates as well as single table predicates.  The join predicates are evaluated by a join operator and the remaining predicates are evaluated by the Filter operator. The amount of memory it consumes is slightly more than the size of one incoming record batch. |
| SelectionVectorRemover | A SelectionVectorRemover is used in conjunction with either a Sort or Filter operator.  This operator maintains roughly twice the amount of memory as required by a single incoming record batch.                                                                                                                                                                                                |  

## Set Operators  

Drill uses the following set operators:  

| Operator  | Description                                                                                                                                                                                                                                                                                                     |
|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Union-All | A Union-All operator accepts rows from 2 input streams and produces a single output stream where the left input rows are emitted first followed by the right input rows. The column names of the output stream are inherited from the left input.  The column types of the two child inputs must be compatible. |  

## Scan Operators  

Drill uses the following scan operators:    

| Operator | Description                                                                                                                                                                                 |
|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Scan     | Performs a scan of the underlying table.  The table may be in one of several formats, such as Parquet, Text, JSON, and so on. The Scan operator encapsulates the formats into one operator. |  

## Receiver Operators 

Drill uses the following receiver operators: 

| Operator          | Description                                                                                                                                                         |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| UnorderedReceiver | The unordered receiver operator can hold up to 5 incoming record batches.                                                                                           |
| MergingReceiver   | This operator holds up to 5 record batches for each incoming stream (generally either number of nodes or number of sending fragments, depending on use of muxxing). |  

## Sender Operators  

Drill uses the following sender operators:  

| Operator        | Description                                                                                                                                                                                                                                                                    |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PartitionSender | The PartitionSender operator maintains a queue for each outbound destination.  May be either the number of outbound minor fragments or the number of the nodes, depending on the use of muxxing operations.  Each queue may store up to 3 record batches for each destination. |

## File Writers  

Drill uses the following file writers:  

| Operator          | Description                                                                                                                                    |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| ParquetFileWriter | The ParquetFileWriter buffers approximately twice the default Parquet row group size in memory per minor fragment (default in Drill is 512mb). |




 


