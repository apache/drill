---
title: "Query Profile Column Descriptions"
date: 2016-11-21 22:28:41 UTC
parent: "Performance Tuning Reference"
--- 

The following tables provide descriptions listed in each of the tables for a query profile.  


## Fragment Overview  Table  

Shows aggregate metrics for each major fragment that executed the query.

The following table lists descriptions for each column in the Fragment Overview  
table:  

| Column Name               | Description                                                                                                                                                                 |
|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Major Fragment ID         | The coordinate ID of the major fragment. For example, 03-xx-xx where 03 is the major fragment ID followed by xx-xx, which represents the minor fragment ID and operator ID. |
| Minor Fragments Reporting | The number of minor fragments that Drill parallelized for the major fragment.                                                                                               |
| First Start               | The total time before the first minor fragment started its task.                                                                                                            |
| Last Start                | The total time before the last minor fragment started its task.                                                                                                             |
| First End                 | The total time for the first minor fragment to finish its task.                                                                                                             |
| Last End                  | The total time for the last minor fragment to finish its task.                                                                                                              |
| Min Runtime               | The minimum of the total amount of time spent by minor fragments to complete their tasks.                                                                                   |
| Avg Runtime               | The average of the total amount of time spent by minor fragments to complete their tasks.                                                                                   |
| Max Runtime               | The maximum of the total amount of time spent by minor fragments to complete their tasks.                                                                                   |
| Last Update               | The last time one of the minor fragments sent a status update to the Foreman. Time is shown in 24-hour notation.                                                            |
| Last Progress             | The last time one of the minor fragments made progress, such as a change in fragment state or read data from disk. Time is shown in 24-hour notation.                       |
| Max Peak Memory           | The maximum of the peak direct memory allocated to any minor fragment.                                                                                                      |

## Major Fragment Block  

Shows metrics for the minor fragments that were parallelized for each major fragment.  

The following table lists descriptions for each column in a major fragment block:  

| Column Name       | Description                                                                                                                                                                                                        |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Minor Fragment ID | The coordinate ID of the minor fragment that was parallelized from the major fragment. For example, 02-03-xx where 02 is the Major Fragment ID, 03 is the Minor Fragment ID, and xx corresponds to an operator ID. |
| Host              | The node on which the minor fragment carried out its task.                                                                                                                                                         |
| Start             | The amount of time passed before the minor fragment started its task.                                                                                                                                              |
| End               | The amount of time passed before the minor fragment finished its task.                                                                                                                                             |
| Runtime           | The duration of time for the fragment to complete a task. This value equals the difference between End and Start time.                                                                                             |
| Max Records       | The maximum number of records consumed by an operator from a single input stream.                                                                                                                                  |
| Max Batches       | The maximum number of input batches across input streams, operators, and minor fragments.                                                                                                                          |
| Last Update       | The last time this fragment sent a status update to the Foreman. Time is shown in 24-hour notation.                                                                                                                |
| Last Progress     | The last time this fragment made progress, such as a change in fragment state or reading data from disk. Time is shown in 24-hour notation.                                                                        |
| Peak Memory       | The peak direct memory allocated during execution for this minor fragment.                                                                                                                                         |
| State             | The status of the minor fragment; either finished, running, cancelled, or failed.                                                                                                                                  |


## Operator Overview  Table  

Shows aggregate metrics for each operator within a major fragment that performed relational operations during query execution.
 
The following table lists descriptions for each column in the Operator Overview table:

| Column Name                                          | Description                                                                                                                                                                                                                   |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Operator ID                                          | The coordinates of an operator that performed an operation during a particular phase of the query. For example, 02-xx-03 where 02 is the Major Fragment ID, xx corresponds to a Minor Fragment ID, and 03 is the Operator ID. |
| Type                                                 | The operator type. Operators can be of type project, filter, hash join, single sender, or unordered receiver.                                                                                                                 |
| Min Setup Time, Avg Setup Time, Max Setup Time       | The minimum, average, and maximum amount of time spent by the operator to set up before performing the operation.                                                                                                             |
| Min Process Time, Avg Process Time, Max Process Time | The minimum, average, and maximum  amount of time spent by the operator to perform the operation.                                                                                                                             |
| Wait (min, avg, max)                                 | These fields represent the minimum, average,  and maximum cumulative times spent by operators waiting for external resources.                                                                                                 |
| Avg Peak Memory                                      | Represents the average of the peak direct memory allocated across minor fragments. Relates to the memory needed by operators to perform their operations, such as hash join or sort.                                          |
| Max Peak Memory                                      | Represents the maximum of the peak direct memory allocated across minor fragments. Relates to the memory needed by operators to perform their operations, such as  hash join or sort.                                         |  

## Operator Block  

Shows time and memory metrics for each operator type within a major fragment.  

The following table provides descriptions for each column presented in the operator block:  

| Column Name    | Description                                                                                                                                                                                              |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Minor Fragment | The coordinate ID of the minor fragment on which the operator ran. For example, 04-03-01 where 04 is the Major Fragment ID, 03 is the Minor Fragment ID, and 01 is the Operator ID.                      |
| Setup Time     | The amount of time spent by the operator to set up before performing its operation. This includes run-time code generation and opening a file.                                                           |
| Process Time   | The amount of time spent by the operator to perform its operation.                                                                                                                                       |
| Wait Time      | The cumulative amount of time spent by an operator waiting for external resources. such as waiting to send records, waiting to receive records, waiting to write to disk, and waiting to read from disk. |
| Max Batches    | The maximum number of record batches consumed from a single input stream.                                                                                                                                |
| Max Records    | The maximum number of records consumed from a single input stream.                                                                                                                                       |
| Peak Memory    | Represents the peak direct memory allocated. Relates to the memory needed by the operators to perform their operations, such as  hash join and sort.                                                     |  


