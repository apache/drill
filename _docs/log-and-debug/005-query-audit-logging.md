---
title: "Query Audit Logging"
date: 2020-08-08
parent: "Log and Debug"
---
The query log provides audit log functionality for the queries executed by various drillbits in the cluster. The log records important information about queries executed on the Drillbit where Drill runs. The log includes the following information:  

* query text
* start/end time
* status
* schema
* query id
* name of the user that launched the query
* client IP address from which the query was launched 

You can query the following log files to get audit logging information:

* `sqlline_queries.json` (embedded mode) 
* `drillbit_queries.json` (distributed mode)

## Checking the Most Recent Queries

For example, to check the most recent queries, query the log using this command:

    SELECT * FROM dfs.`default`.`/Users/drill-user/apache-drill-1.1.0/log/sqlline_queries.json` t ORDER BY `start` LIMIT 5;

    |----------------|------------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------------|------------|
    |     finish     |  outcome   |                queryId                |                                                            queryText                                                                                                                                         | schema  |     start      |  username  |
    |----------------|------------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------------|------------|
    | 1431752662216  | FAILED     |  2aa9302b-bf6f-a378-d66e-151834e87b16 | select * from dfs.`default`.`/Users/nrentachintala/Downloads/testgoogle.json` t limit 1                                                                                                                      |         | 1431752660376  |  anonymous |
    | 1431752769079  | COMPLETED  |  2aa92fc1-b722-c27a-10f7-57a1cf0dd366 | SELECT KVGEN(checkin_info) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` LIMIT 2                                                                               |         | 1431752765303  |  anonymous |
    | 1431752786341  | COMPLETED  |  2aa92faf-2103-047b-9761-32eedefba1e6 | SELECT FLATTEN(KVGEN(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` LIMIT 20                                                                     |         | 1431752784532  |  anonymous |
    | 1431752809084  | FAILED     |  2aa92f97-61d3-1e9a-97b0-c754f5b568d5 | SELECT SUM(checkintbl.checkins.`value`) AS TotalCheckins FROM (SELECT FLATTEN(KVGEN(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` ) checkintbl  |         | 1431752808923  |  anonymous |
    | 1431752853992  | COMPLETED  |  2aa92f87-0250-c6ac-3700-9ae1f98435b8 | SELECT SUM(checkintbl.checkins.`value`) AS TotalCheckins FROM (SELECT FLATTEN(KVGEN(checkin_info)) checkins FROM dfs.`/users/nrentachintala/Downloads/yelp/yelp_academic_dataset_checkin.json` ) checkintbl  |         | 1431752824947  |  anonymous |
    |----------------|------------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------------|------------|
    5 rows selected (0.532 seconds)

{% include startnote.html %}This document aligns Drill output for example purposes. Drill output is not aligned in this case.{% include endnote.html %}

## Checking Drillbit Traffic

To check the total number of queries executed since the session started on the Drillbit, use the following command:

    SELECT COUNT(*) FROM dfs.`default`.`/Users/drill-user/apache-drill-1.1.0/log/sqlline_queries.json`;

    |---------|
    | EXPR$0  |
    |---------|
    | 32      |
    |---------|
    1 row selected (0.144 seconds)

## Getting Query Success Statistics

To get the total number of successful and failed executions, run the following command:

    SELECT outcome, COUNT(*) FROM dfs.`default`.`/Users/drill-user/apache-drill-1.1.0/log/sqlline_queries.json` GROUP BY outcome;

    |------------|---------|
    |  outcome   | EXPR$1  |
    |------------|---------|
    | COMPLETED  | 18      |
    | FAILED     | 14      |
    |------------|---------|
    2 rows selected (0.219 seconds)

Note the queryid column in the audit can be correlated with the profiles of the queries for troubleshooting/diagnostics purposes.
