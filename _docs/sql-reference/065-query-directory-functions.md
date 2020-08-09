---
title: "Query Directory Functions"
date: 2020-08-08
parent: "SQL Reference"
---
You can use the following query directory functions when [querying multiple files or directories]({{site.baseurl}}/docs/querying-directories):

* MAXDIR
* MINDIR
* IMAXDIR
* IMINDIR

The query directory functions restrict a query to one of a number of subdirectories. For example, suppose you had time-series data in subdirectories named 2015, 2014, and 2013. You could use the MAXDIR function to get the latest data and MINDIR to get the earliest data.

In the case where the directory names contain alphabetic characters, the MAXDIR and MINDIR functions return the highest or lowest values, respectively in a case-sensitive string ordering. The IMAXDIR and IMINDIR functions return the corresponding values with [case-insensitive ordering](https://support.office.com/en-za/article/Sort-records-in-case-sensitive-order-8fea1de4-6189-40e7-9359-00cd7d7845c0?ui=en-US&rs=en-ZA&ad=ZA).

The query directory functions are recommended instead of the MAX or MIN aggregate functions to prevent Drill from scanning all data in directories.

## Query Directory Function Syntax

The following syntax shows how to construct a SELECT statement that using the MAXDIR function:

       SELECT * FROM <plugin>.<workspace>.`<filename>`
       WHERE dir<n> = MAXDIR('<plugin>.<workspace>'[, '<filename>']);

Enclose query directory function arguments in single-quotation marks, not back ticks. The first argument to the function is the plugin and workspace names in dot notation. The second argument is the directory name. The directory name is an optional argument. The dir variable, dir0, dir1, and so on, refers to subdirectories in your workspace path, as explained in ["Querying Directories"]({{site.baseurl}}/docs/querying-directories). 

## Query Directory Function Example 

This example creates a top-level directory called `querylogs` in the `/tmp` directory. Using the `/tmp` directory is convenient for example purposes because `/tmp` is predefined as a workspace in the default `dfs` storage plugin. In the `querylogs` directory, you create three subdirectories:

* 2015
* 2014
* 2013

In each subdirectory, you create a CSV file that contains arbitrary log data. Issue MAXDIR and MINDIR queries to exercise the query directory functions:

    SELECT * FROM dfs.tmp.`querylogs` WHERE dir0 = MAXDIR('dfs.tmp','querylogs');
    |--------------------------------|------|
    | columns                        | dir0 |
    |--------------------------------|------|
    | ["2015","some arbitrary data"] | 2015 |
    |--------------------------------|------|
    1 row selected (0.112 seconds)

    SELECT * FROM dfs.tmp.`querylogs` WHERE dir0 = MINDIR('dfs.tmp','querylogs');
    |---------------------------|------|
    | columns                   | dir0 |
    |---------------------------|------|
    | ["2013","even more data"] | 2013 |
    |---------------------------|------|
    1 row selected (0.119 seconds)

In this example, using any variable other than dir0 does not return results because the subdirectory level nesting is only one level deep.


