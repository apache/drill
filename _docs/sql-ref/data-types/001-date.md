---
title: "Date, Time, and Timestamp"
parent: "Data Types"
---
Using familiar date and time formats, listed in the [SQL data types table](/docs/data-types), you can construct query date and time data. You need to cast textual data to date and time data types. The format of date, time, and timestamp text in a textual data source needs to match the SQL query format for successful casting. 

DATE, TIME, and TIMESTAMP store values in Coordinated Universal Time (UTC). Currently, Drill does not support casting a TIMESTAMP with time zone, but you can use the TO_TIMESTAMP function (link to example) in a query to use time stamp data having a time zone.


Before running a query, you can check the formatting of your dates and times. First, create a dummy JSON file to use in the FROM clause for testing queries as shown in the following examples. 
    {"dummy" : "data"}. 

Next, use the following literals in a SELECT statement. 

* `date`
* `time`
* `timestamp`

        SELECT date '2010-2-15' FROM dfs.`/Users/drilluser/apache-drill-0.8.0/dummy.json`;
        +------------+
        |   EXPR$0   |
        +------------+
        | 2010-02-15 |
        +------------+
        1 row selected (0.083 seconds)

        SELECT time '15:20:30' from dfs.`/Users/drilluser/apache-drill-0.8.0/dummy.json`;
        +------------+
        |   EXPR$0   |
        +------------+
        | 15:20:30   |
        +------------+
        1 row selected (0.067 seconds)

        SELECT timestamp '2015-03-11 6:50:08' FROM dfs.`/Users/drilluser/apache-drill-0.8.0/dummy.json`;
        +------------+
        |   EXPR$0   |
        +------------+
        | 2015-03-11 06:50:08.0 |
        +------------+
        1 row selected (0.071 seconds)

## INTERVAL

The INTERVAL type represents a period of time. Use ISO 8601 syntax to format a value of this type:

    P [qty] Y [qty] M [qty] D T [qty] H [qty] M [qty] S

    P [qty] D T [qty] H [qty] M [qty] S

    P [qty] Y [qty] M

where:

* P (Period) marks the beginning of a period of time.
* Y follows a number of years.
* M follows a number of months.
* D follows a number of days.
* H follows a number of hours 0-24.
* M follows a number of minutes.
* S follows a number of seconds and optional milliseconds to the right of a decimal point


INTERVALYEAR (Year, Month) and INTERVALDAY (Day, Hours, Minutes, Seconds, Milliseconds) are a simpler version of INTERVAL with a subset of the fields.  You do not need to specify all fields.

The format of INTERVAL data in the data source differs from the query format. 

You can run the dummy query described earlier to check the formatting of the fields. The input to the following SELECT statements show how to format INTERVAL data in the query. The output shows how to format the data in the data source.

    SELECT INTERVAL '1 10:20:30.123' day to second FROM dfs.`/Users/drilluser/apache-drill-0.8.0/dummy.json`;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1DT37230.123S |
    +------------+
    1 row selected (0.054 seconds)

    SELECT INTERVAL '1-2' year to month FROM dfs.`/Users/khahn/drill/apache-drill-0.8.0-SNAPSHOT/dummy.json`;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y2M      |
    +------------+
    1 row selected (0.927 seconds)

    SELECT INTERVAL '1' year FROM dfs.`/Users/khahn/drill/apache-drill-0.8.0-SNAPSHOT/dummy.json`;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y        |
    +------------+
    1 row selected (0.088 seconds)

    SELECT INTERVAL '13' month FROM dfs.`/Users/khahn/drill/apache-drill-0.8.0-SNAPSHOT/dummy.json`;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y1M      |
    +------------+
    1 row selected (0.076 seconds)

To cast INTERVAL data use the following syntax:

    CAST (column_name AS INTERVAL)
    CAST (column_name AS INTERVAL DAY)
    CAST (column_name AS INTERVAL YEAR)

## Interval Example
A JSON file contains the following objects:

    { "INTERVALYEAR_col":"P1Y", "INTERVALDAY_col":"P1D", "INTERVAL_col":"P1Y1M1DT1H1M" }
    { "INTERVALYEAR_col":"P2Y", "INTERVALDAY_col":"P2D", "INTERVAL_col":"P2Y2M2DT2H2M" }
    { "INTERVALYEAR_col":"P3Y", "INTERVALDAY_col":"P3D", "INTERVAL_col":"P3Y3M3DT3H3M" }

The following CTAS statement shows how to cast text from a JSON file to INTERVAL data types in a Parquet table:

    CREATE TABLE dfs.tmp.parquet_intervals AS 
    (SELECT cast (INTERVAL_col as interval),
           cast( INTERVALYEAR_col as interval year) INTERVALYEAR_col, 
           cast( INTERVALDAY_col as interval day) INTERVALDAY_col 
    FROM `/user/root/intervals.json`);

<!-- Text and include output -->



