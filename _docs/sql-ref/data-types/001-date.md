---
title: "Date, Time, and Timestamp"
parent: "Data Types"
---
Using familiar date and time formats, listed in the [SQL data types table](/docs/data-types/supported-data-types), you can construct query date and time data. You need to cast textual data to date and time data types. The format of date, time, and timestamp text in a textual data source needs to match the SQL query format for successful casting. 

DATE, TIME, and TIMESTAMP store values in Coordinated Universal Time (UTC). Currently, Drill does not support casting a TIMESTAMP with time zone, but you can use the [TO_TIMESTAMP function](/docs/casting/converting-data-types#to_timestamp) in a query to use time stamp data having a time zone.

Next, use the following literals in a SELECT statement. 

* `date`
* `time`
* `timestamp`

        SELECT date '2010-2-15' FROM sys.drillbits;
        +------------+
        |   EXPR$0   |
        +------------+
        | 2010-02-15 |
        +------------+
        1 row selected (0.083 seconds)

        SELECT time '15:20:30' from sys.drillbits;
        +------------+
        |   EXPR$0   |
        +------------+
        | 15:20:30   |
        +------------+
        1 row selected (0.067 seconds)

        SELECT timestamp '2015-03-11 6:50:08' FROM sys.drillbits;
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

You can run the query described earlier to check the formatting of the fields. The input to the following SELECT statements show how to format INTERVAL data in the query. The output shows how to format the data in the data source.

    SELECT INTERVAL '1 10:20:30.123' day to second FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1DT37230.123S |
    +------------+
    1 row selected (0.054 seconds)

    SELECT INTERVAL '1-2' year to month FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y2M      |
    +------------+
    1 row selected (0.927 seconds)

    SELECT INTERVAL '1' year FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y        |
    +------------+
    1 row selected (0.088 seconds)

    SELECT INTERVAL '13' month FROM sys.drillbits;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y1M      |
    +------------+
    1 row selected (0.076 seconds)

For information about casting interval data, see the ["CAST"](/docs/data-type-fmt#cast) function.


