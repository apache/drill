---
title: "Date, Time, and Timestamp"
date: 2017-04-04 00:05:30 UTC
parent: "Data Types"
---
Using familiar date and time formats, listed in the [SQL data types table]({{ site.baseurl }}/docs/supported-data-types), you can construct query date and time data. You need to cast textual data to date and time data types. The format of date, time, and timestamp text in a textual data source needs to match the SQL query format for successful casting. Drill supports date, time, timestamp, and interval literals shown in the following example:

    SELECT DATE '2008-2-23', 
           TIME '12:23:34', 
           TIMESTAMP '2008-2-23 12:23:34.456', 
           INTERVAL '1' YEAR, INTERVAL '2' DAY, 
           DATE_ADD(DATE '2008-2-23', INTERVAL '1 10:20:30' DAY TO SECOND), 
           DATE_ADD(DATE '2010-2-23', 1)
    FROM (VALUES (1));
    +-------------+-----------+--------------------------+---------+---------+------------------------+-------------+
    |   EXPR$0    |  EXPR$1   |          EXPR$2          | EXPR$3  | EXPR$4  |         EXPR$5         |   EXPR$6    |
    +-------------+-----------+--------------------------+---------+---------+------------------------+-------------+
    | 2008-02-23  | 12:23:34  | 2008-02-23 12:23:34.456  | P1Y     | P2D     | 2008-02-24 10:20:30.0  | 2010-02-24  |
    +-------------+-----------+--------------------------+---------+---------+------------------------+-------------+

## INTERVAL

The INTERVAL YEAR and INTERVAL DAY internal types represent a period of time. The INTERVAL YEAR type specifies values from a year to a month. The INTERVAL DAY type specifies values from a day to seconds.

### Interval in Data Source

If your interval data is in the data source, you need to cast the data to an SQL interval type to query the data using Drill. For example, to use interval data in a JSON file, cast the JSON data, which is of the VARCHAR type, to INTERVAL YEAR and INTERVAL DAY using the following ISO 8601 syntax:

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

### Using the Interval Literal in Input

When you want to use interval data in input, use INTERVAL as a keyword that introduces an interval literal that denotes a data type. With the input of interval data, use the following SQL literals to restrict the set of stored interval fields:

* YEAR
* MONTH
* DAY
* HOUR
* MINUTE
* SECOND
* YEAR TO MONTH
* DAY TO HOUR
* DAY TO MINUTE
* DAY TO SECOND
* HOUR TO MINUTE
* HOUR TO SECOND
* MINUTE TO SECOND

### Interval in a Data Source Example

To cast interval data to interval types you can query from a data source such as JSON, see the example in the section, ["Casting Intervals"]({{site.baseurl}}/docs/data-type-conversion/#casting-intervals).

### Literal Interval Examples

In the following example, the INTERVAL keyword followed by 200 adds 200 years to the timestamp. The 3 in parentheses in `YEAR(3)` specifies the precision of the year interval, 3 digits in this case to support the hundreds interval.

    SELECT CURRENT_TIMESTAMP + INTERVAL '200' YEAR(3) FROM (VALUES(1));
    +--------------------------+
    |          EXPR$0          |
    +--------------------------+
    | 2215-08-14 15:18:00.094  |
    +--------------------------+
    1 row selected (0.096 seconds)

The following examples show the input and output format of INTERVAL YEAR (Year, Month) and INTERVAL DAY (Day, Hours, Minutes, Seconds, Milliseconds). The following SELECT statements show how to format the query input. The output shows how to format the data in the data source.

    SELECT INTERVAL '1 10:20:30.123' day to second FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | P1DT37230.123S |
    +------------+
    1 row selected (0.054 seconds)

    SELECT INTERVAL '1-2' year to month FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y2M      |
    +------------+
    1 row selected (0.927 seconds)

    SELECT INTERVAL '1' year FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y        |
    +------------+
    1 row selected (0.088 seconds)

    SELECT INTERVAL '13' month FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | P1Y1M      |
    +------------+
    1 row selected (0.076 seconds)


## DATE, TIME, and TIMESTAMP

DATE, TIME, and TIMESTAMP literals. Drill stores values in Coordinated Universal Time (UTC). Drill supports time functions in the range 1971 to 2037.

Drill does not support TIMESTAMP with time zone; however, if your data includes the time zone, use the [TO_TIMESTAMP function]({{ site.baseurl }}/docs/casting/converting-data-types/#to_timestamp) and [Joda format specifiers]({{site.baseurl}}/docs/data-type-conversion/#format-specifiers-for-date/time-conversions) as shown the examples in section, ["Time Zone Limitation"]({{site.baseurl}}/docs/data-type-conversion/#time-zone-limitation).

Next, use the following literals in a SELECT statement. 

* `date`
* `time`
* `timestamp`

        SELECT date '2010-2-15' FROM (VALUES(1));
        +------------+
        |   EXPR$0   |
        +------------+
        | 2010-02-15 |
        +------------+
        1 row selected (0.083 seconds)

        SELECT time '15:20:30' from (VALUES(1));
        +------------+
        |   EXPR$0   |
        +------------+
        | 15:20:30   |
        +------------+
        1 row selected (0.067 seconds)

        SELECT timestamp '2015-03-11 6:50:08' FROM (VALUES(1));
        +------------+
        |   EXPR$0   |
        +------------+
        | 2015-03-11 06:50:08.0 |
        +------------+
        1 row selected (0.071 seconds)


