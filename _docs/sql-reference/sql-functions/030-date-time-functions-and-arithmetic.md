---
title: "Date/Time Functions and Arithmetic"
date: 2019-04-10
parent: "SQL Functions"
---

In addition to the TO_DATE, TO_TIME, and TO_TIMESTAMP functions, Drill supports a number of other date/time functions and arithmetic operators for use with dates, times, and intervals. Drill supports time functions based on the Gregorian calendar and in the range 1971 to 2037.

This section covers the Drill [time zone limitation]({{site.baseurl}}/docs/data-type-conversion/#time-zone-limitation) and defines the following date/time functions:

**Function**| **Return Type**  
---|---  
[AGE(TIMESTAMP)]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#age)                               | INTERVALDAY or INTERVALYEAR
[EXTRACT(field from time_expression)]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#extract)      | DOUBLE
[CURRENT_DATE]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#other-date-and-time-functions)      | DATE  
[CURRENT_TIME]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#other-date-and-time-functions)      | TIME   
[CURRENT_TIMESTAMP]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#other-date-and-time-functions) | TIMESTAMP 
[DATE_ADD]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#date_add)                                | DATE, TIMESTAMP  
[DATE_DIFF]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#date_diff)                              | DATE, TIMESTAMP
[DATE_PART]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#date_part)                              | DOUBLE  
[DATE_SUB]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#date_sub)                                | DATE, TIMESTAMP     
[LOCALTIME]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#other-date-and-time-functions)         | TIME  
[LOCALTIMESTAMP]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#other-date-and-time-functions)    | TIMESTAMP  
[NOW]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#other-date-and-time-functions)               | TIMESTAMP  
[TIMEOFDAY]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#other-date-and-time-functions)         | VARCHAR  
[UNIX_TIMESTAMP]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#unix_timestamp)                   | BIGINT 
[NEARESTDATE]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic/#nearestdate)**                       | TIMESTAMP
[TIMESTAMPADD]({{site.baseurl}}/docs/date-time-functions-and-arithmetic/#timestampadd)*                     | Inferred based on unit of time
[TIMESTAMPDIFF]({{site.baseurl}}/docs/date-time-functions-and-arithmetic/#timestampdiff)*					   | Inferred based on unit of time												   |  
|   

*Supported in Drill 1.15 and later.
**Supported in Drill 1.16 and later.

## AGE
Returns the interval between two timestamps or subtracts a timestamp from midnight of the current date.

### AGE Syntax

`AGE (timestamp[, timestamp])`

*timestamp* is the data and time formatted as shown in the following examples.

### AGE Usage Notes
Cast string arguments to timestamp to include time data in the calculations of the interval.

### AGE Examples

Find the interval between midnight today, April 3, 2015, and June 13, 1957.

    SELECT AGE('1957-06-13') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | P703M23D   |
    +------------+
    1 row selected (0.064 seconds)

Find the interval between midnight today, May 21, 2015, and hire dates of employees 578 and 761 in the [`employee.json`]({{site.baseurl}}/docs/querying-json-files/) file installed with Drill. The file is installed with Drill and located in the Drill classpath.

    SELECT AGE(CAST(hire_date AS TIMESTAMP)) FROM cp.`employee.json` where employee_id IN( '578','761');
    +------------------+
    |      EXPR$0      |
    +------------------+
    | P236MT25200S     |
    | P211M19DT25200S  |
    +------------------+
    2 rows selected (0.121 seconds)

Find the interval between 11:10:10 PM on January 1, 2001 and 10:10:10 PM on January 1, 2001.

    SELECT AGE(CAST('2010-01-01 10:10:10' AS TIMESTAMP), CAST('2001-01-01 11:10:10' AS TIMESTAMP)) FROM (VALUES(1));
    +------------------+
    |      EXPR$0      |
    +------------------+
    | P109M16DT82800S  |
    +------------------+
    1 row selected (0.122 seconds)

For information about how to read the interval data, see the [Interval section]({{ site.baseurl }}/docs/date-time-and-timestamp/#intervalyear-and-intervalday).

## DATE_ADD
Returns the sum of a date/time and a number of days/hours, or of a date/time and date/time interval.

### DATE_ADD Syntax  
- `DATE_ADD(keyword literal, integer)`  
- `DATE_ADD(keyword literal, interval expr)`  
- `DATE_ADD(column, integer)`  
- `DATE_ADD(column, interval expr)`  

*keyword* is the word date, time, or timestamp.  
*literal* is a date, time, or timestamp literal.  For example, a date in yyyy-mm-dd format enclosed in single quotation marks.  
*integer* is a number of days to add to the date/time.  
*column* is date, time, or timestamp data in a data source column.  
*interval* is the keyword interval.  
*expr* is an interval expression, such as the name of a data source column containing interval data.  

### DATE_ADD Examples

The following examples show how to use the syntax variations.

**`DATE_ADD(keyword literal, integer)` Syntax Example**

Add two days to today's date May 15, 2015.

    SELECT DATE_ADD(date '2015-05-15', 2) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-05-17 |
    +------------+
    1 row selected (0.07 seconds)

**`DATE_ADD(keyword literal, interval expr)` Syntax Example**

Using the example data from the ["Casting Intervals"]({{site.baseurl}}/docs/data-type-conversion/#casting-intervals) section, add intervals from the `intervals.json` file to a literal timestamp. Create an interval expression that casts the INTERVALDAY_col column, which contains P1D, P2D, and P3D, to a timestamp.

    SELECT DATE_ADD(timestamp '2015-04-15 22:55:55', CAST(INTERVALDAY_col as interval second)) FROM dfs.`/Users/drilluser/apache-drill-1.0.0/intervals.json`;
    +------------------------+
    |         EXPR$0         |
    +------------------------+
    | 2015-04-16 22:55:55.0  |
    | 2015-04-17 22:55:55.0  |
    | 2015-04-18 22:55:55.0  |
    +------------------------+
    3 rows selected (0.105 seconds)

The query output is the sum of the timestamp and 1, 2, and 3 days corresponding to P1D, P2D, and P3D.

**DATE_ADD(column, integer) Syntax Example**

Add two days to the value in the birth_date column.

    SELECT DATE_ADD(CAST(birth_date AS date), 2) FROM cp.`employee.json` LIMIT 1;
    +-------------+
    |   EXPR$0    |
    +-------------+
    | 1961-08-28  |
    +-------------+
    1 row selected (0.209 seconds)

**`DATE_ADD(column, interval expr)` Syntax Example**

Add a 10 hour interval to the hire dates of employees listed in the `employee.json` file, which Drill includes in the installation.

1. Take a look at the employee data:

        SELECT * FROM cp.`employee.json` LIMIT 1;
        +--------------+---------------+-------------+------------+--------------+-----------------+-----------+----------------+-------------+------------------------+----------+----------------+------------------+-----------------+---------+--------------------+
        | employee_id  |   full_name   | first_name  | last_name  | position_id  | position_title  | store_id  | department_id  | birth_date  |       hire_date        |  salary  | supervisor_id  | education_level  | marital_status  | gender  |  management_role   |
        +--------------+---------------+-------------+------------+--------------+-----------------+-----------+----------------+-------------+------------------------+----------+----------------+------------------+-----------------+---------+--------------------+
        | 1            | Sheri Nowmer  | Sheri       | Nowmer     | 1            | President       | 0         | 1              | 1961-08-26  | 1994-12-01 00:00:00.0  | 80000.0  | 0              | Graduate Degree  | S               | F       | Senior Management  |
        +--------------+---------------+-------------+------------+--------------+-----------------+-----------+----------------+-------------+------------------------+----------+----------------+------------------+-----------------+---------+--------------------+
        1 row selected (0.137 seconds)

2. Look at the hire_dates for the employee 578 and 761 in `employee.json`.

        SELECT hire_date FROM cp.`employee.json` where employee_id IN( '578','761');
        +------------------------+
        |       hire_date        |
        +------------------------+
        | 1996-01-01 00:00:00.0  |
        | 1998-01-01 00:00:00.0  |
        +------------------------+
        2 rows selected (0.135 seconds)

3. Cast the hire_dates of the employees 578 and 761 to a timestamp, and add 10 hours to the hire_date timestamp. Because Drill reads data from JSON as VARCHAR, you need to cast the hire_date to the TIMESTAMP type. 

        SELECT DATE_ADD(CAST(hire_date AS TIMESTAMP), interval '10' hour) FROM cp.`employee.json` where employee_id IN( '578','761');
        +------------------------+
        |         EXPR$0         |
        +------------------------+
        | 1996-01-01 10:00:00.0  |
        | 1998-01-01 10:00:00.0  |
        +------------------------+
        2 rows selected (0.172 seconds)

**`DATE_ADD(keyword literal, integer)` Syntax Example**

Add 1 year and 1 month to the timestamp 2015-04-15 22:55:55.

    SELECT DATE_ADD(timestamp '2015-04-15 22:55:55', interval '1-2' year to month) FROM (VALUES(1));
    +------------------------+
    |         EXPR$0         |
    +------------------------+
    | 2016-06-15 22:55:55.0  |
    +------------------------+
    1 row selected (0.106 seconds)

Add 1 day 2 and 1/2 hours and 45.100 seconds to the time 22:55:55.

    SELECT DATE_ADD(time '22:55:55', interval '1 2:30:45.100' day to second) FROM (VALUES(1));
    +---------------+
    |    EXPR$0     |
    +---------------+
    | 01:26:40.100  |
    +---------------+
    1 row selected (0.106 seconds)  

## DATE_DIFF  
Returns the difference of a date/time and a number of days/hours, or of a date/time and date/time interval.  

**NOTE:** You can use the Hive `DATEDIFF()` function in Drill with string values, as shown:  
 
	SELECT DATEDIFF('1996-03-01', '1997-02-10 17:32:00.0'), TIMEOFDAY() FROM (VALUES(1));
	+---------+----------------------------------------------+
	| EXPR$0  |                    EXPR$1                    |
	+---------+----------------------------------------------+
	| -346    | 2019-01-15 14:54:21.455 America/Los_Angeles  |
	+---------+----------------------------------------------+    

However, for date and timestamp values, use the `DATE_DIFF` function and convert the interval returned to a number, as shown:    

	SELECT TO_NUMBER(DATE_DIFF(DATE '1996-03-01', TIMESTAMP '1997-02-10 17:32:00.0'), '#'), TIMEOFDAY() FROM (VALUES(1));
	+---------+----------------------------------------------+
	| EXPR$0  |                    EXPR$1                    |
	+---------+----------------------------------------------+
	| -346.0  | 2019-01-15 14:52:15.247 America/Los_Angeles  |
	+---------+----------------------------------------------+

### DATE_DIFF Syntax  
- `DATE_DIFF(keyword literal, integer)`  
- `DATE_DIFF(keyword literal, interval expr)`   
- `DATE_DIFF(column, integer)`  
- `DATE_DIFF(column, interval expr)`  

*keyword* is the word date, time, or timestamp.  
*literal* is a date, time, or timestamp literal.  For example, a date in yyyy-mm-dd format enclosed in single quotation marks.  
*integer* is a number of days to subtract from the date/time.  
*column* is date, time, or timestamp data in a data source column.  
*interval* is the keyword interval.  
*expr* is an interval expression, such as the name of a data source column containing interval data.  

### DATE_DIFF Examples  
The following examples show how to use the syntax variations.  

**DATE_DIFF(keyword literal, integer) Syntax Example**  

Subtract two days from the date May 15, 2015.

	SELECT DATE_DIFF(date '2015-05-15', 2) FROM (VALUES(1));
	+-------------+
	|   EXPR$0    |
	+-------------+
	| 2015-05-13  |
	+-------------+  

**DATE_DIFF(keyword literal, interval expr) Syntax Example**  

Using the example data from the ["Casting Intervals"]({{site.baseurl}}/docs/data-type-conversion/#casting-intervals)  section, subtract intervals from the `intervals.json` file from a literal timestamp. Create an interval expression that casts the INTERVALDAY_col column, which contains P1D, P2D, and P3D, to a timestamp.  

	SELECT DATE_DIFF(timestamp '2015-04-15 22:55:55', CAST(INTERVALDAY_col as interval second)) FROM dfs.`/home/bee/intervals.json`;  
	+------------------------+
	|         EXPR$0         |
	+------------------------+
	| 2015-04-14 22:55:55.0  |
	| 2015-04-13 22:55:55.0  |
	| 2015-04-12 22:55:55.0  |
	+------------------------+  

The query output is the difference of the timestamp and 1, 2, and 3 days corresponding to P1D, P2D, and P3D.  

**DATE_DIFF(column, integer) Syntax Example**  

Subtract two days from the value in the birth_date column.  

	SELECT DATE_DIFF(CAST(birth_date AS date), 2) as a FROM cp.`employee.json` LIMIT 1;
	+-------------+
	|      a      |
	+-------------+
	| 1961-08-24  |
	+-------------+  

**DATE_DIFF(column, interval expr) Syntax Example**  

Subtract a 10 hour interval from the hire dates of employees listed in the `employee.json` file, which Drill includes in the installation.

Cast the hire_dates of the employees 578 and 761 to a timestamp, and subtract 10 hours from the hire_date timestamp. Drill reads data from JSON as VARCHAR; therefore, cast the hire_date to the TIMESTAMP type.

	SELECT DATE_DIFF(CAST(hire_date AS TIMESTAMP), interval '10' hour) FROM cp.`employee.json` where employee_id IN( '578','761');
	+------------------------+
	|         EXPR$0         |
	+------------------------+
	| 1995-12-31 14:00:00.0  |
	| 1997-12-31 14:00:00.0  |
	+------------------------+  

**DATE_DIFF(keyword literal, integer) Syntax Example**  

Subtract 1 year and 2 months from the timestamp 2015-04-15 22:55:55.  

	SELECT DATE_DIFF(timestamp '2015-04-15 22:55:55', interval '1-2' year to month) FROM (VALUES(1));
	+------------------------+
	|         EXPR$0         |
	+------------------------+
	| 2014-02-15 22:55:55.0  |
	+------------------------+  

Subtract 1 day 2 and 1/2 hours and 45.100 seconds from the time 22:55:55.

	SELECT DATE_DIFF(time '22:55:55', interval '1 2:30:45.100' day to second) FROM (VALUES(1));
	+---------------+
	|    EXPR$0     |
	+---------------+
	| 20:25:09.900  |
	+---------------+    


## DATE_PART
Returns a field of a date, time, timestamp, or interval.

### DATE_PART Syntax 

`date_part(keyword, expression)`  

*keyword* is year, month, day, hour, minute, or second enclosed in single quotation marks.  
*expression* is date, time, timestamp, or interval literal enclosed in single quotation marks.

### DATE_PART Usage Notes
Use Unix Epoch timestamp in milliseconds as the expression to get the field of a timestamp.

### DATE_PART Examples

    SELECT DATE_PART('day', '2015-04-02') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 2          |
    +------------+
    1 row selected (0.098 seconds)

    SELECT DATE_PART('hour', '23:14:30.076') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 23         |
    +------------+
    1 row selected (0.088 seconds)

Return the day part of the one year, 2 months, 10 days interval.

    SELECT DATE_PART('day', '1:2:10') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 10         |
    +------------+
    1 row selected (0.069 seconds)

## DATE_SUB
Returns the difference between a date/time and a number of days/hours, or between a date/time and date/time interval.

### DATE_SUB Syntax

`DATE_SUB(keyword literal, integer)`  
`DATE_SUB(keyword literal, interval expr)`  
`DATE_SUB(column, integer)`  
`DATE_SUB(column, interval expr)`  

*keyword* is the word date, time, or timestamp.  
*literal* is a date, time, or timestamp literal. For example, a date in yyyy-mm-dd format enclosed in single quotation marks.   
*integer* is a number of days to subtract from the date, time, or timestamp.  
*column* is date, time, or timestamp data in the data source.  
*interval* is the keyword interval.  
*expr* is an interval expression, such as the name of a data source column containing interval data.  

### DATE_SUB Examples
The following examples show how to apply the syntax variations.

**`DATE_SUB(keyword literal, integer)` Syntax Example**

Subtract two days from today's date May 15, 2015.

    SELECT DATE_SUB(date '2015-05-15', 2) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-05-13 |
    +------------+
    1 row selected (0.088 seconds)

Subtact two months from April 15, 2015.

    SELECT DATE_SUB(date '2015-04-15', interval '2' month) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-02-15 |
    +------------+
    1 row selected (0.088 seconds)

**`DATE_SUB(keyword literal, interval expr)` Syntax Example**

Subtract 10 hours from the timestamp 2015-04-15 22:55:55.

    SELECT DATE_SUB(timestamp '2015-04-15 22:55:55', interval '10' hour) FROM (VALUES(1));
    +------------------------+
    |         EXPR$0         |
    +------------------------+
    | 2015-04-15 12:55:55.0  |
    +------------------------+
    1 row selected (0.108 seconds)

Subtract 10 hours from the time 22 hours, 55 minutes, 55 seconds.

    SELECT DATE_SUB(time '22:55:55', interval '10' hour) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 12:55:55   |
    +------------+
    1 row selected (0.079 seconds)

Subtract 1 year and 1 month from the timestamp 2015-04-15 22:55:55.

    SELECT DATE_SUB(timestamp '2015-04-15 22:55:55', interval '1-2' year to month) FROM (VALUES(1));
    +------------------------+
    |         EXPR$0         |
    +------------------------+
    | 2014-02-15 22:55:55.0  |
    +------------------------+
    1 row selected (0.108 seconds)

Subtract 1 day, 2 and 1/2 hours, and 45.100 seconds from the time 22:55:55.

    SELECT DATE_ADD(time '22:55:55', interval '1 2:30:45.100' day to second) FROM (VALUES(1));
    +---------------+
    |    EXPR$0     |
    +---------------+
    | 01:26:40.100  |
    +---------------+
    1 row selected (0.095 seconds)

**`DATE_SUB(column, integer)` Syntax Example**

    SELECT DATE_SUB(CAST(birth_date AS date), 2) FROM cp.`employee.json` LIMIT 1;
    +-------------+
    |   EXPR$0    |
    +-------------+
    | 1961-08-24  |
    +-------------+
    1 row selected (0.158 seconds)

**`DATE_SUB(column, interval expr)` Syntax Example**

The `employee.json` file, which Drill includes in the installation, lists the hire dates of employees. Cast the hire_dates of the employees 578 and 761 to a timestamp, and add 10 hours to the hire_date timestamp. Because Drill reads data from JSON as VARCHAR, you need to cast the hire_date to the TIMESTAMP type. 

    SELECT DATE_SUB(CAST(hire_date AS TIMESTAMP), interval '10' hour) FROM cp.`employee.json` WHERE employee_id IN( '578','761');
    +------------------------+
    |         EXPR$0         |
    +------------------------+
    | 1995-12-31 14:00:00.0  |
    | 1997-12-31 14:00:00.0  |
    +------------------------+
    2 rows selected (0.161 seconds)

## Other Date and Time Functions

The following examples show how to use these functions:

* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* LOCALTIME
* LOCALTIMESTAMP
* NOW
* TIMEOFDAY

        SELECT CURRENT_DATE FROM (VALUES(1));
        +--------------+
        | current_date |
        +--------------+
        | 2015-04-02   |
        +--------------+
        1 row selected (0.077 seconds)

        SELECT CURRENT_TIME FROM (VALUES(1));
        +--------------+
        | current_time |
        +--------------+
        | 14:32:04.751 |
        +--------------+
        1 row selected (0.073 seconds)

        SELECT CURRENT_TIMESTAMP FROM (VALUES(1));
        +--------------------------+
        |    CURRENT_TIMESTAMP     |
        +--------------------------+
        | 2015-05-17 22:45:55.848  |
        +--------------------------+
        1 row selected (0.109 seconds)

        SELECT LOCALTIME FROM (VALUES(1));

        +---------------+
        |   LOCALTIME   |
        +---------------+
        | 22:46:19.656  |
        +---------------+
        1 row selected (0.105 seconds)

        SELECT LOCALTIMESTAMP FROM (VALUES(1));

        +--------------------------+
        |      LOCALTIMESTAMP      |
        +--------------------------+
        | 2015-05-17 22:46:47.944  |
        +--------------------------+
        1 row selected (0.08 seconds)

        SELECT NOW() FROM (VALUES(1));
        +--------------------------+
        |          EXPR$0          |
        +--------------------------+
        | 2015-05-17 22:47:11.008  |
        +--------------------------+
        1 row selected (0.085 seconds)

If you set up Drill for [UTC time]({{ site.baseurl }}/docs/data-type-conversion/#time-zone-limitation), TIMEOFDAY returns the result for the UTC time zone.

    SELECT TIMEOFDAY() FROM (VALUES(1));
    +-----------------------------+
    |           EXPR$0            |
    +-----------------------------+
    | 2015-04-02 22:05:02.424 UTC |
    +-----------------------------+
    1 row selected (1.191 seconds)

If you did not set up Drill for UTC time, TIMEOFDAY returns the local date and time with time zone information.

    SELECT TIMEOFDAY() FROM (VALUES(1));
    +----------------------------------------------+
    |                    EXPR$0                    |
    +----------------------------------------------+
    | 2015-05-17 22:47:38.012 America/Los_Angeles  |
    +----------------------------------------------+
    1 row selected (0.08 seconds)

## EXTRACT

Returns a component of a timestamp, time, date, or interval.

### EXTRACT Syntax

`EXTRACT (extract_expression)`  

*extract_expression* is:

    component FROM (timestamp | time | date | interval)

*component* is one of the following time units: year, month, day, hour, minute, second.

### EXTRACT Usage Notes

The extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

### EXTRACT Examples

On the third day of the month, run the following function:

    SELECT EXTRACT(day FROM NOW()), EXTRACT(day FROM CURRENT_DATE) FROM (VALUES(1));

    +------------+------------+
    |   EXPR$0   |   EXPR$1   |
    +------------+------------+
    | 3          | 3          |
    +------------+------------+
    1 row selected (0.208 seconds)

At 8:00 am, extract the hour from the value of CURRENT_DATE.

    SELECT EXTRACT(hour FROM CURRENT_DATE) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 8          |
    +------------+

What is the hour component of this time: 17:12:28.5?

    SELECT EXTRACT(hour FROM TIME '17:12:28.5') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 17         |
    +------------+
    1 row selected (0.056 seconds)

What is the seconds component of this timestamp: 2001-02-16 20:38:40

    SELECT EXTRACT(SECOND FROM TIMESTAMP '2001-02-16 20:38:40') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 40.0       |
    +------------+
    1 row selected (0.062 seconds)


## Date, Time, and Interval Arithmetic Functions

Is the day returned from the NOW function the same as the day returned from the CURRENT_DATE function?

    SELECT EXTRACT(day FROM NOW()) = EXTRACT(day FROM CURRENT_DATE) FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | true       |
    +------------+
    1 row selected (0.092 seconds)

Every 23 hours, a 4 hour task started. What time does the task end? 

    SELECT TIME '04:00:00' + interval '23:00:00' hour to second FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | 03:00:00   |
    +------------+
    1 row selected (0.097 seconds)

Is the time 2:00 PM?

    SELECT EXTRACT(hour FROM CURRENT_DATE) = 2 FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | false      |
    +------------+
    1 row selected (0.033 seconds)

## UNIX_TIMESTAMP

Returns UNIX Epoch time, which is the number of seconds elapsed since January 1, 1970.

### UNIX_TIMESTAMP Syntax

`UNIX_TIMESTAMP()`  
`UNIX_TIMESTAMP(string date)`  
`UNIX_TIMESTAMP(string date, string pattern)`  

These functions perform the following operations, respectively:

* Gets current Unix timestamp in seconds if given no arguments.  
* Converts the time string in format yyyy-MM-dd HH:mm:ss to a Unix timestamp in seconds using the default timezone and locale.  
* Converts the time string with the given pattern to a Unix time stamp in seconds.  

```
SELECT UNIX_TIMESTAMP() FROM (VALUES(1));
+-------------+
|   EXPR$0    |
+-------------+
| 1435711031  |
+-------------+
1 row selected (0.749 seconds)

SELECT UNIX_TIMESTAMP('2009-03-20 11:15:55') FROM (VALUES(1));
+-------------+
|   EXPR$0    |
+-------------+
| 1237572955  |
+-------------+
1 row selected (1.848 seconds)

SELECT UNIX_TIMESTAMP('2009-03-20', 'yyyy-MM-dd') FROM (VALUES(1));
+-------------+
|   EXPR$0    |
+-------------+
| 1237532400  |
+-------------+
1 row selected (0.181 seconds)

SELECT UNIX_TIMESTAMP('2015-05-29 08:18:53.0', 'yyyy-MM-dd HH:mm:ss.SSS') FROM (VALUES(1));
+-------------+
|   EXPR$0    |
+-------------+
| 1432912733  |
+-------------+
1 row selected (0.171 seconds)  
```    

## NEARESTDATE  
Quickly and easily aggregates timestamp data by various units of time.   

**Note:** Drill 1.16 and later supports the NEARESTDATE function.

### NEARESTDATE Syntax  

NEARESTDATE(*column*, '*interval*' )   
    

### NEARESTDATE Usage Notes 
* Use with COUNT and GROUP BY to aggregate timestamp data. 
* *column* is a data source column with timestamp values.  
* *interval* is any of the following units of time: 
	* YEAR
	* QUARTER
	* MONTH
	* WEEK_SUNDAY
	* WEEK_MONDAY
	* DAY
	* HOUR
	* HALF_HOUR
	* QUARTER_HOUR
	* MINUTE
	* 30SECOND
	* 15SECOND
	* SECOND  

### NEARESTDATE Examples   

The following example uses the NEARESTDATE function to aggregate hire dates by year:

	SELECT NEARESTDATE(hire_date, 'YEAR' ) AS hireDate, COUNT(*) AS `count` FROM cp.`employee.json` GROUP BY NEARESTDATE(hire_date, 'YEAR');
	+-----------------------+-------+
	|       hireDate        | count |
	+-----------------------+-------+
	| 1994-01-01 00:00:00.0 | 23    |
	| 1998-01-01 00:00:00.0 | 539   |
	| 1996-01-01 00:00:00.0 | 503   |
	| 1995-01-01 00:00:00.0 | 12    |
	| 1997-01-01 00:00:00.0 | 74    |
	| 1993-01-01 00:00:00.0 | 4     |
	+-----------------------+-------+   

The following example applies the NEARESTDATE function to a timestamp value (2019-02-01 07:22:00) and returns the timestamp value for each time unit indicated:  

	SELECT nearestDate( TO_TIMESTAMP('2019-02-01 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'YEAR') AS nearest_year, nearestDate( TO_TIMESTAMP('2019-02-01 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'QUARTER') AS nearest_quarter, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'MONTH') AS nearest_month, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'DAY') AS nearest_day, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'WEEK_SUNDAY') AS nearest_week_sunday, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'WEEK_MONDAY') AS nearest_week_monday, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'HOUR') AS nearest_hour, nearestDate( TO_TIMESTAMP('2019-02-15 07:42:00', 'yyyy-MM-dd HH:mm:ss'), 'HALF_HOUR') AS nearest_half_hour, nearestDate( TO_TIMESTAMP('2019-02-15 07:48:00', 'yyyy-MM-dd HH:mm:ss'), 'QUARTER_HOUR') AS nearest_quarter_hour, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:00', 'yyyy-MM-dd HH:mm:ss'), 'MINUTE') AS nearest_minute, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:22', 'yyyy-MM-dd HH:mm:ss'), 'HALF_MINUTE') AS nearest_30second, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:22', 'yyyy-MM-dd HH:mm:ss'), 'QUARTER_MINUTE') AS nearest_15second, nearestDate( TO_TIMESTAMP('2019-02-15 07:22:31', 'yyyy-MM-dd HH:mm:ss'), 'SECOND') AS nearest_second FROM (VALUES(1));
	+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
	|     nearest_year      |    nearest_quarter    |     nearest_month     |      nearest_day      |  nearest_week_sunday  |  nearest_week_monday  |     nearest_hour      |   nearest_half_hour   | nearest_quarter_hour  |    nearest_minute     |   nearest_30second    |   nearest_15second    |    nearest_second     |
	+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
	| 2019-01-01 00:00:00.0 | 2019-01-01 00:00:00.0 | 2019-02-01 00:00:00.0 | 2019-02-15 00:00:00.0 | 2019-02-10 00:00:00.0 | 2019-02-11 00:00:00.0 | 2019-02-15 07:00:00.0 | 2019-02-15 07:30:00.0 | 2019-02-15 07:45:00.0 | 2019-02-15 07:22:00.0 | 2019-02-15 07:22:00.0 | 2019-02-15 07:22:15.0 | 2019-02-15 07:22:31.0 |
	+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
	

## TIMESTAMPADD  
Adds an interval of time, in the given time units, to a datetime expression.  

**Note:** Drill 1.15 and later supports the TIMESTAMPADD function.   

### TIMESTAMPADD Syntax  
TIMESTAMPADD(*time\_unit, interval, datetime\_expression*)  

### TIMESTAMPADD Usage Notes  
- *datetime\_expression* is a column or literal with date, time, or timestamp values. 
- *time\_unit* is any of the following: Nanosecond, Microsecond, Second, Minute, Hour, Day, Month, Year, Week, Quarter
- *interval* is the amount of *time\_unit* to add. 
- Drill uses the *time\_unit* to infer the return type.
- You can include the `SQL_TSI_` prefix with the any of the supported time units, as shown: 
  
		SELECT TIMESTAMPADD(SQL_TSI_MINUTE,5,TIME '05:05:05');
		+-----------+
		|  EXPR$0   |
		+-----------+
		| 05:10:05  |
		+-----------+  


### TIMESTAMPADD Examples  

Add three years to a date literal:  

	SELECT TIMESTAMPADD(YEAR,3,DATE '1982-05-06');
	+------------------------+
	|         EXPR$0         |
	+------------------------+
	| 1985-05-06 00:00:00.0  |
	+------------------------+

Add a quarter (3 months) to the date values in the first column of the dates.csv file:

	SELECT TIMESTAMPADD(QUARTER, 1, CAST(COLUMNS[0] as date)) q1 FROM dfs.`/quarter/dates.csv`;
	+------------------------+
	|           q1           |
	+------------------------+
	| 2018-04-01 00:00:00.0  |
	| 2017-05-02 00:00:00.0  |
	| 2000-08-06 00:00:00.0  |
	+------------------------+  

Dates in column[0] before applying the TIMESTAMPADD function:

	SELECT COLUMNS[0] FROM dfs.`/quarter/dates.csv`;
	+-------------+
	|   EXPR$0    |
	+-------------+
	| 2018-01-01  |
	| 2017-02-02  |
	| 2000-05-06  |
	+-------------+


## TIMESTAMPDIFF  
Calculates an interval of time, in the given time units, by subtracting *datetime\_expression1* from *datetime\_expression2* (*datetime\_expression2* − *datetime\_expression1*).  

**Note:** Drill 1.15 and later supports the TIMESTAMPDIFF function.       

### TIMESTAMPDIFF Syntax  
TIMESTAMPDIFF(*time\_unit, datetime\_expression1, datetime\_expression2*)  

### TIMESTAMPDIFF Usage Notes  
- *datetime\_expression* is a column or literal with date, time, or timestamp values. 
- *time\_unit* is any of the following: Nanosecond, Microsecond, Second, Minute, Hour, Day, Month, Year, Week, Quarter
- You can include two date expressions, or one date expression with one datetime expression. 
- Drill uses the *time\_unit* to infer the return type.
- You can include the `SQL_TSI_` prefix with the any of the supported time units, as shown: 
  
		SELECT TIMESTAMPDIFF(SQL_TSI_MONTH, DATE '1982-05-06', DATE '2018-12-26');
		+---------+
		| EXPR$0  |
		+---------+
		| 439     |
		+---------+  
 

### TIMESTAMPDIFF Examples   

Subtracts the date literal '1982-05-06' from the date literal '2018-12-26' and returns the difference in months:  
	
	SELECT TIMESTAMPDIFF(MONTH, DATE'1982-05-06', DATE '2018-12-26');
	+---------+
	| EXPR$0  |
	+---------+
	| 439     |
	+---------+

Subtracts the date literal '1970-01-15' from the dates in the first column of the dates.csv file and returns the difference in seconds:    

	SELECT TIMESTAMPDIFF(SECOND, DATE '1970-01-15', CAST(COLUMNS[0] as date)) a FROM dfs.`/quarter/dates.csv`
	+-------------+
	|      a      |
	+-------------+
	| 1513555200  |
	| 1484784000  |
	| 956361600   |
	+-------------+

Subtracts the date in the third column from the date in the first column (columns[0]-columns[2]) of the dates.csv file and returns the difference in seconds:   
   
	SELECT TIMESTAMPDIFF(SECOND, CAST(COLUMNS[2] as date), CAST(COLUMNS[0] as date)) a from dfs.`/quarter/dates.csv`;
	+------------+
	|     a      |
	+------------+
	| 0          |
	| 0          |
	| -92016000  |
	+------------+
