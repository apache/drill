---
title: "Date/Time Functions and Arithmetic"
parent: "SQL Functions"
---

In addition to the TO_DATE, TO_TIME, and TO_TIMESTAMP functions, Drill supports a number of other date/time functions and arithmetic operators for use with dates, times, and intervals. Drill supports time functions based on the Gregorian calendar and in the range 1971 to 2037.

This section defines the following date/time functions:

**Function**| **Return Type**  
---|---  
[AGE(TIMESTAMP)]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#age)| INTERVALDAY or INTERVALYEAR
[EXTRACT(field from time_expression)]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#extract)| double precision
[CURRENT_DATE]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#current_*x*-local*x*-now-and-timeofday)| DATE  
[CURRENT_TIME]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#current_*x*-local*x*-now-and-timeofday)| TIME   
[CURRENT_TIMESTAMP]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#current_*x*-local*x*-now-and-timeofday)| TIMESTAMP 
[DATE_ADD]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#date_add)| date/datetime  
[DATE_PART]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#date_part)| double precision  
[DATE_SUB]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#date_sub)| date/datetime     
[LOCALTIME]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#current_*x*-local*x*-now-and-timeofday)| TIME  
[LOCALTIMESTAMP]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#current_*x*-local*x*-now-and-timeofday)| TIMESTAMP  
[NOW]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#current_*x*-local*x*-now-and-timeofday)| TIMESTAMP  
[TIMEOFDAY]({{ site.baseurl }}/docs/date-time-functions-and-arithmetic#current_*x*-local*x*-now-and-timeofday)| text  

## AGE
Returns the interval between two timestamps or subtracts a timestamp from midnight of the current date.

#### Syntax

    AGE (timestamp[, timestamp]);

*timestamp* is a timestamp formatted as shown in the examples.

#### Usage Notes
Cast string arguments to timestamp to include time data in the calculations of the interval.

#### Examples

Find the interval between midnight April 3, 2015 and June 13, 1957.

    SELECT AGE('1957-06-13') FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | P703M23D   |
    +------------+
    1 row selected (0.064 seconds)

Find the interval between 11:10:10 PM on January 1, 2001 and 10:10:10 PM on January 1, 2001.

    SELECT AGE(CAST('2010-01-01 10:10:10' AS TIMESTAMP), CAST('2001-01-01 11:10:10' AS TIMESTAMP)) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | P109M16DT82800S |
    +------------+
    1 row selected (0.161 seconds)

For information about how to read the interval data, see the [Interval section]({{ site.baseurl }}/docs/date-time-and-timestamp#interval).

### DATE_ADD
Returns the sum of a date/time and a number of days/hours, or of a date/time and date/time interval.

#### Syntax

    DATE_ADD(date literal_date, integer);

    DATE_ADD(keyword literal, interval expr); 

*date* is the keyword date.  
*literal_date* is a date in yyyy-mm-dd format enclosed in single quotation marks.  
*integer* is a number of days to add to the date/time.  


*keyword* is the word date, time, or timestamp.  
*literal* is a date, time, or timestamp literal.  
*interval* is a keyword  
*expr* is an interval expression.  

#### Examples

Add two days to today's date May 15, 2015.

    SELECT DATE_ADD(date '2015-05-15', 2) from sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-05-17 |
    +------------+
    1 row selected (0.07 seconds)

Add two months to April 15, 2015.

    SELECT DATE_ADD(date '2015-04-15', interval '2' month) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-06-15 00:00:00.0 |
    +------------+
    1 row selected (0.073 seconds)

Add 10 hours to the timestamp 2015-04-15 22:55:55.

    SELECT DATE_ADD(timestamp '2015-04-15 22:55:55', interval '10' hour) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-16 08:55:55.0 |
    +------------+
    1 row selected (0.068 seconds)

Add 10 hours to the time 22 hours, 55 minutes, 55 seconds.

    SELECT DATE_ADD(time '22:55:55', interval '10' hour) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 08:55:55   |
    +------------+
    1 row selected (0.085 seconds)

Add 1 year and 1 month to the timestamp 2015-04-15 22:55:55.

    SELECT DATE_ADD(timestamp '2015-04-15 22:55:55', interval '1-2' year to month) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2016-06-15 22:55:55.0 |
    +------------+
    1 row selected (0.065 seconds)

Add 1 day 2 and 1/2 hours and 45.100 seconds to the time 22:55:55.

    SELECT DATE_ADD(time '22:55:55', interval '1 2:30:45.100' day to second) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 01:26:40.100 |
    +------------+
    1 row selected (0.07 seconds)

### DATE_PART
Returns a field of a date, time, timestamp, or interval.

#### Syntax 

    date_part(keyword, expression);

*keyword* is year, month, day, hour, minute, or second enclosed in single quotation marks.  
*expression* is date, time, timestamp, or interval literal enclosed in single quotation marks.

#### Usage Notes
Use Unix Epoch timestamp in milliseconds as the expression to get the field of a timestamp.

#### Examples

    SELECT DATE_PART('day', '2015-04-02') FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2          |
    +------------+
    1 row selected (0.098 seconds)

    SELECT DATE_PART('hour', '23:14:30.076') FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 23         |
    +------------+
    1 row selected (0.088 seconds)

Find the hour part of the timestamp for April 2, 2015 23:25:43. Use Unix Epoch timestamp in milliseconds, which is 1428017143000 in UTC.

    SELECT DATE_PART('hour', 1428017143000) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 23         |
    +------------+
    1 row selected (0.07 seconds)

Return the day part of the one year, 2 months, 10 days interval.

    SELECT DATE_PART('day', '1:2:10') FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 10         |
    +------------+
    1 row selected (0.069 seconds)

### DATE_SUB
Returns the difference between a date/time and a number of days/hours, or between a date/time and date/time interval.

#### Syntax

    DATE_SUB(date literal_date, integer);

    DATE_SUB(keyword literal, interval expr); 

*date* is the keyword date.  
*literal_date* is a date in yyyy-mm-dd format enclosed in single quotation marks.  
*integer* is a number of days to subtract from the date/time.  


*keyword* is the word date, time, or timestamp.  
*literal* is a date, time, or timestamp literal.  
*interval* is a keyword.  
*expr* is an interval expression.

#### Examples

Subtract two days to today's date May 15, 2015.

    SELECT DATE_SUB(date '2015-05-15', 2) from sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-05-13 |
    +------------+
    1 row selected (0.088 seconds)

Subtact two months from April 15, 2015.

    SELECT DATE_SUB(date '2015-04-15', interval '2' month) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-02-15 |
    +------------+
    1 row selected (0.088 seconds)

Subtract 10 hours from the timestamp 2015-04-15 22:55:55.

    SELECT DATE_SUB(timestamp '2015-04-15 22:55:55', interval '10' hour) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-15 12:55:55.0 |
    +------------+
    1 row selected (0.068 seconds)

Subtract 10 hours from the time 22 hours, 55 minutes, 55 seconds.

    SELECT DATE_SUB(time '22:55:55', interval '10' hour) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 12:55:55   |
    +------------+
    1 row selected (0.079 seconds)

Subtract 1 year and 1 month from the timestamp 2015-04-15 22:55:55.

    SELECT DATE_SUB(timestamp '2015-04-15 22:55:55', interval '1-2' year to month) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2014-02-15 22:55:55.0 |
    +------------+
    1 row selected (0.073 seconds)

Subtract 1 day, 2 and 1/2 hours, and 45.100 seconds from the time 22:55:55.

    SELECT DATE_ADD(time '22:55:55', interval '1 2:30:45.100' day to second) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 01:26:40.100 |
    +------------+
    1 row selected (0.073 seconds)

### CURRENT_*x*, LOCAL*x*, NOW, and TIMEOFDAY

The following examples show how to use these functions:

* CURRENT_DATE
* CURRENT_TIME
* CURRENT_TIMESTAMP
* LOCALTIME
* LOCALTIMESTAMP
* NOW
* TIMEOFDAY

    SELECT CURRENT_DATE FROM sys.version;
    +--------------+
    | current_date |
    +--------------+
    | 2015-04-02   |
    +--------------+
    1 row selected (0.077 seconds)

    SELECT CURRENT_TIME FROM sys.version;
    +--------------+
    | current_time |
    +--------------+
    | 14:32:04.751 |
    +--------------+
    1 row selected (0.073 seconds)

    SELECT CURRENT_TIMESTAMP FROM sys.version;
    +-------------------+
    | current_timestamp |
    +-------------------+
    | 2015-04-02 14:32:34.047 |
    +-------------------+
    1 row selected (0.061 seconds)

    SELECT LOCALTIME FROM sys.version;

    +------------+
    | localtime  |
    +------------+
    | 14:33:04.95 |
    +------------+
    1 row selected (0.051 seconds)

    SELECT LOCALTIMESTAMP FROM sys.version;

    +----------------+
    | LOCALTIMESTAMP |
    +----------------+
    | 2015-04-02 23:13:13.204 |
    +----------------+
    1 row selected (0.105 seconds)

    SELECT NOW() FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-02 23:14:30.076 |
    +------------+
    1 row selected (0.05 seconds)

If you set up Drill for [UTC time]({{ site.baseurl }}/docs/casting-converting-data-types/time-zone-limitation), TIMEOFDAY returns the result for the UTC time zone.

    SELECT TIMEOFDAY() FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-02 22:05:02.424 UTC |
    +------------+
    1 row selected (1.191 seconds)

If you did not set up Drill for UTC time, TIMEOFDAY returns the local date and time with time zone information.

    SELECT TIMEOFDAY() FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 2015-04-02 15:01:31.114 America/Los_Angeles |
    +------------+
    1 row selected (1.199 seconds)

### EXTRACT

Returns a component of a timestamp, time, date, or interval.

#### Syntax

    EXTRACT (extract_expression);

*extract_expression* is:

    component FROM (timestamp | time | date | interval)

*component* is supported time unit.

#### Usage Notes

The extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.

#### Examples

On the third day of the month, run the following function:

    SELECT EXTRACT(day FROM NOW()), EXTRACT(day FROM CURRENT_DATE) FROM sys.version;

    +------------+------------+
    |   EXPR$0   |   EXPR$1   |
    +------------+------------+
    | 3          | 3          |
    +------------+------------+
    1 row selected (0.208 seconds)

At 8:00 am, extract the hour from the value of CURRENT_DATE.

    SELECT EXTRACT(hour FROM CURRENT_DATE) FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 8          |
    +------------+

What is the hour component of this time: 17:12:28.5?

    SELECT EXTRACT(hour FROM TIME '17:12:28.5') from sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 17         |
    +------------+
    1 row selected (0.056 seconds)

What is the seconds component of this timestamp: 2001-02-16 20:38:40

    SELECT EXTRACT(SECOND FROM TIMESTAMP '2001-02-16 20:38:40') from sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 40.0       |
    +------------+
    1 row selected (0.062 seconds)


## Date, Time, and Interval Arithmetic Functions
<!-- date +/- integer
date + interval  -->

Is the day returned from the NOW function the same as the day returned from the CURRENT_DATE function?

    SELECT EXTRACT(day FROM NOW()) = EXTRACT(day FROM CURRENT_DATE) FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | true       |
    +------------+
    1 row selected (0.092 seconds)

Every 23 hours, a 4 hour task started. What time does the task end? 

    SELECT TIME '04:00:00' + interval '23:00:00' hour to second FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | 03:00:00   |
    +------------+
    1 row selected (0.097 seconds)

Is the time 2:00 PM?

    SELECT EXTRACT(hour FROM CURRENT_DATE) = 2 FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | false      |
    +------------+
    1 row selected (0.033 seconds)


<!-- timestamp + interval 
timestamptz + interval
date + intervalday 
time + intervalday 
timestamp + intervalday
timestamptz + intervalday
date + intervalyear 
time + intervalyear
timestamp + intervalyear 
timestamptz + intervalyear
date + time
date - date
time - time
timestamp - timestamp
timestamptz - timestamptz
interval +/- interval
intervalday +/- intervalday
intervalyear +/- intervalyear
interval *//(div) integer or float or double
intervalday *//(div) integer or float or double
intervalyear *//(div) integer or float or double
-interval
-intervalday
-intervalyear

extract(field from timestamptz)
extract(field from interval)
extract(field from intervalday)
extract(field from intervalyear) -->

