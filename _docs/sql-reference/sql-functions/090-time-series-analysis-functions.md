---
title: "Time Series Analysis Functions"
date: 2020-11-02
parent: "SQL Functions"
---
<!-- TODO: when were these introduced?

**Introduced in release:** 1.14.
-->

When analyzing time based data, you will often have to aggregate by time grains.  While some time grains will be easy to calculate, others, such as quarter, can be quite difficult.  These functions enable a user to quickly and easily aggregate data by various units of time.  Usage is as follows:

```sql
SELECT <fields>
FROM <data>
GROUP BY nearestDate(<timestamp_column>, <time increment>
```

So let's say that a user wanted to count the number of hits on a web server per 15 minute, the query might look like this:

```sql
SELECT nearestDate(`eventDate`, 'QUARTER_HOUR' ) AS eventDate,
COUNT(*) AS hitCount
FROM dfs.`log.httpd`
GROUP BY nearestDate(`eventDate`, 'QUARTER_HOUR')
```
Currently supports the following time units:
 * YEAR,
 * QUARTER,
 * MONTH,
 * WEEK_SUNDAY,
 * WEEK_MONDAY,
 * DAY,
 * HOUR,
 * HALF_HOUR,
 * QUARTER_HOUR,
 * MINUTE,
 * HALF_MINUTE,
 * QUARTER_MINUTE,
 * SECOND

There are two versions of the function, one which accepts a date and interval, and the other accepts a string, format string and interval.

### Time Bucket Functions

These functions are useful for doing time series analysis by grouping the data into arbitrary intervals.  Examples in addition to those in this section may be found [here](https://blog.timescale.com/blog/simplified-time-series-analytics-using-the-time_bucket-function/). 

There are two versions of the function:
* `time_bucket(<timestamp>, <interval>)`
* `time_bucket_ns(<timestamp>,<interval>)`

Both functions accept a `BIGINT` timestamp and an interval in milliseconds as arguments. The `time_bucket_ns()` function accepts timestamps in nanoseconds and `time_bucket()` accepts timestamps in milliseconds.  Both return timestamps in the original format.

### Examples

The query below calculates the average for the `cpu` metric for every five minute interval.

```sql
SELECT time_bucket(time_stamp, 30000) AS five_min, avg(cpu)
  FROM metrics
  GROUP BY five_min
  ORDER BY five_min DESC LIMIT 12;
```

## User Agent Functions

Drill UDF for parsing User Agent Strings.  This function is based on Niels Basjes Java library for parsing user agent strings which is available [here](https://github.com/nielsbasjes/yauaa).

### Usage

The function `parse_user_agent()` takes a user agent string as an argument and returns a map of the available fields. Note that not every field will be present in every user agent string. 

```sql
SELECT parse_user_agent( columns[0] ) as ua 
FROM dfs.`/tmp/data/drill-httpd/ua.csv`;
```

The query above returns:

```json
{
  "DeviceClass":"Desktop",
  "DeviceName":"Macintosh",
  "DeviceBrand":"Apple",
  "OperatingSystemClass":"Desktop",
  "OperatingSystemName":"Mac OS X",
  "OperatingSystemVersion":"10.10.1",
  "OperatingSystemNameVersion":"Mac OS X 10.10.1",
  "LayoutEngineClass":"Browser",
  "LayoutEngineName":"Blink",
  "LayoutEngineVersion":"39.0",
  "LayoutEngineVersionMajor":"39",
  "LayoutEngineNameVersion":"Blink 39.0",
  "LayoutEngineNameVersionMajor":"Blink 39",
  "AgentClass":"Browser",
  "AgentName":"Chrome",
  "AgentVersion":"39.0.2171.99",
  "AgentVersionMajor":"39",
  "AgentNameVersion":"Chrome 39.0.2171.99",
  "AgentNameVersionMajor":"Chrome 39",
  "DeviceCpu":"Intel"
}
```

The function returns a Drill map, so you can access any of the fields using Drill's table.map.key notation. For example, the query below illustrates how to extract a field from this map and summarize it:

```sql
SELECT uadata.ua.AgentNameVersion AS Browser,
COUNT( * ) AS BrowserCount
FROM (
   SELECT parse_user_agent( columns[0] ) AS ua
   FROM dfs.drillworkshop.`user-agents.csv`
) AS uadata
GROUP BY uadata.ua.AgentNameVersion
ORDER BY BrowserCount DESC
```

The function can also be called with an optional field as an argument. I.e.

```sql
SELECT parse_user_agent( `user_agent`, 'AgentName` ) as AgentName ...
```

which will just return the requested field. If the user agent string is empty, all fields will have the value of `Hacker`.  

