# Drill User Defined Functions

This `README` documents functions which users have submitted to Apache Drill.  

## User Agent Functions
Drill UDF for parsing User Agent Strings.
This function is based on Niels Basjes Java library for parsing user agent strings which is available here: <https://github.com/nielsbasjes/yauaa>.

### Usage
The function `parse_user_agent()` takes a user agent string as an argument and returns a map of the available fields. Note that not every field will be present in every user agent string. 
```
SELECT parse_user_agent( columns[0] ) as ua 
FROM dfs.`/tmp/data/drill-httpd/ua.csv`;
```
The query above returns:
```
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

```
SELECT uadata.ua.AgentNameVersion AS Browser,
COUNT( * ) AS BrowserCount
FROM (
   SELECT parse_user_agent( columns[0] ) AS ua
   FROM dfs.drillworkshop.`user-agents.csv`
) AS uadata
GROUP BY uadata.ua.AgentNameVersion
ORDER BY BrowserCount DESC
```
The function can also be called with an optional field as an argument. IE:
```
SELECT parse_user_agent( `user_agent`, 'AgentName` ) as AgentName ...
```
which will just return the requested field. If the user agent string is empty, all fields will have the value of `Hacker`.  
