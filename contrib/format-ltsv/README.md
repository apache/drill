# Drill LTSV files Plugin

Drill LTSV storage plugin allows you to perform interactive analysis using SQL against LTSV files.

For more information about LTSV, please see [LTSV (Labeled Tab-separated Values)](http://ltsv.org/).

## Configuration
There are several optional configuration parameters which you can use to modify how ltsv files are read.  In general, it is not necessary to change these from the defaults.  They are:

* `parseMode`: Sets the error tolerance of the LTSV parser.  Possible values are `lenient` and `strict`.  Defaults to `lenient`.
* `escapeCharacter`: Character to be used to escape control character.
* `kvDelimiter`: Character to delimit key/value pairs.
* `entryDelimiter`: Character to delimit entries.
* `lineEnding`: Character to denote line endings.
* `quoteChar`: Character to denote quoted strings.

With the exception of `parseMode`, all fields accept a single character string.

## Example of Querying an LTSV File

### About the Data

Each line in the LTSV file has the following structure:

```
time:<value> TAB host:<value> TAB forwardedfor:<value> TAB req:<value> TAB status:<value> TAB size:<value> TAB referer:<value> TAB ua:<value> TAB reqtime:<value> TAB apptime:<value> TAB vhost:<value> NEWLINE
```

For example,

```
time:30/Nov/2016:00:55:08 +0900<TAB>host:xxx.xxx.xxx.xxx<TAB>forwardedfor:-<TAB>req:GET /v1/xxx HTTP/1.1<TAB>status:200<TAB>size:4968<TAB>referer:-<TAB>ua:Java/1.8.0_131<TAB>reqtime:2.532<TAB>apptime:2.532<TAB>vhost:api.example.com
time:30/Nov/2016:00:56:37 +0900<TAB>host:xxx.xxx.xxx.xxx<TAB>forwardedfor:-<TAB>req:GET /v1/yyy HTTP/1.1<TAB>status:200<TAB>size:412<TAB>referer:-<TAB>ua:Java/1.8.0_201<TAB>reqtime:3.580<TAB>apptime:3.580<TAB>vhost:api.example.com
```

The Drill dfs storage plugin definition includes an LTSV format that requires a file to have an `.ltsv` extension.

### Query the Data

Issue a SELECT statement to get the second row in the file.

```
0: jdbc:drill:zk=local> SELECT * FROM dfs.`/tmp/sample.ltsv` WHERE reqtime > 3.0;
+-----------------------------+------------------+---------------+-----------------------+---------+-------+----------+-----------------+----------+----------+------------------+
|            time             |       host       | forwardedfor  |          req          | status  | size  | referer  |       ua        | reqtime  | apptime  |      vhost       |
+-----------------------------+------------------+---------------+-----------------------+---------+-------+----------+-----------------+----------+----------+------------------+
| 30/Nov/2016:00:56:37 +0900  | xxx.xxx.xxx.xxx  | -             | GET /v1/yyy HTTP/1.1  | 200     | 412   | -        | Java/1.8.0_201  | 3.580    | 3.580    | api.example.com  |
+-----------------------------+------------------+---------------+-----------------------+---------+-------+----------+-----------------+----------+----------+------------------+
1 row selected (6.074 seconds)
```

### Providing a Schema
The LTSV reader does supports provided schema.  You can read about Drill's [provided schema functionality here](https://drill.apache.org/docs/plugin-configuration-basics/#specifying-the-schema-as-table-function-parameter)

An example query would be:

```sql
SELECT * FROM table(cp.`simple.ltsv` (type=> 'ltsv', schema => 
    'inline=(`referer` VARCHAR, 
    `vhost`VARCHAR, `size` INT, 
    `forwardedfor` VARCHAR, 
    `reqtime` DOUBLE, 
    `apptime` DOUBLE, 
    `status` INT)'))
```
Only scalar types are supported in the LTSV reader.
