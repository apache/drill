---
title: "SQL Functions"
parent: "SQL Reference"
---
You can use the following types of functions in your Drill queries:

  * Scalar Functions
  * Aggregate Functions
  * Aggregate Statistics Functions
  * Convert Functions
  * Nested Data Functions

## Scalar Functions

### Math

You can use the following scalar math functions in your Drill queries:

  * ABS
  * CEIL
  * CEILING
  * DIV
  * FLOOR
  * MOD
  * POWER 
  * RANDOM
  * ROUND
  * SIGN
  * SQRT
  * TRUNC

### String Functions

The following table provides the string functions that you can use in your
Drill queries:

Function| Return Type  
--------|---  
char_length(string) or character_length(string)| int  
concat(str "any" [, str "any" [, ...] ])| text
convert_from(string bytea, src_encoding name)| text 
convert_to(string text, dest_encoding name)| bytea
initcap(string)| text
left(str text, n int)| text
length(string)| int
length(string bytes, encoding name )| int
lower(string)| text
lpad(string text, length int [, fill text])| text
ltrim(string text [, characters text])| text
position(substring in string)| int
regexp_replace(string text, pattern text, replacement text [, flags text])|text
replace(string text, from text, to text)| text
right(str text, n int)| text
rpad(string text, length int [, fill text])| text
rtrim(string text [, characters text])| text
strpos(string, substring)| int
substr(string, from [, count])| text
substring(string [from int] [for int])| text
trim([leading | trailing | both] [characters] from string)| text
upper(string)| text
  
  
### Date/Time Functions

The following table provides the date/time functions that you can use in your
Drill queries:

**Function**| **Return Type**  
---|---  
current_date| date  
current_time| time with time zone  
current_timestamp| timestamp with time zone  
date_add(date,interval expr type)| date/datetime  
date_part(text, timestamp)| double precision  
date_part(text, interval)| double precision  
date_sub(date,INTERVAL expr type)| date/datetime  
extract(field from interval)| double precision  
extract(field from timestamp)| double precision  
localtime| time  
localtimestamp| timestamp  
now()| timestamp with time zone  
timeofday()| text  
  
### Data Type Formatting Functions

The following table provides the data type formatting functions that you can
use in your Drill queries:

**Function**| **Return Type**  
---|---  
to_char(timestamp, text)| text  
to_char(int, text)| text  
to_char(double precision, text)| text  
to_char(numeric, text)| text  
to_date(text, text)| date  
to_number(text, text)| numeric  
to_timestamp(text, text)| timestamp with time zone  
to_timestamp(double precision)| timestamp with time zone  
  
## Aggregate Functions

The following table provides the aggregate functions that you can use in your
Drill queries:

**Function** | **Argument Type** | **Return Type**  
  --------   |   -------------   |   -----------
avg(expression)| smallint, int, bigint, real, double precision, numeric, or interval| numeric for any integer-type argument, double precision for a floating-point argument, otherwise the same as the argument data type
count(*)| _-_| bigint
count([DISTINCT] expression)| any| bigint
max(expression)| any array, numeric, string, or date/time type| same as argument type
min(expression)| any array, numeric, string, or date/time type| same as argument type
sum(expression)| smallint, int, bigint, real, double precision, numeric, or interval| bigint for smallint or int arguments, numeric for bigint arguments, double precision for floating-point arguments, otherwise the same as the argument data type
  
  
## Aggregate Statistics Functions

The following table provides the aggregate statistics functions that you can use in your Drill queries:

**Function**| **Argument Type**| **Return Type**
  --------  |   -------------  |   -----------
stddev(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
stddev_pop(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
stddev_samp(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
variance(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
var_pop(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
var_samp(expression)| smallint, int, bigint, real, double precision, or numeric| double precision for floating-point arguments, otherwise numeric
  
  
## Convert Functions

You can use the CONVERT_TO and CONVERT_FROM functions to encode and decode
data when you query your data sources with Drill. For example, HBase stores
data as encoded byte arrays (VARBINARY data). When you issue a query with the
CONVERT_FROM function on HBase, Drill decodes the data and converts it to the
specified data type. In instances where Drill sends data back to HBase during
a query, you can use the CONVERT_TO function to change the data type to bytes.

Although you can achieve the same results by using the CAST function for some
data types (such as VARBINARY to VARCHAR conversions), in general it is more
efficient to use CONVERT functions when your data sources return binary data.
When your data sources return more conventional data types, you can use the
CAST function.

The following table provides the data types that you use with the CONVERT_TO
and CONVERT_FROM functions:

**Type**| **Input Type**| **Output Type**  
---|---|---  
BOOLEAN_BYTE| bytes(1)| boolean  
TINYINT_BE| bytes(1)| tinyint  
TINYINT| bytes(1)| tinyint  
SMALLINT_BE| bytes(2)| smallint  
SMALLINT| bytes(2)| smallint  
INT_BE| bytes(4)| int  
INT| bytes(4)| int  
BIGINT_BE| bytes(8)| bigint  
BIGINT| bytes(8)| bigint  
FLOAT| bytes(4)| float (float4)  
DOUBLE| bytes(8)| double (float8)  
INT_HADOOPV| bytes(1-9)| int  
BIGINT_HADOOPV| bytes(1-9)| bigint  
DATE_EPOCH_BE| bytes(8)| date  
DATE_EPOCH| bytes(8)| date  
TIME_EPOCH_BE| bytes(8)| time  
TIME_EPOCH| bytes(8)| time  
UTF8| bytes| varchar  
UTF16| bytes| var16char  
UINT8| bytes(8)| uint8  
  
A common use case for CONVERT_FROM is when a data source embeds complex data
inside a column. For example, you may have an HBase or MapR-DB table with
embedded JSON data:

    select CONVERT_FROM(col1, 'JSON') 
    FROM hbase.table1
    ...

## Nested Data Functions

This section contains descriptions of SQL functions that you can use to
analyze nested data:

  * [FLATTEN Function](/docs/flatten-function)
  * [KVGEN Function](/docs/kvgen-function)
  * [REPEATED_COUNT Function](/docs/repeated-count-function)