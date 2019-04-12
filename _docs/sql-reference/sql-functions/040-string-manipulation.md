---
title: "String Manipulation"
date: 2019-04-12
parent: "SQL Functions"
---

You can use the following string functions in Drill queries:

Function| Return Type  
--------|---  
[BYTE_SUBSTR]({{ site.baseurl }}/docs/string-manipulation/#byte_substr)|BINARY or VARCHAR
[CHAR_LENGTH]({{ site.baseurl }}/docs/string-manipulation/#char_length)| INTEGER  
[CONCAT]({{ site.baseurl }}/docs/string-manipulation/#concat)| VARCHAR
[ILIKE]({{ site.baseurl }}/docs/string-manipulation/#ilike)| BOOLEAN
[INITCAP]({{ site.baseurl }}/docs/string-manipulation/#initcap)| VARCHAR
[LENGTH]({{ site.baseurl }}/docs/string-manipulation/#length)| INTEGER
[LOWER]({{ site.baseurl }}/docs/string-manipulation/#lower)| VARCHAR
[LPAD]({{ site.baseurl }}/docs/string-manipulation/#lpad)| VARCHAR
[LTRIM]({{ site.baseurl }}/docs/string-manipulation/#ltrim)| VARCHAR
[POSITION]({{ site.baseurl }}/docs/string-manipulation/#position)| INTEGER
[REGEXP_MATCHES]({{ site.baseurl }}/docs/string-manipulation/#regexp_matches)|BOOLEAN
[REGEXP_REPLACE]({{ site.baseurl }}/docs/string-manipulation/#regexp_replace)|VARCHAR
[RPAD]({{ site.baseurl }}/docs/string-manipulation/#rpad)| VARCHAR
[RTRIM]({{ site.baseurl }}/docs/string-manipulation/#rtrim)| VARCHAR
[STRPOS]({{ site.baseurl }}/docs/string-manipulation/#strpos)| INTEGER
[SUBSTR]({{ site.baseurl }}/docs/string-manipulation/#substr)| VARCHAR
[TRIM]({{ site.baseurl }}/docs/string-manipulation/#trim)| VARCHAR
[UPPER]({{ site.baseurl }}/docs/string-manipulation/#upper)| VARCHAR

## BYTE_SUBSTR
Returns in binary format a substring of a string.

### BYTE_SUBSTR Syntax

    BYTE_SUBSTR( string-expression, start  [, length [(string-expression)]] )

*string-expression* is the entire string, a column name having string values for example.
*start* is a start position in the string. 1 is the first position.
*length* is the number of characters to the right of the start position to include in the output expressed in either of the following ways:
* As an integer. For example, 19 includes 19 characters to the right of the start position in the output.
* AS length(string-expression). For example, length(my_string) includes the number of characters in my_string minus the number of the start position.

### BYTE_SUBSTR Usage Notes
Combine the use of BYTE_SUBSTR and CONVERT_FROM to separate parts of a HBase composite key for example. 

### BYTE_SUBSTR Examples

A composite HBase row key consists of strings followed by a reverse timestamp (long). For example: AMZN_9223370655563575807. Use BYTE_SUBSTR and CONVERT_FROM to separate parts of a HBase composite key.

    SELECT CONVERT_FROM(BYTE_SUBSTR(row_key,6,19),'UTF8') FROM root.`mydata` LIMIT 1;
    +---------------------+
    |       EXPR$0        |
    +---------------------+
    | 9223370655563575807 |
    +---------------------+
    1 rows selected (0.271 seconds)

    SELECT CONVERT_FROM(BYTE_SUBSTR(row_key,6,length(row_key)),'UTF8') FROM root.`mydata` LIMIT 1;
    +---------------------+
    |       EXPR$0        |
    +---------------------+
    | 9223370655563575807 |
    +---------------------+
    1 rows selected (0.271 seconds)

## CHAR_LENGTH 
Returns the number of characters in a string.

### CHAR_LENGTH Syntax

    CHAR_LENGTH(string)

### CHAR_LENGTH Usage Notes
You can use the alias CHARACTER_LENGTH.

### CHAR_LENGTH Example

    SELECT CHAR_LENGTH('Drill rocks') FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | 11         |
    +------------+
    1 row selected (0.127 seconds)

## CONCAT
Concatenates arguments.

### CONCAT Syntax

    CONCAT(string [, string [, ...] )

### CONCAT Example

    SELECT CONCAT('Drill', ' ', 1.0, ' ', 'release') FROM (VALUES(1));

    +--------------------+
    |       EXPR$0       |
    +--------------------+
    | Drill 1.0 release  |
    +--------------------+
    1 row selected (0.134 seconds)

Alternatively, you can use the [string concatenation operation]({{ site.baseurl }}/docs/operators/#string-concatenate-operator) to concatenate strings.

## ILIKE
Compares argument one and two and returns true if values match.

### ILIKE Syntax

ILIKE( string, string )

### ILIKE Examples

```
SELECT ILIKE('abc', 'abc') FROM (VALUES(1));
+---------+
| EXPR$0  |
+---------+
| true    |
+---------+
1 row selected (0.185 seconds)
```
```
SELECT ILIKE(last_name, 'Spence') FROM cp.`employee.json` limit 3;
+---------+
| EXPR$0  |
+---------+
| false   |
| false   |
| true    |
+---------+
3 rows selected (0.17 seconds)
```

## INITCAP
Returns the string using initial caps.

### INITCAP Syntax

    INITCAP(string)

### INITCAP Examples

    SELECT INITCAP('apache drill release 1.0') FROM (VALUES(1));

    +---------------------------+
    |          EXPR$0           |
    +---------------------------+
    | Apache Drill Release 1.0  |
    +---------------------------+
    1 row selected (0.106 seconds)

## LENGTH
Returns the number of characters in the string.

### LENGTH Syntax
    LENGTH( string [, encoding] )

### LENGTH Example

    SELECT LENGTH('apache drill release 1.0') FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | 24         |
    +------------+
    1 row selected (0.127 seconds)

    SELECT LENGTH(row_key, 'UTF8') FROM root.`students`;

    +------------+
    |   EXPR$0   |
    +------------+
    | 8          |
    | 8          |
    | 8          |
    | 8          |
    +------------+
    4 rows selected (0.259 seconds)

## LOWER
Converts characters in the string to lowercase.

### LOWER Syntax

    LOWER (string)

### LOWER Example

    SELECT LOWER('Apache Drill') FROM (VALUES(1));

    +---------------+
    |    EXPR$0     |
    +---------------+
    | apache drill  |
    +---------------+
    1 row selected (0.103 seconds)

## LPAD
Pads the string to the length specified by prepending the fill or a space. Truncates the string if longer than the specified length.
. 

### LPAD Syntax

    LPAD (string, length [, fill text])

### LPAD Example

    SELECT LPAD('Release 1.0', 27, 'of Apache Drill 1.0') FROM (VALUES(1));

    +------------------------------+
    |            EXPR$0            |
    +------------------------------+
    | of Apache Drill Release 1.0  |
    +------------------------------+
    1 row selected (0.132 seconds)

## LTRIM
Removes any characters from the beginning of string1 that match the characters in string2. 

### LTRIM Syntax

    LTRIM(string1, string2)

### LTRIM Examples

    SELECT LTRIM('Apache Drill', 'Apache ') FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | Drill      |
    +------------+
    1 row selected (0.131 seconds)

    SELECT LTRIM('A powerful tool Apache Drill', 'Apache ') FROM (VALUES(1));

    +----------------------------+
    |           EXPR$0           |
    +----------------------------+
    | owerful tool Apache Drill  |
    +----------------------------+
    1 row selected (0.1 seconds)

## POSITION
Returns the location of a substring.

### POSITION Syntax

    POSITION('substring' in 'string')

### POSITION Example

    SELECT POSITION('c' in 'Apache Drill') FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | 4          |
    +------------+
    1 row selected (0.12 seconds)


##REGEXP_MATCHES  

Matches a regexp pattern to a target string. Returns a boolean value; true if the value matches the regexp, false if the value does not match the regexp.  

###REGEXP_MATCHES Syntax 

REGEXP_MATCHES(string_expression, pattern)

*string_expression* is the string to be matched.  

*pattern* is the regular expression.  

### REGEXP_MATCHES Examples

Shows several POSIX metacharacters that return true for the given string expressions:   

	select regexp_matches('abc', 'abc|def') as a, regexp_matches('cat', '[hc]at$') as b,  regexp_matches('cat', '.at') as c, regexp_matches('cat', '[hc]at') as d, regexp_matches('cat', '[^b]at') as e, regexp_matches('cat', '^[hc]at') as f, regexp_matches('[a]', '\[.\]') as g, regexp_matches('sat', 's.*') as h, regexp_matches('sat','[^hc]at') as i, regexp_matches('hat', '[hc]?at') as j, regexp_matches('cchchat', '[hc]*at') as k, regexp_matches('chat', '[hc]+at') as l;
	
	+------+------+------+------+------+------+------+------+------+------+------+------+
	|  a   |  b   |  c   |  d   |  e   |  f   |  g   |  h   |  i   |  j   |  k   |  l   |
	+------+------+------+------+------+------+------+------+------+------+------+------+
	| true | true | true | true | true | true | true | true | true | true | true | true |
	+------+------+------+------+------+------+------+------+------+------+------+------+  

Shows case-sensitivity:

	select regexp_matches('abc', 'A*.C');
	+--------+
	| EXPR$0 |
	+--------+
	| false  |
	+--------+
	
	select regexp_matches('abc', 'a*.c');
	+--------+
	| EXPR$0 |
	+--------+
	| true   |
	+--------+



##REGEXP_REPLACE

Substitutes new text for substrings that match [Java regular expression patterns](http://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).

### REGEXP_REPLACE Syntax

    REGEXP_REPLACE(source_char, pattern, replacement)

*source* is the character expression to be replaced.

*pattern* is the regular expression.

*replacement* is the string to substitute for the source.

### REGEXP_REPLACE Examples

Replace a's with b's in this string.

    SELECT REGEXP_REPLACE('abc, acd, ade, aef', 'a', 'b') FROM (VALUES(1));
    +---------------------+
    |       EXPR$0        |
    +---------------------+
    | bbc, bcd, bde, bef  |
    +---------------------+
    1 row selected (0.105 seconds)


Use the regular expression *a* followed by a period (.) in the same query to replace all a's and the subsequent character.

    SELECT REGEXP_REPLACE('abc, acd, ade, aef', 'a.','b') FROM (VALUES(1));
    +-----------------+
    |     EXPR$0      |
    +-----------------+
    | bc, bd, be, bf  |
    +-----------------+
    1 row selected (0.113 seconds)


## RPAD
Pads the string to the length specified. Appends the text you specify after the fill keyword using spaces for the fill if you provide no text or insufficient text to achieve the length.  Truncates the string if longer than the specified length.

### RPAD Syntax

    RPAD (string, length [, fill text])

### RPAD Example

    SELECT RPAD('Apache Drill ', 22, 'Release 1.0') FROM (VALUES(1));
    +-------------------------+
    |         EXPR$0          |
    +-------------------------+
    | Apache Drill Release 1  |
    +-------------------------+
    1 row selected (0.107 seconds)

## RTRIM
Removes any characters from the end of string1 that match the characters in string2.  

### RTRIM Syntax

    RTRIM(string1, string2)

### RTRIM Examples

    SELECT RTRIM('Apache Drill', 'Drill ') FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | Apache     |
    +------------+
    1 row selected (0.135 seconds)

    SELECT RTRIM('1.0 Apache Tomcat 1.0', 'Drill 1.0') from (VALUES(1));
    +--------------------+
    |       EXPR$0       |
    +--------------------+
    | 1.0 Apache Tomcat  |
    +--------------------+
    1 row selected (0.102 seconds)

## STRPOS
Returns the location of the substring in a string.

### STRPOS Syntax

STRPOS(string, substring)

### STRPOS Example

    SELECT STRPOS('Apache Drill', 'Drill') FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | 8          |
    +------------+
    1 row selected (0.22 seconds)

## SUBSTR
Extracts characters from position 1 - x of the string an optional y times.

### SUBSTR Syntax

    SUBSTR(string, x, y)

### SUBSTR Usage Notes
You can use the alias SUBSTRING for this function.


### SUBSTR Example

    SELECT SUBSTR('Apache Drill', 8) FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | Drill      |
    +------------+
    1 row selected (0.134 seconds)

    SELECT SUBSTR('Apache Drill', 3, 2) FROM (VALUES(1));

    +------------+
    |   EXPR$0   |
    +------------+
    | ac         |
    +------------+
    1 row selected (0.129 seconds)

## TRIM
Removes any characters from the beginning, end, or both sides of string2 that match the characters in string1.  

### TRIM Syntax

    TRIM ([leading | trailing | both] [string1] from string2)

### TRIM Example

    SELECT TRIM(trailing 'l' from 'Drill') FROM (VALUES(1));
    +------------+
    |   EXPR$0   |
    +------------+
    | Dri        |
    +------------+
    1 row selected (0.172 seconds)

    SELECT TRIM(both 'l' from 'long live Drill') FROM (VALUES(1));
    +---------------+
    |    EXPR$0     |
    +---------------+
    | ong live Dri  |
    +---------------+
    1 row selected (0.104 seconds)

    SELECT TRIM(leading 'l' from 'long live Drill') FROM (VALUES(1));
    +-----------------+
    |     EXPR$0      |
    +-----------------+
    | ong live Drill  |
    +-----------------+
    1 row selected (0.101 seconds)

## UPPER
Converts characters in the string to uppercase.

### UPPER Syntax

    UPPER (string)

### UPPER Example

    SELECT UPPER('Apache Drill') FROM (VALUES(1));

    +---------------+
    |    EXPR$0     |
    +---------------+
    | APACHE DRILL  |
    +---------------+
    1 row selected (0.081 seconds)
