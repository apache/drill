---
title: "String Manipulation"
parent: "SQL Functions"
---

You can use the following string functions in Drill queries:

Function| Return Type  
--------|---  
[BYTE_SUBSTR(string, start [, length])](/docs/string-manipulation#byte_substr)|byte array or text
[CHAR_LENGTH(string) or character_length(string)](/docs/string-manipulation#char_length)| int  
[CONCAT(str "any" [, str "any" [, ...] ])](/docs/string-manipulation#concat)| text
[INITCAP(string)](/docs/string-manipulation#initcap)| text
[LENGTH(string [, encoding name ])](/docs/string-manipulation#length)| int
[LOWER(string)](/docs/string-manipulation#lower)| text
[LPAD(string, length [, fill])](/docs/string-manipulation#lpad)| text
[LTRIM(string [, characters])](/docs/string-manipulation#ltrim)| text
[POSITION(substring in string)](/docs/string-manipulation#position)| int
[REGEXP_REPLACE(string, pattern, replacement](/docs/string-manipulation#regexp_replace)|text
[RPAD(string, length [, fill ])](/docs/string-manipulation#rpad)| text
[RTRIM(string [, characters])](/docs/string-manipulation#rtrim)| text
[STRPOS(string, substring)](/docs/string-manipulation#strpos)| int
[SUBSTR(string, from [, count])](/docs/string-manipulation#substr)| text
[TRIM([position_option] [characters] from string)](/docs/string-manipulation#trim)| text
[UPPER(string)](/docs/string-manipulation#upper)| text

## BYTE_SUBSTR
Returns in binary format a substring of a string.

### Syntax

    BYTE_SUBSTR( string-expression, start  [, length [(string-expression)]] );

*string-expression* is the entire string, a column name having string values for example.
*start* is a start position in the string. 1 is the first position.
*length* is the number of characters to the right of the start position to include in the output expressed in either of the following ways:
* As an integer. For example, 19 includes 19 characters to the right of the start position in the output.
* AS length(string-expression). For example, length(my_string) includes the number of characters in my_string minus the number of the start position.

### Usage Notes
Combine the use of BYTE_SUBSTR and CONVERT_FROM to separate parts of a HBase composite key for example. 

### Examples

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

### Syntax

    ( CHAR_LENGTH | CHARACTER_LENGTH ) (string);

### Example

    SELECT CHAR_LENGTH('Drill rocks') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 11         |
    +------------+
    1 row selected (0.127 seconds)

## CONCAT
Concatenates arguments.

### Syntax

    CONCAT(string [, string [, ...] );

### Example

    SELECT CONCAT('Drill', ' ', 1.0, ' ', 'release') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | Drill 1.0 release |
    +------------+
    1 row selected (0.221 seconds)

Alternatively, you can use the [string concatenation operation](/docs/operators#string-concatenate-operator) to concatenate strings.

## INITCAP
Returns the string using initial caps.

### Syntax

    INITCAP(string);

### Examples

    SELECT INITCAP('apache drill release 1.0') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | Apache Drill Release 1.0 |
    +------------+
    1 row selected (0.124 seconds)

## LENGTH
Returns the number of characters in the string.

### Syntax
    LENGTH( string [, encoding] );

### Example

    SELECT LENGTH('apache drill release 1.0') FROM sys.version;

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
Converts characters in the string to lower case.

### Syntax

    LOWER (string);

### Example

    SELECT LOWER('Apache Drill') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | apache drill |
    +------------+
    1 row selected (0.113 seconds)

## LPAD
Pads the string to the length specified by prepending the fill or a space. Truncates the string if longer than the specified length.
. 

### Syntax

    LPAD (string, length [, fill text]);

### Example

    SELECT LPAD('Release 1.0', 27, 'of Apache Drill 1.0') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | of Apache Drill Release 1.0 |
    +------------+
    1 row selected (0.112 seconds)

## LTRIM
Removes the longest string having only characters specified in the second argument string from the beginning of the string.

### Syntax

    LTRIM(string, string);

### Example

    SELECT LTRIM('Apache Drill', 'Apache ') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | Drill      |
    +------------+
    1 row selected (0.131 seconds)

## POSITION
Returns the location of a substring.

### Syntax

    POSITION('substring' in 'string')

### Example

    SELECT POSITION('c' in 'Apache Drill') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 4          |
    +------------+
    1 row selected (0.12 seconds)

## REGEXP_REPLACE

Substitutes new text for substrings that match POSIX regular expression patterns.

### Syntax

    REGEXP_REPLACE(source_char, pattern, replacement);

*source* is the character expression to be replaced.

*pattern* is the regular expression.

*replacement* is the string to substitute for the source.

### Examples

Flatten and replace a's with b's in this JSON data.

    {"id":1,"strs":["abc","acd"]}
    {"id":2,"strs":["ade","aef"]}

    SELECT id, REGEXP_REPLACE(FLATTEN(strs), 'a','b') FROM tmp.`regex-flatten.json`;

    +------------+------------+
    |     id     |   EXPR$1   |
    +------------+------------+
    | 1          | bbc        |
    | 1          | bcd        |
    | 2          | bde        |
    | 2          | bef        |
    +------------+------------+
    4 rows selected (0.186 seconds)

Use the regular expression a. in the same query to replace all a's and the subsequent character.

    SELECT ID, REGEXP_REPLACE(FLATTEN(strs), 'a.','b') FROM tmp.`regex-flatten.json`;

    +------------+------------+
    |     id     |   EXPR$1   |
    +------------+------------+
    | 1          | bc         |
    | 1          | bd         |
    | 2          | be         |
    | 2          | bf         |
    +------------+------------+
    4 rows selected (0.132 seconds)

## RPAD
Pads the string to the length specified by appending the fill or a space. Truncates the string if longer than the specified length.

### Syntax

    RPAD (string, length [, fill text]);

### Example

    SELECT RPAD('Apache Drill ', 22, 'Release 1.0') FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | Apache Drill Release 1 |
    +------------+
    1 row selected (0.15 seconds)

## RTRIM
Removes the longest string having only characters specified in the second argument string from the end of the string.

### Syntax

    RTRIM(string, string);

### Example

    SELECT RTRIM('Apache Drill', 'Drill ') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | Apache     |
    +------------+
    1 row selected (0.135 seconds)

## STRPOS
Returns the location of the substring in a string.

### Syntax

STRPOS(string, substring)

### Example

    SELECT STRPOS('Apache Drill', 'Drill') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | 8          |
    +------------+
    1 row selected (0.22 seconds)

## SUBSTR
Extracts characters from position 1 - x of the string an optional y times.

### Syntax

(SUBSTR | SUBSTRING)(string, x, y)


### Example

    SELECT SUBSTR('Apache Drill', 8) FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | Drill      |
    +------------+
    1 row selected (0.134 seconds)

    SELECT SUBSTR('Apache Drill', 3, 2) FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | ac         |
    +------------+
    1 row selected (0.129 seconds)

## TRIM
Removes the longest string having only the characters from the beginning, end, or both ends of the string.

### Syntax

    TRIM ([leading | trailing | both] [characters] from string)

### Example

    SELECT TRIM(trailing 'l' from 'Drill') FROM sys.version;
    +------------+
    |   EXPR$0   |
    +------------+
    | Dri        |
    +------------+
    1 row selected (0.172 seconds)

## UPPER
Converts characters in the string to lower case.

### Syntax

    UPPER (string);

### Example

    SELECT UPPER('Apache Drill') FROM sys.version;

    +------------+
    |   EXPR$0   |
    +------------+
    | APACHE DRILL |
    +------------+
    1 row selected (0.104 seconds)