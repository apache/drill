---
title: "Reserved Keywords"
date: 2019-04-12
parent: "SQL Reference"
---
When you use a reserved keyword in a Drill query, enclose the word in
backticks. For example, if you issue the following query to Drill,  
you must include backticks around the word TABLES because TABLES is a reserved
keyword:

``SELECT * FROM INFORMATION_SCHEMA.`TABLES`;``

The following tables provide the Drill reserved keywords that require back
ticks:  

| A                     | B               | C                                | D             | E             | F           | G        | H         | I            | J    | K    | L              | M                   |
|-----------------------|-----------------|----------------------------------|---------------|---------------|-------------|----------|-----------|--------------|------|------|----------------|---------------------|
|                       |                 |                                  |               |               |             |          |           |              |      |      |                |                     |
| ABS                   | BEFORE          | CALL                             | DATA          | EACH          | FALSE       | GENERAL  | HANDLER   | IDENTITY     | JAR  | KEEP | LAG            | MAP                 |
|                       |                 |                                  | DATABASES     |               |             |          |           |              |      |      |                |                     |
| ABSOLUTE              | BEGIN           | CALLED                           | DATE          | ELEMENT       | FETCH       | GET      | HAVING    | IF           | JOIN | KEY  | LANGUAGE       | MATCH               |
| ACTION                |                 |                                  |               |               | FILES       |          |           |              |      |      |                |                     |
| ADD                   | BEGIN_FRAME     | CARDINALITY                      | DAY           | ELSE          | FILTER      | GLOBAL   | HOLD      | IMMEDIATE    |      |      | LARGE          | MATCHES             |
| AFTER                 | BEGIN_PARTITION | CASCADE                          | DAYS          | ELSEIF        | FIRST       | GO       | HOUR      | IMMEDIATELY  |      |      | LAST           | MATCH_NUMBER        |
| ALL                   | BETWEEN         | CASCADED                         | DEALLOCATE    | EMPTY         | FIRST_VALUE | GOTO     |  HOURS    | IMPORT       |      |      | LAST_VALUE     |  MATCH_RECOGNIZE    |
| ALLOCATE              | BIGINT          | CASE                             | DEC           | END           | FLOAT       | GRANT    |           | IN           |      |      | LATERAL        | MAX                 |
| ALLOW                 | BINARY          | CAST                             | DECIMAL       | END-EXEC      | FLOOR       | GROUP    |           | INDICATOR    |      |      | LEAD           |   MAX_CARDINALITY   |
|                       | BIT             | CATALOG                          | DECLARE       | END_FRAME     | FOR         | GROUPING |           | INITIAL      |      |      | LEADING        | MEASURES            |
| ALTER                 |                 |                                  |               |               |             |          |           |              |      |      |                |                     |
| ANALYZE               | BIT_LENGTH      | CEIL                             | DEFAULT       | END_PARTITION | FOREIGN     | GROUPS   |           | INITIALLY    |      |      | LEAVE          | MEMBER              |
| AND                   |                 |                                  |               |               |             |          |           |              |      |      |                |  MERGE              |
| ANY                   | BLOB            | CEILING                          | DEFERRABLE    | EQUALS        | FOREVER     |          |           | INNER        |      |      | LEFT           | METADATA            |
| ARE                   | BOOLEAN         | CHAR                             | DEFERRED      | ESCAPE        | FOUND       |          |           | INOUT        |      |      | LEVEL          | METHOD              |
|                       |                 |                                  |               | ESTIMATE      |             |          |           |              |      |      |                |                     |
|                       | BOTH            | CHARACTER                        | DEFINE        | EVERY         | FRAME_ROW   |          |           | INPUT        |      |      | LIKE           | MIN                 |
|                       |                 |                                  |               |               |             |          |           |              |      |      |                |                     |
| ARRAY                 | BREADTH         | CHARACTER_LENGTH                 | DELETE        | EXCEPT        | FREE        |          |           | INSENSITIVE  |      |      | LIKE_REGEX     |  MINUS              |
| ARRAY_AGG             | BY              | CHAR_LENGTH                      | DENSE_RANK    | EXCEPTION     | FROM        |          |           | INSERT       |      |      | LIMIT          | MINUTE              |
| ARRAY_MAX_CARDINALITY |                 | CHECK                            | DEPTH         | EXEC          | FULL        |          |           | INT          |      |      | LN             | MINUTES             |
| AS                    |                 | CLASSIFIER                       | DEREF         | EXECUTE       | FUNCTION    |          |           | INTEGER      |      |      | LOCAL          | MOD                 |
| ASC                   |                 | CLOB                             | DESC          | EXISTS        | FUSION      |          |           | INTERSECT    |      |      | LOCALTIME      | MODIFIES            |
| ASENSITIVE            |                 | CLOSE                            | DESCRIBE      |   EXIT        |             |          |           | INTERSECTION |      |      | LOCALTIMESTAMP | MODULE              |
| ASSERTION             |                 | COALESCE                         | DESCRIPTOR    | EXP           |             |          |           | INTERVAL     |      |      | LOCATOR        | MONTH               |
|                       |                 |                                  |               |               |             |          |           |              |      |      |                |                     |
| ASYMMETRIC            |                 | COLLATE                          | DETERMINISTIC | EXPLAIN       |             |          |           | INTO         |      |      | LOOP           | MULTISET            |
| AT                    |                 | COLLATION                        | DIAGNOSTICS   | EXTEND        |             |          |           | IS           |      |      | LOWER          |                     |
| ATOMIC                |                 | COLLECT                          | DISALLOW      | EXTERNAL      |             |          |           | ISOLATION    |      |      |                |                     |
| AUTHORIZATION         |                 | COLUMN(S)                        | DISCONNECT    | EXTRACT       |             |          |           | ITERATE      |      |      |                |                     |
| AVG                   |                 | COMMIT                           |  DISTINCT     |               |             |          |           |              |      |      |                |                     |
|                       |                 | COMPUTE                          |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONDITION                        |   DO          |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONNECT                          | DOMAIN        |               |             |          |           |              |      |      |                |                     |
|                       |                 |                                  |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONNECTION                       | DOUBLE        |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONSTRAINT                       | DROP          |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONSTRAINTS                      | DYNAMIC       |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONSTRUCTOR                      |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONTAINS                         |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONTINUE                         |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CONVERT                          |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CORR                             |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CORRESPONDING                    |               |               |             |          |           |              |      |      |                |                     |
|                       |                 |                                  |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | COUNT                            |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | COVAR_POP                        |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | COVAR_SAMP                       |               |               |             |          |           |              |      |      |                |                     |
|                       |                 |  CREATE                          |               |               |             |          |           |              |      |      |                |                     |
|                       |                 |  CROSS                           |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CUBE                             |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CUME_DIST                        |               |               |             |          |           |              |      |      |                |                     |
|                       |                 |  CURRENT                         |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_CATALOG                  |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_DATE                     |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_DEFAULT_TRANSFORM_GROUP  |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_PATH                     |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_ROLE                     |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_ROW                      |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_SCHEMA                   |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_TIME                     |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_TIMESTAMP                |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_TRANSFORM_GROUP_FOR_TYPE |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CURRENT_USER                     |               |               |             |          |           |              |      |      |                |                     |
|                       |                 |  CURSOR                          |               |               |             |          |           |              |      |      |                |                     |
|                       |                 | CYCLE                            |               |               |             |          |           |              |      |      |                |                     |  

| N         | O                 | P               | Q   | R              | S                                           | T               | U       | V            | W            | X   | Y        | Z   |
|-----------|-------------------|-----------------|-----|----------------|---------------------------------------------|-----------------|---------|--------------|--------------|-----|----------|-----|
|           |                   |                 |     |                | SAMPLE                                      |                 |         |              |              |     |          |     |
| NAMES     | OBJECT            | PAD             | N/A | RANGE          | SAVEPOINT                                   |  TABLE(S)       | UESCAPE | VALUE        |  WHEN        | N/A | YEAR     | N/A |
|           |                   |                 |     |                |                                             |                 |         |              |              |     |          |     |
| NATIONAL  | OCCURRENCES_REGEX | PARAMETER       |     | RANK           | SCHEMA(S)                                   | TABLESAMPLE     | UNDER   |  VALUES      | WHENEVER     |     |  YEARS   |     |
|           |                   |                 |     |                |                                             |                 |         |              |              |     |          |     |
|  NATURAL  | OCTET_LENGTH      | PARTIAL         |     | READ           | SCOPE                                       | TEMPORARY       |  UNDO   | VALUE_OF     |  WHERE       |     | ZONE     |     |
| NCHAR     | OF                |  PARTITION      |     | READS          | SCROLL                                      |  THEN           |  UNION  | VARBINARY    |   WHILE      |     | COLUMNS  |     |
| NCLOB     | OFFSET            | PATH            |     | REAL           | SEARCH                                      |  TIME           | UNIQUE  | VARCHAR      | WIDTH_BUCKET |     |          |     |
| NEW       | OLD               |  PATTERN        |     | RECURSIVE      | SECOND                                      |  TIMESTAMP      | UNKNOWN | VARYING      |  WINDOW      |     |          |     |
| NEXT      | OMIT              | PER             |     | REF            |   SECONDS                                   | TIMEZONE_HOUR   |  UNNEST | VAR_POP      |  WITH        |     |          |     |
| NO        | ON                | PERCENT         |     | REFERENCES     | SECTION                                     | TIMEZONE_MINUTE | UNTIL   | VAR_SAMP     | WITHIN       |     |          |     |
|           |                   |                 |     | REFERENCING    |                                             |                 |         |              |              |     |          |     |
| NONE      | ONE               | PERCENTILE_CONT |     | REFRESH        | SEEK                                        | TINYINT         |  UPDATE | VERSION      | WITHOUT      |     |          |     |
|           |                   |                 |     |                |                                             |                 |         |              |              |     |          |     |
| NORMALIZE | ONLY              | PERCENTILE_DISC |     | REGR_AVGX      |  SELECT                                     | TO              | UPPER   | VERSIONING   | WORK         |     |          |     |
| NOT       | OPEN              | PERCENT_RANK    |     | REGR_AVGY      | SENSITIVE                                   | TRAILING        | UPSERT  |   VERSIONS   | WRITE        |     |          |     |
|           |                   |                 |     |                |                                             |                 |         |              |              |     |          |     |
| NTH_VALUE | OPTION            | PERIOD          |     | REGR_COUNT     | SESSION                                     | TRANSACTION     | USAGE   | VIEW         |              |     |          |     |
|           |                   |                 |     |                |                                             |                 | USE     |              |              |     |          |     |
| NTILE     | OR                | PERMUTE         |     | REGR_INTERCEPT | SESSION_USER                                | TRANSLATE       | USER    |              |              |     |          |     |
| NULL      |  ORDER            | PORTION         |     | REGR_R2        |  SET                                        | TRANSLATE_REGEX | USING   |              |              |     |          |     |
| NULLIF    | ORDINALITY        | POSITION        |     | REGR_SLOPE     |  SETS                                       | TRANSLATION     |         |              |              |     |          |     |
| NUMERIC   | OUT               | POSITION_REGEX  |     | REGR_SXX       | SHOW                                        | TREAT           |         |              |              |     |          |     |
|           | OUTER             | POWER           |     | REGR_SXY       |   SIGNAL                                    | TRIGGER         |         |              |              |     |          |     |
|           | OUTPUT            | PRECEDES        |     | REGR_SYY       | SIMILAR                                     | TRIM            |         |              |              |     |          |     |
|           | OVER              | PRECISION       |     | RELATIVE       | SIZE                                        | TRIM_ARRAY      |         |              |              |     |          |     |
|           |                   |                 |     |                |                                             |                 |         |              |              |     |          |     |
|           | OVERLAPS          | PREPARE         |     | RELEASE        |   SKIP    messes with JavaCC's <SKIP> token | TRUE            |         |              |              |     |          |     |
|           | OVERLAY           | PRESERVE        |     |   REPEAT       | SMALLINT                                    | TRUNCATE        |         |              |              |     |          |     |
|           |                   | PREV            |     | RESET          |  SOME                                       |                 |         |              |              |     |          |     |
|           |                   | PRIMARY         |     |   RESIGNAL     | SPACE                                       |                 |         |              |              |     |          |     |
|           |                   | PRIOR           |     | RESTRICT       | SPECIFIC                                    |                 |         |              |              |     |          |     |
|           |                   |                 |     |                |                                             |                 |         |              |              |     |          |     |
|           |                   | PRIVILEGES      |     | RESULT         | SPECIFICTYPE                                |                 |         |              |              |     |          |     |
|           |                   | PROCEDURE       |     | RETURN         | SQL                                         |                 |         |              |              |     |          |     |
|           |                   | PROPERTIES      |     |                |                                             |                 |         |              |              |     |          |     |
|           |                   | PUBLIC          |     | RETURNS        |   SQLCODE                                   |                 |         |              |              |     |          |     |
|           |                   |                 |     | REVOKE         |   SQLERROR                                  |                 |         |              |              |     |          |     |
|           |                   |                 |     |  RIGHT         | SQLEXCEPTION                                |                 |         |              |              |     |          |     |
|           |                   |                 |     | ROLE           | SQLSTATE                                    |                 |         |              |              |     |          |     |
|           |                   |                 |     | ROLLBACK       | SQLWARNING                                  |                 |         |              |              |     |          |     |
|           |                   |                 |     |  ROLLUP        | SQRT                                        |                 |         |              |              |     |          |     |
|           |                   |                 |     | ROUTINE        | START                                       |                 |         |              |              |     |          |     |
|           |                   |                 |     |  ROW           | STATE                                       |                 |         |              |              |     |          |     |
|           |                   |                 |     |  ROWS          | STATIC                                      |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | STATISTICS                                  |                 |         |              |              |     |          |     |
|           |                   |                 |     | ROW_NUMBER     | STDDEV_POP                                  |                 |         |              |              |     |          |     |
|           |                   |                 |     | RUNNING        | STDDEV_SAMP                                 |                 |         |              |              |     |          |     |
|           |                   |                 |     |                |  STREAM                                     |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SUBMULTISET                                 |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SUBSET                                      |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SUBSTRING                                   |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SUBSTRING_REGEX                             |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SUCCEEDS                                    |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SUM                                         |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SYMMETRIC                                   |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SYSTEM                                      |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SYSTEM_TIME                                 |                 |         |              |              |     |          |     |
|           |                   |                 |     |                | SYSTEM_USER                                 |                 |         |              |              |     |          |     |