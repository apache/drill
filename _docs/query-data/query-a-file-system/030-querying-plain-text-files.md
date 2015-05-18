---
title: "Querying Plain Text Files"
parent: "Querying a File System"
---
You can use Drill to access both structured file types and plain text files
(flat files). This section shows a few simple examples that work on flat
files:

  * CSV files (comma-separated values)
  * TSV files (tab-separated values)
  * PSV files (pipe-separated values)

The examples here show CSV files, but queries against TSV and PSV files return
equivalent results. However, make sure that your registered storage plugins
recognize the appropriate file types and extensions. For example, the
following configuration expects PSV files (files with a pipe delimiter) to
have a `tbl` extension, not a `psv` extension. Drill returns a "file not
found" error if references to files in queries do not match these conditions.

    "formats": {
        "psv": {
          "type": "text",
          "extensions": [
            "tbl"
          ],
          "delimiter": "|"
        }

## SELECT * FROM a CSV File

The first query selects rows from a `.csv` text file. The file contains seven
records:

    $ more plays.csv
 
    1599,As You Like It
    1601,Twelfth Night
    1594,Comedy of Errors
    1595,Romeo and Juliet
    1596,The Merchant of Venice
    1610,The Tempest
    1599,Hamlet

Drill recognizes each row as an array of values and returns one column for
each row.

    0: jdbc:drill:zk=local> select * from dfs.`/Users/brumsby/drill/plays.csv`;
 
    +-----------------------------------+
    |              columns              |
    +-----------------------------------+
    | ["1599","As You Like It"]         |
    | ["1601","Twelfth Night"]          |
    | ["1594","Comedy of Errors"]       |
    | ["1595","Romeo and Juliet"]       |
    | ["1596","The Merchant of Venice"] |
    | ["1610","The Tempest"]            |
    | ["1599","Hamlet"]                 |
    +-----------------------------------+
    7 rows selected (0.089 seconds)

## Columns[n] Syntax

You can use the `COLUMNS[n]` syntax in the SELECT list to return these CSV
rows in a more readable, column by column, format. (This syntax uses a zero-
based index, so the first column is column `0`.)

    0: jdbc:drill:zk=local> select columns[0], columns[1] from dfs.`/Users/brumsby/drill/plays.csv`;
 
    +------------+------------------------+
    |   EXPR$0   |         EXPR$1         |
    +------------+------------------------+
    | 1599       | As You Like It         |
    | 1601       | Twelfth Night          |
    | 1594       | Comedy of Errors       |
    | 1595       | Romeo and Juliet       |
    | 1596       | The Merchant of Venice |
    | 1610       | The Tempest            |
    | 1599       | Hamlet                 |
    +------------+------------------------+
    7 rows selected (0.137 seconds)

You can use aliases to return meaningful column names. Note that `YEAR` is a
reserved word, so the `Year` alias must be enclosed by back ticks.

    0: jdbc:drill:zk=local> select columns[0] as `Year`, columns[1] as Play 
    from dfs.`/Users/brumsby/drill/plays.csv`;
 
    +------------+------------------------+
    |    Year    |    Play                |
    +------------+------------------------+
    | 1599       | As You Like It         |
    | 1601       | Twelfth Night          |
    | 1594       | Comedy of Errors       |
    | 1595       | Romeo and Juliet       |
    | 1596       | The Merchant of Venice |
    | 1610       | The Tempest            |
    | 1599       | Hamlet                 |
    +------------+------------------------+
    7 rows selected (0.113 seconds)

You cannot refer to the aliases in subsequent clauses of the query. Use the
original `columns[n]` syntax, as shown in the WHERE clause for the following
example:

    0: jdbc:drill:zk=local> select columns[0] as `Year`, columns[1] as Play 
    from dfs.`/Users/brumsby/drill/plays.csv` where columns[0]>1599;
 
    +------------+---------------+
    |    Year    |      Play     |
    +------------+---------------+
    | 1601       | Twelfth Night |
    | 1610       | The Tempest   |
    +------------+---------------+
    2 rows selected (0.201 seconds)

Note that the restriction with the use of aliases applies to queries against
all data sources.

