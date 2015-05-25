---
title: "Querying Plain Text Files"
parent: "Querying a File System"
---
You can use Drill to access structured file types and plain text files
(flat files), such as the following file types:

  * CSV files (comma-separated values)
  * TSV files (tab-separated values)
  * PSV files (pipe-separated values)

Follow these general guidelines for querying a plain text file:

  * Use a storage plugin that defines the file format, such as comma-separated (CSV) or tab-separated values (TSV), of the data in the plain text file.
  * In the SELECT statement, use the `COLUMNS[n]` syntax in lieu of column names, which do not exist in a plain text file. The first column is column `0`.
  * In the FROM clause, use the path to the plain text file instead of using a table name. Enclose the path and file name in back ticks. 

Make sure that your registered storage plugins
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

## Example of Querying a TSV File

This example uses a tab-separated value (TSV) file that you download from a
Google internet site. The data in the file consists of phrases from books that
Google scans and generates for its [Google Books Ngram
Viewer](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html). You
use the data to find the relative frequencies of Ngrams. 

### About the Data

Each line in the TSV file has the following structure:

`ngram TAB year TAB match_count TAB volume_count NEWLINE`

For example, lines 1722089 and 1722090 in the file contain this data:

<table ><tbody><tr><th >ngram</th><th >year</th><th colspan="1" >match_count</th><th >volume_count</th></tr><tr><td ><p class="p1">Zoological Journal of the Linnean</p></td><td >2007</td><td colspan="1" >284</td><td >101</td></tr><tr><td colspan="1" ><p class="p1">Zoological Journal of the Linnean</p></td><td colspan="1" >2008</td><td colspan="1" >257</td><td colspan="1" >87</td></tr></tbody></table> 
  
In 2007, "Zoological Journal of the Linnean" occurred 284 times overall in 101
distinct books of the Google sample.

### Download and Set Up the Data

After downloading the file, you use the `dfs` storage plugin, and then select
data from the file as you would a table. In the SELECT statement, enclose the
path and name of the file in back ticks.

  1. Download the compressed Google Ngram data from this location:  
    
     http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-5gram-20120701-zo.gz

  2. Unzip the file.  
     A file named googlebooks-eng-all-5gram-20120701-zo appears.

  3. Change the file name to add a `.tsv` extension.  
The Drill `dfs` storage plugin definition includes a TSV format that requires
a file to have this extension.

### Query the Data

Get data about "Zoological Journal of the Linnean" that appears more than 250
times a year in the books that Google scans.

  1. Switch back to using the `dfs` storage plugin.
  
          USE dfs;

  2. Issue a SELECT statement to get the first three columns in the file.  
     * In the FROM clause of the example, substitute your path to the TSV file.  
     * Use aliases to replace the column headers, such as EXPR$0, with user-friendly column headers, Ngram, Publication Date, and Frequency.
     * In the WHERE clause, enclose the string literal "Zoological Journal of the Linnean" in single quotation marks.  
     * Limit the output to 10 rows.  
  
         SELECT COLUMNS[0] AS Ngram,
                COLUMNS[1] AS Publication_Date,
                COLUMNS[2] AS Frequency
         FROM `/Users/drilluser/Downloads/googlebooks-eng-all-5gram-20120701-zo.tsv`
         WHERE ((columns[0] = 'Zoological Journal of the Linnean')
             AND (columns[2] > 250)) LIMIT 10;

     The output is:

         +------------------------------------+-------------------+------------+
         |               Ngram                | Publication_Date  | Frequency  |
         +------------------------------------+-------------------+------------+
         | Zoological Journal of the Linnean  | 1993              | 297        |
         | Zoological Journal of the Linnean  | 1997              | 255        |
         | Zoological Journal of the Linnean  | 2003              | 254        |
         | Zoological Journal of the Linnean  | 2007              | 284        |
         | Zoological Journal of the Linnean  | 2008              | 257        |
         +------------------------------------+-------------------+------------+
         5 rows selected (1.175 seconds)

The Drill default storage plugins support common file formats. If you need
support for some other file format, such as GZ, create a custom storage plugin. You can also create a storage plugin to simplify querying file having long path names. A workspace name replaces the long path name.


## Create a Storage Plugin

This example covers how to create and use a storage plugin to simplify queries or to query a file type that `dfs` does not specify, GZ in this case. First, you create the storage plugin in the Drill Web UI. Next, you connect to the
file through the plugin to query a file.

You can create a storage plugin using the Apache Drill Web UI to query the GZ file containing the compressed TSV data directly.

  1. Create an `ngram` directory on your file system.
  2. Copy the GZ file `googlebooks-eng-all-5gram-20120701-zo.gz` to the `ngram` directory.
  3. Open the Drill Web UI by navigating to <http://localhost:8047/storage>.   
     To open the Drill Web UI, the [Drill shell]({{site.baseurl}}/docs/starting-drill-on-linux-and-mac-os-x/) must still be running.
  4. In New Storage Plugin, type `myplugin`.  
     ![new plugin]({{ site.baseurl }}/docs/img/ngram_plugin.png)    
  5. Click **Create**.  
     The Configuration screen appears.
  6. Replace null with the following storage plugin definition, except on the location line, use the path to your `ngram` directory instead of the drilluser's path and give your workspace an arbitrary name, for example, ngram:
  
        {
          "type": "file",
          "enabled": true,
          "connection": "file:///",
          "workspaces": {
            "ngram": {
              "location": "/Users/drilluser/ngram",
              "writable": false,
              "defaultInputFormat": null
           }
         },
         "formats": {
           "tsv": {
             "type": "text",
             "extensions": [
               "gz"
             ],
             "delimiter": "\t"
            }
          }
        }

  7. Click **Create**.  
     The success message appears briefly.
  8. Click **Back**.  
     The new plugin appears in Enabled Storage Plugins.  
     ![new plugin]({{ site.baseurl }}/docs/img/ngram_plugin.png) 
  9. Go back to the Drill shell, and list the storage plugins.  
          SHOW DATABASES;

          +---------------------+
          |     SCHEMA_NAME     |
          +---------------------+
          | INFORMATION_SCHEMA  |
          | cp.default          |
          | dfs.default         |
          | dfs.root            |
          | dfs.tmp             |
          | myplugin.default    |
          | myplugin.ngram      |
          | sys                 |
          +---------------------+
          8 rows selected (0.105 seconds)

Your custom plugin appears in the list and has two workspaces: the `ngram`
workspace that you defined and a default workspace.

### Connect to and Query a File

When querying the same data source repeatedly, avoiding long path names is
important. This exercise demonstrates how to simplify the query. Instead of
using the full path to the Ngram file, you use dot notation in the FROM
clause.

``<workspace name>.`<location>```

This syntax assumes you connected to a storage plugin that defines the
location of the data. To query the data source while you are _not_ connected to
that storage plugin, include the plugin name:

``<plugin name>.<workspace name>.`<location>```

This exercise shows how to query Ngram data when you are connected to `myplugin`.

  1. Connect to the ngram file through the custom storage plugin.  
     `USE myplugin;`
  2. Get data about "Zoological Journal of the Linnean" that appears more than 250 times a year in the books that Google scans. In the FROM clause, instead of using the full path to the file as you did in the last exercise, connect to the data using the storage plugin workspace name ngram.
  
         SELECT COLUMNS[0], 
                COLUMNS[1], 
                COLUMNS[2] 
         FROM ngram.`/googlebooks-eng-all-5gram-20120701-zo.gz` 
         WHERE ((columns[0] = 'Zoological Journal of the Linnean') 
          AND (columns[2] > 250)) 
         LIMIT 10;

     The five rows of output appear.  

To continue with this example and query multiple files in a directory, see the section, ["Example of Querying Multiple Files in a Directory"]({{site.baseurl}}/docs/querying-directories/#example-of-querying-multiple-files-in-a-directory).

