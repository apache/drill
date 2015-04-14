---
title: "Lesson 2: Query Plain Text"
parent: "Query Basics Tutorial"
---
The lesson shows you how to query a plain text file. Drill handles plain text
files and directories like standard SQL tables and can infer knowledge about
the schema of the data. No setup is required. For example, you do not need to
perform extract, transform, and load (ETL) operations on the data source.
Exercises in the tutorial demonstrate the general guidelines for querying a
plain text file:

  * Use a storage plugin that defines the file format, such as comma-separated (CSV) or tab-separated values (TSV), of the data in the plain text file.
  * In the SELECT statement, use the `COLUMNS[n]` syntax in lieu of column names, which do not exist in a plain text file. The first column is column `0`.
  * In the FROM clause, use the path to the plain text file instead of using a table name. Enclose the path and file name in backticks. 

## Prerequisites

This lesson uses a tab-separated value (TSV) files that you download from a
Google internet site. The data in the file consists of phrases from books that
Google scans and generates for its [Google Books Ngram
Viewer](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html). You
use the data to find the relative frequencies of Ngrams.

## About the Data

Each line in the TSV file has the following structure:

`ngram TAB year TAB match_count TAB volume_count NEWLINE`

For example, lines 1722089 and 1722090 in the file contain this data:

<table ><tbody><tr><th >ngram</th><th >year</th><th colspan="1" >match_count</th><th >volume_count</th></tr><tr><td ><p class="p1">Zoological Journal of the Linnean</p></td><td >2007</td><td colspan="1" >284</td><td >101</td></tr><tr><td colspan="1" ><p class="p1">Zoological Journal of the Linnean</p></td><td colspan="1" >2008</td><td colspan="1" >257</td><td colspan="1" >87</td></tr></tbody></table> 
  
In 2007, "Zoological Journal of the Linnean" occurred 284 times overall in 101
distinct books of the Google sample.

## Download and Set Up the Data

After downloading the file, you use the `dfs` storage plugin, and then select
data from the file as you would a table. In the SELECT statement, enclose the
path and name of the file in backticks.

  1. Download the compressed Google Ngram data from this location:  
    
     http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-5gram-20120701-zo.gz)

  2. Unzip the file.  
     A file named googlebooks-eng-all-5gram-20120701-zo appears.

  3. Change the file name to add a `.tsv` extension.  
The Drill `dfs` storage plugin definition includes a TSV format that requires
a file to have this extension.

## Query the Data

Get data about "Zoological Journal of the Linnean" that appears more than 250
times a year in the books that Google scans.

  1. Switch back to using the `dfs` storage plugin.
  
          USE dfs;

  2. Issue a SELECT statement to get the first three columns in the file. In the FROM clause of the example, substitute your path to the TSV file. In the WHERE clause, enclose the string literal "Zoological Journal of the Linnean" in single quotation marks. Limit the output to 10 rows.
     
         SELECT COLUMNS[0], COLUMNS[1], COLUMNS[2]
         FROM `/Users/drilluser/Downloads/googlebooks-eng-all-5gram-20120701-zo.tsv`
         WHERE ((columns[0] = 'Zoological Journal of the Linnean')
           AND (columns[2] > 250)) LIMIT 10;
           
     The output is:
     
         +------------+------------+------------+
         |   EXPR$0   |   EXPR$1   |   EXPR$2   |
         +------------+------------+------------+
         | Zoological Journal of the Linnean | 1993       | 297        |
         | Zoological Journal of the Linnean | 1997       | 255        |
         | Zoological Journal of the Linnean | 2003       | 254        |
         | Zoological Journal of the Linnean | 2007       | 284        |
         | Zoological Journal of the Linnean | 2008       | 257        |
         +------------+------------+------------+
         5 rows selected (1.599 seconds)

  3. Repeat the query using aliases to replace the column headers, such as EXPR$0, with user-friendly column headers, Ngram, Publication Date, and Frequency. In the FROM clause of the example, substitute your path to the TSV file. 
  
         SELECT COLUMNS[0] AS Ngram,
                COLUMNS[1] AS Publication_Date,
                COLUMNS[2] AS Frequency
         FROM `/Users/drilluser/Downloads/googlebooks-eng-all-5gram-20120701-zo.tsv`
         WHERE ((columns[0] = 'Zoological Journal of the Linnean')
             AND (columns[2] > 250)) LIMIT 10;

     The improved output is:

         +------------+------------------+------------+
         |   Ngram    | Publication_Date | Frequency  |
         +------------+------------------+------------+
         | Zoological Journal of the Linnean | 1993             | 297        |
         | Zoological Journal of the Linnean | 1997             | 255        |
         | Zoological Journal of the Linnean | 2003             | 254        |
         | Zoological Journal of the Linnean | 2007             | 284        |
         | Zoological Journal of the Linnean | 2008             | 257        |
         +------------+------------------+------------+
         5 rows selected (1.628 seconds)
