---
title: "PARTITION BY Clause"
date: 2018-11-02
parent: "SQL Commands"
---
The PARTITION BY clause in the CTAS command partitions data, which Drill [prunes]({{site.baseurl}}/docs/partition-pruning/) to improve performance when you query the data. (Drill 1.1.0)

{% include startnote.html %}Parquet data generated in Drill 1.2 and earlier needs to be migrated before attempting to use the data in later releases.{% include endnote.html %}

See [Migrating Parquet Data]({{site.baseurl}}/docs/migrating-parquet-data/).

<!-- Note: parquet files produced using Drill 1.1 and 1.2 will need to have their metadata rewritten to work with partition pruning in 1.3 and beyond, information on the simple migration process can be found on [Github](https://github.com/parthchandra/drill-upgrade). The need for this migration process came out of a bug fix included in the 1.3 release to accurately process parquet files produced by other tools (like Pig and Hive) that had a risk of inaccurate metadata that could cause inaccurate
results. Drill results have been accurate on files it created, and the files all contain accurate metadata, the migration tool simply marks these files as having been produced by Drill. Unfortunately without this migration we cannot reliably tell them apart from files produced by other tools. The migration tool should only be used on files produced by Drill, not those produced with other software products. Data from other tools will need to be read in and completely rewritten to generate
accurate metadata. This can be done using Drill or whatever tool originally produced them, as long as it is using a recent version of parquet (1.8 or later). -->

## Syntax

`[ PARTITION BY ( column_name[, . . .] ) ]`

The PARTITION BY clause partitions the data by the first column_name, and then subpartitions the data by the next column_name, if there is one, and so on. 

Only the Parquet storage format is supported for partitioning. Before using CTAS, [set the `store.format` option]({{site.baseurl}}/docs/create-table-as-ctas/#setting-the-storage-format) for the table to Parquet.

When the base table in the SELECT statement is schema-less, include columns in the PARTITION BY clause in the table's column list, or use a select all (SELECT *) statement:  

    CREATE TABLE dest_name [ (column, . . .) ]
    [ PARTITION BY (column, . . .) ] 
    AS SELECT column_list FROM <source_name>;

When columns in the source table have ambiguous names, such as COLUMNS[0], define one or more column aliases in the SELECT statement. Use the alias name or names in the CREATE TABLE list. List aliases in the same order as the corresponding columns in the SELECT statement. Matching order is important because Drill performs an overwrite operation.  

    CREATE TABLE dest_name (alias1, alias2, . . .) 
    [ PARTITION BY (alias1, . . . ) ] 
    AS SELECT column1 alias1, column2 alias2, . . .;

For example:

    CREATE TABLE by_yr (yr, ngram, occurrances) PARTITION BY (yr) AS SELECT columns[1] yr, columns[0] ngram, columns[2] occurrances FROM `googlebooks-eng-all-5gram-20120701-zo.tsv`;

When the partition column is resolved to * column (due to a SELECT * query) in a schema-less query, the * column cannot be a result of join operation. 

The output of CTAS using a PARTITION BY clause creates separate files. Each file contains one partition value, and Drill can create multiple files for the same partition value.

[Partition pruning]({{site.baseurl}}/docs/partition-pruning/) uses the Parquet column statistics to determine which columns can be used to prune.

## Partioning Example
This example uses a tab-separated value (TSV) files that you download from a
Google internet site. The data in the file consists of phrases from books that
Google scans and generates for its [Google Books Ngram
Viewer](https://storage.googleapis.com/books/ngrams/books/datasetsv2.html). You
use the data to find the relative frequencies of Ngrams.

### About the Data

Each line in the TSV file has the following structure:

`ngram TAB year TAB match_count TAB volume_count NEWLINE`

For example, lines 1722089 and 1722090 in the file contain this data:

| ngram                             | year | match_count | volume_count |
|-----------------------------------|------|-------------|--------------|
| Zoological Journal of the Linnean | 2007 | 284         | 101          |
| Zoological Journal of the Linnean | 2008 | 257         | 87           |
  
In 2007, "Zoological Journal of the Linnean" occurred 284 times overall in 101
distinct books of the Google sample.

### Creating a Partitioned Table of Ngram Data

For this example, you can use Drill in distributed mode to write 1.7M lines of data to a distributed file system, such as HDFS. Alternatively, you can use Drill in embedded mode and write fewer lines of data to a local file system. 

1. Download the compressed Google Ngram data from this location:  
    
        wget http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-5gram-20120701-zo.gz  
2. Unzip the file. On Linux for example use the `gzip -d` command:  
   `gzip -d googlebooks-eng-all-5gram-20120701-zo.gz`  
   A file named `googlebooks-eng-all-5gram-20120701-zo` appears.
3. Embedded mode: Move the unzipped file name to `/tmp` and add a `.tsv` extension. The Drill `dfs` storage plugin definition includes a TSV format that requires
a file to have this extension.  
   `mv googlebooks-eng-all-5gram-20120701-zo /tmp/googlebooks-eng-all-5gram-20120701-zo.tsv`  
   Distributed mode: Move the unzipped file to HDFS and add a `.tsv` extension.
   `hadoop fs -put googlebooks-eng-all-5gram-20120701-zo /tmp/googlebooks-eng-all-5gram-20120701-zo.tsv`
4. Start the Drill shell, and use the default `dfs.tmp` workspace, which is predefined as writable.  
   Distributed mode: This example assumes that the default `dfs` either works with your distributed file system out-of-the-box, or that you have adapted the storage plugin to your environment.  
   `USE dfs.tmp`;  
5. Set the `store.format` property to the default parquet if you changed the default.
   ``ALTER SESSION SET `store.format` = 'parquet';``  
6. Partition Google Ngram data by year in a directory named `by_yr`.  
   Embedded mode:  

        CREATE TABLE by_yr (yr, ngram, occurrances) PARTITION BY (yr) AS SELECT columns[1] yr, columns[0] ngram, columns[2] occurrances FROM `googlebooks-eng-all-5gram-20120701-zo.tsv` LIMIT 100;
    Output is:  

        +-----------+----------------------------+
        | Fragment  | Number of records written  |
        +-----------+----------------------------+
        | 0_0       | 100                        |
        +-----------+----------------------------+
        1 row selected (5.775 seconds)
    Distributed mode:  

        CREATE TABLE by_yr (yr, ngram, occurrances) PARTITION BY (yr) AS SELECT columns[1] yr, columns[0] ngram, columns[2] occurrances FROM `googlebooks-eng-all-5gram-20120701-zo.tsv`;
    Output is:

        +-----------+----------------------------+
        | Fragment  | Number of records written  |
        +-----------+----------------------------+
        | 0_0       | 1773829                    |
        +-----------+----------------------------+
        1 row selected (54.159 seconds)

    Drill writes more than 1.7M rows of data to the table. The files look something like this:

		0_0_1.parquet	0_0_28.parquet	0_0_46.parquet	0_0_64.parquet
		0_0_10.parquet	0_0_29.parquet	0_0_47.parquet	0_0_65.parquet
		0_0_11.parquet	0_0_3.parquet	0_0_48.parquet	0_0_66.parquet
		0_0_12.parquet	0_0_30.parquet	0_0_49.parquet	0_0_67.parquet
        . . .  
7. Query the `by_yr` directory to check to see how the data appears.  
   `SELECT * FROM by_yr LIMIT 100;`  
   The output looks something like this:

        +-------+------------------------------------------------------------+--------------+
        |  yr   |                           ngram                            | occurrances  |
        +-------+------------------------------------------------------------+--------------+
        | 1737  | Zone_NOUN ,_. and_CONJ the_DET Tippet_NOUN                 | 1            |
        | 1737  | Zones_NOUN of_ADP the_DET Earth_NOUN ,_.                   | 2            |
        . . .
        | 1737  | Zobah , David slew of                                      | 1            |
        | 1966  | zone by virtue of the  | 1           |
        +-------+------------------------+-------------+
        100 rows selected (2.184 seconds)
   Files are partitioned by year. The output is not expected to be in perfect sorted order because Drill reads files sequentially. 
8. Distributed mode: Query the data to find all ngrams in 1993.

        SELECT * FROM by_yr WHERE yr=1993;
        +-------+-------------------------------------------------------------+--------------+
        |  yr   |                            ngram                            | occurrances  |
        +-------+-------------------------------------------------------------+--------------+
        | 1993  | zoom out , click the                                        | 1            |
        | 1993  | zones on earth . _END_                                          | 4           |
        . . .
        | 1993  | zoology at Oxford University ,                                  | 5           |
        | 1993  | zones_NOUN ,_. based_VERB mainly_ADV on_ADP  | 2           |
        +-------+----------------------------------------------+-------------+
        31,100 rows selected (5.45 seconds)

    Drill performs partition pruning when you query partitioned data, which improves performance. Performance can be improved further by casting the yr and occurrances columns to INTEGER, as described in section ["Tips for Performant Querying"](/docs/text-files-csv-tsv-psv/#tips-for-performant-querying).
9. Distributed mode: Query the unpartitioned data to compare the performance of the query of the partitioned data in the last step.

        SELECT * FROM `/googlebooks-eng-all-5gram-20120701-zo.tsv` WHERE (columns[1] = '1993');

        SELECT * FROM `googlebooks-eng-all-5gram-20120701-zo.tsv` WHERE (columns[1] = '1993');
        +--------------------------------------------------------------------------------+
        |                                    columns                                     |
        +--------------------------------------------------------------------------------+
        | ["Zone , the government of","1993","1","1"]                                    |
        | ["Zone : Fragments for a","1993","7","7"]                                      |
        . . .
        | ["zooxanthellae_NOUN and_CONJ the_DET evolution_NOUN of_ADP","1993","4","3"]  |
        +-------------------------------------------------------------------------------+
        31,100 rows selected (8.389 seconds)

    The larger the data set you query, the greater the performance benefit from partition pruning. 

## Other Examples

    USE cp;
	CREATE TABLE mytable1 PARTITION BY (r_regionkey) AS 
	  SELECT r_regionkey, r_name FROM cp.`tpch/region.parquet`;
	CREATE TABLE mytable2 PARTITION BY (r_regionkey) AS 
	  SELECT * FROM cp.`tpch/region.parquet`;
	CREATE TABLE mytable3 PARTITION BY (r_regionkey) AS
	  SELECT r.r_regionkey, r.r_name, n.n_nationkey, n.n_name 
	  FROM cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r
	  WHERE n.n_regionkey = r.r_regionkey;



