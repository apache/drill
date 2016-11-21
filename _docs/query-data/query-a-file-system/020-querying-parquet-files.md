---
title: "Querying Parquet Files"
date: 2016-11-21 22:14:44 UTC
parent: "Querying a File System"
---

The Drill installation includes a `sample-data` directory with Parquet files
that you can query. Use SQL to query the `region.parquet` and
`nation.parquet` files in the `sample-data` directory.

{% include startnote.html %}The Drill installation location may differ from the examples used here.{% include endnote.html %} 

The examples assume that Drill was [installed in embedded mode]({{ site.baseurl }}/docs/installing-drill-in-embedded-mode). If you installed Drill in distributed mode, or your `sample-data` directory differs from the location used in the examples. Change the `sample-data` directory to the correct location before you run the queries.

## Region File

To view the data in the `region.parquet` file, issue the following query:

        SELECT * FROM dfs.`<path-to-installation>/apache-drill-<version>/sample-data/region.parquet`;

The query returns the following results:

    +--------------+--------------+-----------------------+
    | R_REGIONKEY  |    R_NAME    |       R_COMMENT       |
    +--------------+--------------+-----------------------+
    | 0            | AFRICA       | lar deposits. blithe  |
    | 1            | AMERICA      | hs use ironic, even   |
    | 2            | ASIA         | ges. thinly even pin  |
    | 3            | EUROPE       | ly final courts cajo  |
    | 4            | MIDDLE EAST  | uickly special accou  |
    +--------------+--------------+-----------------------+
    5 rows selected (0.272 seconds)

## Nation File

If you followed the Apache Drill in 10 Minutes instructions to install Drill
in embedded mode, the path to the parquet file varies between operating
systems.

To view the data in the `nation.parquet` file, issue the query appropriate for
your operating system:

        SELECT * FROM dfs.`<path-to-installation>/sample-data/nation.parquet`;

The query returns the following results:

    +--------------+-----------------+--------------+-----------------------+
    | N_NATIONKEY  |     N_NAME      | N_REGIONKEY  |       N_COMMENT       |
    +--------------+-----------------+--------------+-----------------------+
    | 0            | ALGERIA         | 0            |  haggle. carefully f  |
    | 1            | ARGENTINA       | 1            | al foxes promise sly  |
    | 2            | BRAZIL          | 1            | y alongside of the p  |
    | 3            | CANADA          | 1            | eas hang ironic, sil  |
    | 4            | EGYPT           | 4            | y above the carefull  |
    | 5            | ETHIOPIA        | 0            | ven packages wake qu  |
    | 6            | FRANCE          | 3            | refully final reques  |
    | 7            | GERMANY         | 3            | l platelets. regular  |
    | 8            | INDIA           | 2            | ss excuses cajole sl  |
    | 9            | INDONESIA       | 2            |  slyly express asymp  |
    | 10           | IRAN            | 4            | efully alongside of   |
    | 11           | IRAQ            | 4            | nic deposits boost a  |
    | 12           | JAPAN           | 2            | ously. final, expres  |
    | 13           | JORDAN          | 4            | ic deposits are blit  |
    | 14           | KENYA           | 0            |  pending excuses hag  |
    | 15           | MOROCCO         | 0            | rns. blithely bold c  |
    | 16           | MOZAMBIQUE      | 0            | s. ironic, unusual a  |
    | 17           | PERU            | 1            | platelets. blithely   |
    | 18           | CHINA           | 2            | c dependencies. furi  |
    | 19           | ROMANIA         | 3            | ular asymptotes are   |
    | 20           | SAUDI ARABIA    | 4            | ts. silent requests   |
    | 21           | VIETNAM         | 2            | hely enticingly expr  |
    | 22           | RUSSIA          | 3            |  requests against th  |
    | 23           | UNITED KINGDOM  | 3            | eans boost carefully  |
    | 24           | UNITED STATES   | 1            | y final packages. sl  |
    +--------------+-----------------+--------------+-----------------------+
    25 rows selected (0.102 seconds)
