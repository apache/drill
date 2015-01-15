---
title: "Querying Parquet Files"
parent: "Querying a File System"
---
Your Drill installation includes a `sample-date` directory with Parquet files
that you can query. Use SQL syntax to query the `region.parquet` and
`nation.parquet` files in the `sample-data` directory.

**Note:** Your Drill installation location may differ from the examples used here. The examples assume that Drill was installed in embedded mode on your machine following the [Apache Drill in 10 Minutes ](https://cwiki.apache.org/confluence/display/DRILL/Apache+Drill+in+10+Minutes)tutorial. If you installed Drill in distributed mode, or your `sample-data` directory differs from the location used in the examples, make sure to change the `sample-data` directory to the correct location before you run the queries.

#### Region File

If you followed the Apache Drill in 10 Minutes instructions to install Drill
in embedded mode, the path to the parquet file varies between operating
systems.

To view the data in the `region.parquet` file, issue the query appropriate for
your operating system:

  * Linux  
``SELECT * FROM dfs.`/opt/drill/apache-drill-0.4.0-incubating/sample-
data/region.parquet`; ``

   * Mac OS X  
``SELECT * FROM dfs.`/Users/max/drill/apache-drill-0.4.0-incubating/sample-
data/region.parquet`;``

   * Windows  
``SELECT * FROM dfs.`C:\drill\apache-drill-0.4.0-incubating\sample-
data\region.parquet`;``

The query returns the following results:

    +------------+------------+
    |   EXPR$0   |   EXPR$1   |
    +------------+------------+
    | AFRICA     | lar deposits. blithely final packages cajole. regular waters ar |
    | AMERICA    | hs use ironic, even requests. s |
    | ASIA       | ges. thinly even pinto beans ca |
    | EUROPE     | ly final courts cajole furiously final excuse |
    | MIDDLE EAST | uickly special accounts cajole carefully blithely close reques |
    +------------+------------+
    5 rows selected (0.165 seconds)
    0: jdbc:drill:zk=local>

#### Nation File

If you followed the Apache Drill in 10 Minutes instructions to install Drill
in embedded mode, the path to the parquet file varies between operating
systems.

To view the data in the `nation.parquet` file, issue the query appropriate for
your operating system:

  * Linux  
``SELECT * FROM dfs.`/opt/drill/apache-drill-0.4.0-incubating/sample-
data/nation.parquet`; ``

  * Mac OS X  
``SELECT * FROM dfs.`/Users/max/drill/apache-drill-0.4.0-incubating/sample-
data/nation.parquet`;``

  * Windows  
``SELECT * FROM dfs.`C:\drill\apache-drill-0.4.0-incubating\sample-
data\nation.parquet`;``

The query returns the following results:

    +------------+------------+------------+------------+
    |   EXPR$0   |   EXPR$1   |   EXPR$2   |   EXPR$3   |
    +------------+------------+------------+------------+
    | 0          | 0          | ALGERIA    |  haggle. carefully final deposits det |
    | 1          | 1          | ARGENTINA  | al foxes promise slyly according to t |
    | 2          | 1          | BRAZIL     | y alongside of the pending deposits.  |
    | 3          | 1          | CANADA     | eas hang ironic, silent packages. sly |
    | 4          | 4          | EGYPT      | y above the carefully unusual theodol |
    | 5          | 0          | ETHIOPIA   | ven packages wake quickly. regu |
    | 6          | 3          | FRANCE     | refully final requests. regular, iron |
    | 7          | 3          | GERMANY    | l platelets. regular accounts x-ray:  |
    | 8          | 2          | INDIA      | ss excuses cajole slyly across the pa |
    | 9          | 2          | INDONESIA  |  slyly express asymptotes. regular de |
    | 10         | 4          | IRAN       | efully alongside of the slyly final d |
    | 11         | 4          | IRAQ       | nic deposits boost atop the quickly f |
    | 12         | 2          | JAPAN      | ously. final, express gifts cajole a |
    | 13         | 4          | JORDAN     | ic deposits are blithely about the ca |
    | 14         | 0          | KENYA      |  pending excuses haggle furiously dep |
    | 15         | 0          | MOROCCO    | rns. blithely bold courts among the c |
    | 16         | 0          | MOZAMBIQUE | s. ironic, unusual asymptotes wake bl |
    | 17         | 1          | PERU       | platelets. blithely pending dependenc |
    | 18         | 2          | CHINA      | c dependencies. furiously express not |
    | 19         | 3          | ROMANIA    | ular asymptotes are about the furious |
    | 20         | 4          | SAUDI ARABIA | ts. silent requests haggle. closely |
    | 21         | 2          | VIETNAM    | hely enticingly express accounts. eve |
    | 22         | 3          | RUSSIA     |  requests against the platelets use n |
    | 23         | 3          | UNITED KINGDOM | eans boost carefully special requ |
    | 24         | 1          | UNITED STATES | y final packages. slow foxes cajol |
    +------------+------------+------------+------------+
    25 rows selected (2.401 seconds)
    0: jdbc:drill:zk=local>