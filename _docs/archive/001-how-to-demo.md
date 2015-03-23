---
title: "How to Run the Drill Demo"
parent: "Archived Pages"
---
# How To Run the Drill Demo
This section describes how to get started by running the Drill demo.

## Pre-requisites

  * Maven 2 or higher

    On Ubuntu, you can do this as root:
    
        apt-get install maven2

    On the Mac, maven is pre-installed.

    Note that installing maven can result in installing java 1.6 and setting that
to your default version. Make sure you check java version before compiling or
running.

  * Java 1.7

    You will need java 1.7 to compile and run the Drill demo.

    On Ubuntu you can get the right version of Java by doing this as root:
    
        apt-get install openjdk-7-jdk
        sudo update-alternatives --set java $(update-alternatives --list java | grep 7 | head -1)

    On a Mac, go to [Oracle's web-
site](http://www.oracle.com/technetwork/java/javase/downloads/java-se-
jdk-7-download-432154.html) to download and install java 7. You will also need
to set JAVA_HOME in order to use the right version of java.

    Drill will not compile correctly using java 6. There is also a subtle problem
that can occur if you have both  
java 6 and java 7 with the default version set to 6. In that case, you may be
able to compile, but execution may not work correctly.

    Send email to the dev list if this is a problem for you.

  * Protobuf

    Drill requires Protobuf 2.5. Install this on Ubuntu using:
    
        apt-get install protobuf-compiler

    On Centos 6.4, OEL or RHEL you will need to compile protobuf-compiler:
    
		wget http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.bz2
		tar xfj protobuf-2.5.0.tar.bz2
		pushd protobuf-2.5.0
		./configure
		make
		sudo make install

  * git

    On Ubuntu you can install git by doing this as root:
    
        apt-get install git-all

On the Mac or Windows, go to [this site](http://git-scm.com/downloads) to
download and install git.

## Check your installation versions

Run

	java -version
	mvn -version

Verify that your default java and maven versions are correct, and that maven
runs the right version of java. On my Mac, you see something like this:

	ted:apache-drill-1.0.0-m1$ java -version
	java version "1.7.0_11"
	Java(TM) SE Runtime Environment (build 1.7.0_11-b21)
	Java HotSpot(TM) 64-Bit Server VM (build 23.6-b04, mixed mode)
	ted:apache-drill-1.0.0-m1$ mvn -version
	Apache Maven 3.0.3 (r1075438; 2011-02-28 09:31:09-0800)
	Maven home: /usr/share/maven
	Java version: 1.7.0_11, vendor: Oracle Corporation
	Java home: /Library/Java/JavaVirtualMachines/jdk1.7.0_11.jdk/Contents/Home/jre
	Default locale: en_US, platform encoding: UTF-8
	OS name: "mac os x", version: "10.7.5", arch: "x86_64", family: "mac"
	ted:apache-drill-1.0.0-m1$

## Get the Source

    git clone https://git-wip-us.apache.org/repos/asf/incubator-drill.git

## Compile the Code

	cd incubator-drill/sandbox/prototype
	mvn clean install -DskipTests
	rm .classpath

This takes about a minute on a not-terribly-current MacBook.

## Run the interactive Drill shell

    ./sqlline -u jdbc:drill:schema=parquet-local -n admin -p admin

The first time you run this program, you will get reams of output. What is
happening is that the program is running maven in order to build a
(voluminous) class path for the actual program and stores this classpath into
the file called `.classpath`. When you run this program again, it will note
that this file already exists and avoid re-creating it. You should delete this
file every time the dependencies of Drill are changed. If you start getting
"class not found" errors, that is a good hint that `.classpath` is out of date
and needs to be deleted and recreated.

The `-u` argument to [sqlline](https://github.com/julianhyde/sqlline) is a
JDBC connection string that directs sqlline to connect to drill. The Drill
JDBC driver currently includes enough smarts to run Drill in embedded mode so
this command also effectively starts a local drill bit. The `schema=` part of
the JDBC connection string causes Drill to consider the "parquet-local"
storage engine to be default. Other storage engines can be specified. The list
of supported storage engines can be found in the file
`./sqlparser/src/main/resources/storage-engines.json`. Each storage engine
specifies the format of the data and how to get the data. See the section
below on "Storage Engines" for more detail.

When you run sqlline, you should see something like this after quite a lot of
log messages:

	Connected to: Drill (version 1.0)
	Driver: Apache Drill JDBC Driver (version 1.0)
	Autocommit status: true
	Transaction isolation: TRANSACTION_REPEATABLE_READ
	sqlline version ??? by Marc Prud'hommeaux
	0: jdbc:drill:schema=parquet-local>

**Tip:** To quit sqlline at any time, type "!quit" at the prompt.

    0: jdbc:drill:schema=parquet-local> !quit

## Run a Query

Once you have sqlline running you can now try out some queries:

    select * from "sample-data/region.parquet";

You should see a number of debug messages and then something like this:

	+--------------------------------------------------------------------------------------------------------------------------------------------+
	|                                                                                  _MAP                                                      |
	+--------------------------------------------------------------------------------------------------------------------------------------------+
	| {"R_REGIONKEY":0,"R_NAME":"AFRICA","R_COMMENT":"lar deposits. blithely final packages cajole. regular waters are final requests. regular a |
	| {"R_REGIONKEY":1,"R_NAME":"AMERICA","R_COMMENT":"hs use ironic, even requests. s"}                                                         |
	| {"R_REGIONKEY":2,"R_NAME":"ASIA","R_COMMENT":"ges. thinly even pinto beans ca"}                                                            |
	| {"R_REGIONKEY":3,"R_NAME":"EUROPE","R_COMMENT":"ly final courts cajole furiously final excuse"}                                            |
	| {"R_REGIONKEY":4,"R_NAME":"MIDDLE EAST","R_COMMENT":"uickly special accounts cajole carefully blithely close requests. carefully final asy |
	+--------------------------------------------------------------------------------------------------------------------------------------------+
	5 rows selected (1.103 seconds)

Drill has no idea what the structure of this
file is in terms of what fields exist, but Drill does know that every record
has a pseudo-field called `_MAP`. This field contains a map of all of the
actual fields to values. When returned via JDBC, this fields is rendered as
JSON since JDBC doesn't really understand maps.

This can be made more readable by using a query like this:

	select _MAP['R_REGIONKEY'] as region_key, _MAP['R_NAME'] as name, _MAP['R_COMMENT'] as comment
	from "sample-data/region.parquet";

The output will look something like this:

	+-------------+--------------+---------------------------------------------------------------------------------------------------------------+
	| REGION_KEY  |     NAME     |                                                       COMMENT                                                 |
	+-------------+--------------+---------------------------------------------------------------------------------------------------------------+
	| 0           | AFRICA       | lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are accordi |
	| 1           | AMERICA      | hs use ironic, even requests. s                                                                               |
	| 2           | ASIA         | ges. thinly even pinto beans ca                                                                               |
	| 3           | EUROPE       | ly final courts cajole furiously final excuse                                                                 |
	| 4           | MIDDLE EAST  | uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl  |
	+-------------+--------------+---------------------------------------------------------------------------------------------------------------+

In upcoming versions, Drill will insert the `_MAP[ ... ]` goo and will also
unwrap the contents of `_MAP` in results so that things seem much more like
ordinary SQL. The reason that things work this way now is that SQL itself
requires considerable type information for queries to be parsed and that
information doesn't necessarily exist for all kinds of files, especially those
with very flexible schemas. To avoid all these problems, Drill adopts the
convention of the _MAP fields for all kinds of input.

## A Note Before You Continue

Drill currently supports a wide variety of queries. It currently also has a
fair number of deficiencies in terms of the number of operators that are
actually supported and exactly which expressions are passed through to the
execution engine in the correct form for execution.

These problems fall into roughly three categories,

  * missing operators. Many operators have been implemented for only a subset of the types available in Drill. This will cause queries to work for some types of data, but not for others. This is particularly true for operators with many possible type signatures such as comparisons. This lack is being remedied at a fast pace so check back in frequently if you suspect this might be a problem.

    Missing operators will result in error messages like this:

      `UnsupportedOperationException:[ Missing function implementation: compare_to
(BIT-OPTIONAL, BIT-OPTIONAL) ]`

  * missing casts. The SQL parser currently has trouble producing a valid logical plan without sufficient type information. Ironically, this type information is often not necessary to the execution engine because Drill generates the code on the fly based on the types of the data it encounters as data are processed. Currently, the work-around is to cast fields in certain situations to give the parser enough information to proceed. This problem will be remedied soon, but probably not quite as quickly as the missing operators.

    The typical error message that indicates you need an additional cast looks
like

    `Cannot apply '>' to arguments of type '<ANY> > <CHAR(1)>'. Supported
form(s): '<COMPARABLE_TYPE> > <COMPARABLE_TYPE>'`

 * weak optimizer. The current optimizer that transforms the logical plan into a physical plan is not the fully-featured cost based optimizer that Optiq normally uses. This is because some of the transformations that are needed for Drill are not yet fully supported by Optiq. In order to allow end-to-end execution of queries, a deterministic peep-hole optimizer has been used instead. This optimizer cannot handle large plan transformations and so some queries cannot be transformed correctly from logical to physical plan. We expect that the necessary changes to the cost-based optimizer will allow it to be used in an upcoming release, but didn't want to delay the current release waiting for that to happen.

## Try Fancier Queries

This query does a join between two files:

	SELECT nations.name, regions.name FROM (
	  SELECT _MAP['N_REGIONKEY'] as regionKey, _MAP['N_NAME'] as name
	  FROM "sample-data/nation.parquet") nations
	join (
	  SELECT _MAP['R_REGIONKEY'] as regionKey, _MAP['R_NAME'] as name
	  FROM "sample-data/region.parquet") regions
	  on nations.regionKey = regions.regionKey
	  order by nations.name;

Notice the use of sub-queries to avoid the spread of the `_MAP` idiom.

This query illustrates how a cast is currently necessary to make the parser
happy:

	SELECT
	  _MAP['N_REGIONKEY'] as regionKey,
	  _MAP['N_NAME'] as name
	FROM
	  "sample-data/nation.parquet"
	WHERE
	  cast(_MAP['N_NAME'] as varchar) IN ('MOROCCO', 'MOZAMBIQUE');

Here are more queries that you can try.

	// count distinct
	SELECT count(distinct _MAP['N_REGIONKEY']) FROM "sample-data/nation.parquet";
	
	// aliases
	SELECT
	  _MAP['N_REGIONKEY'] as regionKey,
	  _MAP['N_NAME'] as name
	FROM "sample-data/nation.parquet";
	
	// order by
	SELECT
	  _MAP['N_REGIONKEY'] as regionKey,
	  _MAP['N_NAME'] as name
	FROM
	  "sample-data/nation.parquet"
	ORDER BY
	  _MAP['N_NAME'] DESC;
	  
	  // subquery order by
	select * from (
	SELECT
	  _MAP['N_REGIONKEY'] as regionKey,
	  _MAP['N_NAME'] as name
	FROM
	  "sample-data/nation.parquet"
	) as x
	ORDER BY
	  name DESC;
	  
	  // String where
	SELECT
	  _MAP['N_REGIONKEY'] as regionKey,
	  _MAP['N_NAME'] as name
	FROM
	  "sample-data/nation.parquet"
	WHERE
	  cast(_MAP['N_NAME'] as varchar) > 'M';
	  
	  // INNER Join + Order (parquet)
	SELECT n.name, r.name FROM
	(SELECT _MAP['N_REGIONKEY'] as regionKey, _MAP['N_NAME'] as name FROM "sample-data/nation.parquet")n
	join (SELECT _MAP['R_REGIONKEY'] as regionKey, _MAP['R_NAME'] as name FROM "sample-data/region.parquet")r
	using (regionKey);
	
	// INNER Join + Order (parquet)
	SELECT n.name, r.name FROM
	(SELECT _MAP['N_REGIONKEY'] as regionKey, _MAP['N_NAME'] as name FROM "sample-data/nation.parquet")n
	join (SELECT _MAP['R_REGIONKEY'] as regionKey, _MAP['R_NAME'] as name FROM "sample-data/region.parquet")r
	on n.regionKey = r.regionKey
	order by n.name;

## Analyze the Execution of Queries

Drill sends log events to a logback socket appender. This makes it easy to
catch and filter these log events using a tool called Lilith. You can download
[Lilith](http://www.huxhorn.de/) and install it easily. A tutorial can be
[found here](http://ekkescorner.wordpress.com/2009/09/05/osgi-logging-part-8
-viewing-log-events-lilith/). This is especially important if you find errors
that you want to report back to the mailing list since Lilith will help you
isolate the stack trace of interest.

By default, Lilith uses a slightly lurid splash page based on a pre-Raphaelite
image of the mythical Lilith. This is easily disabled if the image is not to
your taste (or if your work-mates are not well-versed in Victorian views of
Sumerian mythology).

