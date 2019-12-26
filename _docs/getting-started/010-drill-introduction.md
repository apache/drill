---
title: "Drill Introduction"
date: 2019-12-26
parent: "Getting Started"
---
Drill is an Apache open-source SQL query engine for Big Data exploration.
Drill is designed from the ground up to support high-performance analysis on
the semi-structured and rapidly evolving data coming from modern Big Data
applications, while still providing the familiarity and ecosystem of ANSI SQL,
the industry-standard query language. Drill provides plug-and-play integration
with existing Apache Hive and Apache HBase deployments.   

## What's New in Apache Drill 1.17  
* <a href='https://issues.apache.org/jira/browse/DRILL-6540'>DRILL-6540</a> - Upgrade to HADOOP-3.0 libraries. The hadoop-winutils version that worked for previous releases does not work with Drill 1.17 and later. Use the hadoop-winutils version provided with Drill 1.17 or use custom hadoop-winutils built for Hadoop 3.2.0.  
* <a href='https://issues.apache.org/jira/browse/DRILL-6739'>DRILL-6739</a> - Update Kafka libs to 2.0.0+ version
* <a href='https://issues.apache.org/jira/browse/DRILL-7401'>DRILL-7401</a> - Upgrade to Sqlline 1.9 
* <a href='https://issues.apache.org/jira/browse/DRILL-7200'>DRILL-7200</a> - Update Calcite to 1.19.0 / 1.20.0
* <a href='https://issues.apache.org/jira/browse/DRILL-5674'>DRILL-5674</a> - Support for .zip compression
* <a href='https://issues.apache.org/jira/browse/DRILL-6835'>DRILL-6835</a> - Schema provision using File / Table Function
* <a href='https://issues.apache.org/jira/browse/DRILL-7337'>DRILL-7337</a> - Support for vararg UDFs 
* <a href='https://issues.apache.org/jira/browse/DRILL-7096'>DRILL-7096</a> - Develop vector for canonical Map<K,V>
* <a href='https://issues.apache.org/jira/browse/DRILL-7343'>DRILL-7343</a> - User-Agent UDFs added

Hive complex types support:
* <a href='https://issues.apache.org/jira/browse/DRILL-7251'>DRILL-7251</a> - Read Hive array without nulls
* <a href='https://issues.apache.org/jira/browse/DRILL-7252'>DRILL-7252</a> - Read Hive map using Dict<K,V> vector
* <a href='https://issues.apache.org/jira/browse/DRILL-7253'>DRILL-7253</a> - Read Hive struct without nulls
* <a href='https://issues.apache.org/jira/browse/DRILL-7254'>DRILL-7254</a> - Read Hive union without nulls
* <a href='https://issues.apache.org/jira/browse/DRILL-7268'>DRILL-7268</a> - Read Hive array with parquet native reader

New format plugins support:
* <a href='https://issues.apache.org/jira/browse/DRILL-4303'>DRILL-4303</a> - ESRI Shapefile (shp) format plugin
* <a href='https://issues.apache.org/jira/browse/DRILL-7177'>DRILL-7177</a> - Format Plugin for Excel Files
* <a href='https://issues.apache.org/jira/browse/DRILL-6096'>DRILL-6096</a> - Provide mechanisms to specify field delimiters and quoted text for TextRecordWriter   
* Parquet format improvements, including runtime row group pruning (<a href='https://issues.apache.org/jira/browse/DRILL-7062'>DRILL-7062</a>), empty parquet creation (<a href='https://issues.apache.org/jira/browse/DRILL-7156'>DRILL-7156</a>), reading (<a href='https://issues.apache.org/jira/browse/DRILL-4517'>DRILL-4517</a>) support, and more.

Metastore support:
* <a href='https://issues.apache.org/jira/browse/DRILL-7272'>DRILL-7272</a> - Implement Drill Iceberg Metastore plugin
* <a href='https://issues.apache.org/jira/browse/DRILL-7273'>DRILL-7273</a> - Create operator for handling metadata
* <a href='https://issues.apache.org/jira/browse/DRILL-7357'>DRILL-7357</a> - Expose Drill Metastore data through INFORMATION_SCHEMA    


## What's New in Apache Drill 1.16  
- [ANALYZE TABLE statement]({{site.baseurl}}/docs/analyze-table/) to computes statistics on Parquet data ([DRILL-1328](https://issues.apache.org/jira/browse/DRILL-1328))   
- [CREATE OR REPLACE SCHEMA command]({{site.baseurl}}/docs/create-or-replace-schema/) to define a schema for text files ([DRILL-6964](https://issues.apache.org/jira/browse/DRILL-6964))   
- [REFRESH TABLE METADATA command]({{site.baseurl}}/docs/refresh-table-metadata/) can generate metadata cache files for specific columns ([DRILL-7058](https://issues.apache.org/jira/browse/DRILL-7058))  
- [SYSLOG (RFC-5424) Format Plugin]({{site.baseurl}}/docs/syslog-format-plugin/) ([DRILL-6582](https://issues.apache.org/jira/browse/DRILL-6582))
- [NEAREST DATE function]({{site.baseurl}}/docs/date-time-functions-and-arithmetic/#nearestdate) to facilitate time series analysis ([DRILL-7077](https://issues.apache.org/jira/browse/DRILL-7077))
- [Format plugin for LTSV files]({{site.baseurl}}/docs/ltsv-format-plugin/) ([DRILL-7014](https://issues.apache.org/jira/browse/DRILL-7014))  
- Ability to query Hive views, like querying Hive tables in a hive schema, for example `SELECT * FROM hive.`hive_view`; ([DRILL-540](https://issues.apache.org/jira/browse/DRILL-540))
- [Upgrade to SQLLine 1.7]({{site.baseurl}}/docs/configuring-the-drill-shell/) changes the default prompt to `apache drill (schema_name)>` or you can define a custom prompt using the command `!set prompt <new-prompt>`. ([DRILL-6989](https://issues.apache.org/jira/browse/DRILL-6989)) 
- Calcite updated to version 1.18.0 ([DRILL-6862](https://issues.apache.org/jira/browse/DRILL-6862))    
- Several Drill Web UI improvements, including:
	- [Storage plugin management improvements](https://drill.apache.org/docs/configuring-storage-plugins/#exporting-storage-plugin-configurations) ([DRILL-6562](https://issues.apache.org/jira/browse/DRILL-6562))  
	- [Query progress indicators and warnings ]({{site.baseurl}}/docs/query-profiles/#query-profile-warnings) ([DRILL-6879](https://issues.apache.org/jira/browse/DRILL-6879))
	- Ability to [limit the result size for better UI response]({{site.baseurl}}/docs/planning-and-execution-options/#setting-an-auto-limit-on-the-number-of-rows-returned-for-result-sets) ([DRILL-6050](https://issues.apache.org/jira/browse/DRILL-6050))  
	- Ability to [sort the list of profiles in the Drill Web UI]({{site.baseurl}}/docs/query-profiles/#viewing-a-query-profile) ([DRILL-6942](https://issues.apache.org/jira/browse/DRILL-6942)) 
	- [Display query state in query result page]({{site.baseurl}}/docs/starting-the-web-ui/#running-queries-from-the-web-ui) ([DRILL-6939](https://issues.apache.org/jira/browse/DRILL-6939))  
	- [Button to reset the options filter](https://drill.apache.org/docs/planning-and-execution-options/#setting-options-from-the-drill-web-ui) ([DRILL-6921](https://issues.apache.org/jira/browse/DRILL-6921))    

## What's New in Apache Drill 1.15  

- Drill can leverage [indexes]({{site.baseurl}}/docs/querying-indexes-introduction/) to create index-based query plans. ([DRILL-6381](https://issues.apache.org/jira/browse/DRILL-6381)) 
- Support for aliases in the [GROUP BY clause]({{site.baseurl}}/docs/group-by-clause/). ([DRILL-1248](https://issues.apache.org/jira/browse/DRILL-1248))
- [CROSS JOIN](https://drill.apache.org/docs/from-clause/#join-types) support. ([DRILL-786](https://issues.apache.org/jira/browse/DRILL-786))
- The INFORMATION_SCHEMA contains a [FILES table]({{site.baseurl}}/docs/querying-the-information-schema/#files) that you can query for information about directories and files. ([DRILL-6680](https://issues.apache.org/jira/browse/DRILL-6680)) 
- [System functions table]({{site.baseurl}}/docs/querying-system-tables/#querying-the-functions-table) that exposes the available SQL functions in Drill and also detects UDFs that have been dynamically loaded into Drill. ([DRILL-3988](https://issues.apache.org/jira/browse/DRILL-3988))
- New [system options table]({{site.baseurl}}/docs/querying-system-tables/#querying-the-options-table). ([DRILL-6684](https://issues.apache.org/jira/browse/DRILL-6684))
- Support for [TIMESTAMPADD]({{site.baseurl}}/docs/date-time-functions-and-arithmetic/#timestampadd) and [TIMESTAMPDIFF]({{site.baseurl}}/docs/date-time-functions-and-arithmetic/#timestampdiff) datetime functions. ([DRILL-3610](https://issues.apache.org/jira/browse/DRILL-3610))
- Ability to [secure znodes with custom ACLs]({{site.baseurl}}/docs/configuring-custom-acls-to-secure-znodes/) (Access Control Lists) ([DRILL-5671](https://issues.apache.org/jira/browse/DRILL-5671)).
- All [cast and data type conversion functions]({{site.baseurl}}/docs/data-type-conversion/) return null for an empty string ('') when the `drill.exec.functions.cast_empty_string_to_null` option is enabled. ([DRILL-6817](https://issues.apache.org/jira/browse/DRILL-6817))
- [Storage plugin names are case-insensitive]({{site.baseurl}}/docs/lexical-structure/). ([DRILL-6492](https://issues.apache.org/jira/browse/DRILL-6492))
- Ability to access your AWS access key ID and secret access key using the Credential Provider API for the [S3 storage plugin]({{site.baseurl}}/docs/s3-storage-plugin/#using-an-external-provider-for-credentials). ([DRILL-6662](https://issues.apache.org/jira/browse/DRILL-6662))
- [Upgrade to SQLLine 1.6]({{site.baseurl}}/docs/configuring-the-drill-shell/) includes the ability to add custom configuration. ([DRILL-3853](https://issues.apache.org/jira/browse/DRILL-3853))
- [New SQLLine connection parameters]({{site.baseurl}}/docs/configuring-the-drill-shell/#sqlline-connection-parameters) ([DRILL-3933](https://issues.apache.org/jira/browse/DRILL-3933))
- New option, `exec.query.return_result_set_for_ddl`, [prevents Drill from returning a result set for DDL statements]({{site.baseurl}}/docs/interfaces-introduction/) when set to "false." Useful for clients tools that connect to Drill (via JDBC) if they do not expect a result set. ([DRILL-6834](https://issues.apache.org/jira/browse/DRILL-6834))
- [Parquet filter pushdown for VARCHAR and DECIMAL data types]({{site.baseurl}}/docs/parquet-filter-pushdown/#parquet-filter-pushdown-for-varchar-and-decimal-data-types) ([DRILL-6744](https://issues.apache.org/jira/browse/DRILL-6744))
- Improved query performance with the [semi-join functionality](https://drill.apache.org/docs/sort-based-and-hash-based-memory-constrained-operators/#disabling-the-hash-operators) inside the Hash-Join operator. ([DRILL-6735](https://issues.apache.org/jira/browse/DRILL-6735))
- [Lateral join](https://drill.apache.org/docs/lateral-join/) functionality is enabled by default. ([DRILL-6729](https://issues.apache.org/jira/browse/DRILL-6729))
- Support JPPD (Join Predicate Push Down). [DRILL-6385](https://issues.apache.org/jira/browse/DRILL-6385)
- Multiple [Web UI improvements]({{site.baseurl}}/docs/planning-and-execution-options/#setting-options-from-the-drill-web-ui) to simplify the use of options and submit queries, including:
	- Search field 
	- Quick Filters ([DRILL-5735](https://issues.apache.org/jira/browse/DRILL-5735))
	- Default button ([DRILL-6668](https://issues.apache.org/jira/browse/DRILL-6668))
	- [Web display options]({{site.baseurl}}/docs/planning-and-execution-options/#setting-options-from-the-drill-web-ui) ([DRILL-6544](https://issues.apache.org/jira/browse/DRILL-6544)) 
	- [Meta+Enter key combination to submit queries]({{site.baseurl}}/docs/starting-the-web-ui/) ([DRILL-6611](https://issues.apache.org/jira/browse/DRILL-6611))  

## What's New in Apache Drill 1.14  
- Ability to [run Drill in a Docker container]({{site.baseurl}}/docs/running-drill-on-docker/). ([DRILL-6346](https://issues.apache.org/jira/browse/DRILL-6346))  
- Ability to [export and save your storage plugin configurations]({{site.baseurl}}/docs/configuring-storage-plugins/#exporting-storage-plugin-configurations) to a JSON file for reuse. ([DRILL-4580](https://issues.apache.org/jira/browse/DRILL-4580))  
- Ability to manage storage plugin configurations in the Drill configuration file, [storage-plugins-override.conf]({{site.baseurl}}/docs/configuring-storage-plugins/#configuring-storage-plugins-with-the-storage-plugins-override.conf-file). ([DRILL-6494](https://issues.apache.org/jira/browse/DRILL-6494))  
- [Functions that return data type information]({{site.baseurl}}/docs/data-type-functions/). ([DRILL-6361](https://issues.apache.org/jira/browse/DRILL-6361))  
- The Drill [kafka storage plugin supports filter pushdown for query conditions]({{site.baseurl}}/docs/kafka-storage-plugin/#filter-pushdown-support) on certain Kafka metadata fields in messages. ([DRILL-5977](https://issues.apache.org/jira/browse/DRILL-5977))  
- [Spill to disk]({{site.baseurl}}/docs/sort-based-and-hash-based-memory-constrained-operators/#spill-to-disk) for the Hash Join operator. ([DRILL-6027](https://issues.apache.org/jira/browse/DRILL-6027))  
- The dfs storage plugin supports a [Logfile plugin extension]({{site.baseurl}}/docs/logfile-plugin/) that enables Drill to directly read and query log files of any format. ([DRILL-6104](https://issues.apache.org/jira/browse/DRILL-6104))  
- [Phonetic]({{site.baseurl}}/docs/phonetic-functions/) and [string distance]({{site.baseurl}}/docs/phonetic-functions/) functions. ([DRILL-6519](https://issues.apache.org/jira/browse/DRILL-6519))  
- The [store.hive.conf.properties option]({{site.baseurl}}/docs/hive-storage-plugin/#setting-hive-properties) enables you to specify Hive properties at the session level using the SET command. ([DRILL-6575](https://issues.apache.org/jira/browse/DRILL-6575))  
- [Drill can directly manage the CPU resources]({{site.baseurl}}/docs/configuring-cgroups-to-control-cpu-usage/) through the Drill start-up script, drill-env.sh; you no longer have to manually add the PID to the cgroup.procs file each time a Drillbit restarts. ([DRILL-143](https://issues.apache.org/jira/browse/DRILL-143))  
- Drill can query the metadata in various image formats with the [image metadata format plugin]({{site.baseurl}}/docs/image-metadata-format-plugin/). ([DRILL-4364](https://issues.apache.org/jira/browse/DRILL-4364))  
- [Enhanced decimal data type support]({{site.baseurl}}/docs/supported-data-types/#decimal-data-type). ([DRILL-6094](https://issues.apache.org/jira/browse/DRILL-6094))  
- [Option to push LIMIT(0) on top of SCAN]({{site.baseurl}}/docs/limit-clause/#limit-0). ([DRILL-6574](https://issues.apache.org/jira/browse/DRILL-6574))  
- Parquet filter pushdown improvements:  
       - Drill can [infer filter conditions]({{site.baseurl}}/docs/parquet-filter-pushdown/#viewing-the-query-plan) for join queries and push the filter conditions down to the data source. ([DRILL-6173](https://issues.apache.org/jira/browse/DRILL-6173))  
       - Drill uses a native reader to read Hive tables when you enable the [store.hive.optimize_scan_with_native_readers option]({{site.baseurl}}/docs/configuration-options-introduction/). When enabled, Drill reads data faster and applies filter pushdown optimizations. ([DRILL-6331](https://issues.apache.org/jira/browse/DRILL-6331))  
- Early release of [lateral join]({{site.baseurl}}/docs/lateral-join/). ([DRILL-5999](https://issues.apache.org/jira/browse/DRILL-5999))        

## What's New in Apache Drill 1.13  
- JDK 8 support. ([DRILL-1491](https://issues.apache.org/jira/browse/DRILL-1491))    
- Upgrade to [Calcite version 1.15](https://calcite.apache.org/docs/history.html#v1-15-0). ([DRILL-3993](https://issues.apache.org/jira/browse/DRILL-3993)) 
- JDBC Statement.setQueryTimeout(int) support to cancel queries if they do not complete within the specified time. ([DRILL-3640](https://issues.apache.org/jira/browse/DRILL-3640))  
- Batch processing improvements that enable you to [limit the amount of memory]({{site.baseurl}}/docs/configuring-drill-memory/#modifying-memory-allocated-to-queries) that the Flatten, Merge Join, and External Sort operators allocate to outgoing batches. ([DRILL-6123](https://issues.apache.org/jira/browse/DRILL-6123))  
- Enhanced DESCRIBE command. ([DRILL-4559](https://issues.apache.org/jira/browse/DRILL-4559))   
- Support for SPNEGO to extend Kerberos to Web applications through HTTP. ([DRILL-5425](https://issues.apache.org/jira/browse/DRILL-5425))   
- Ability to run [Drill under YARN]({{site.baseurl}}/docs/drill-on-yarn/). ([DRILL-1170](https://issues.apache.org/jira/browse/DRILL-1170))   
- Parquet filter pushdown support for IS [NOT] NULL, TRUE, and FALSE operators and implicit and explicit casts for timestamp, date, and time data types. ([DRILL-6174](https://issues.apache.org/jira/browse/DRILL-6174))  
- Performance improvements with support for project push down, filter push down, and partition pruning on dynamically expanded columns when represented as a star in the ITEM operator. ([DRILL-6118](https://issues.apache.org/jira/browse/DRILL-6118))  
- Updated Hive libraries and the Drill Hive client updated to 2.3.2 with support for querying Hive transactional ORC bucketed tables. ([DRILL-5978](https://issues.apache.org/jira/browse/DRILL-5978))
- Ability to automatically manage memory allocations during Drill startup. ([DRILL-5741](https://issues.apache.org/jira/browse/DRILL-5741))  
- Ability to query an empty directory and use it for queries with any JOIN and UNION (UNION ALL) operators. ([Drill-4185](https://issues.apache.org/jira/browse/DRILL-4185))  
- Non-numeric support for JSON processing. ([Drill-5919](https://issues.apache.org/jira/browse/DRILL-5919))  
- New options to that enable you to configure the number of Jetty acceptors and selectors ([DRILL-5994](https://issues.apache.org/jira/browse/DRILL-5994))  
- Support SQL syntax highlighting of queries, auto-complete support in SQL editors, and snippets. ([DRILL-5868](https://issues.apache.org/jira/browse/DRILL-5868))  
- Improved performance of the Single Merge Exchange operator. ([DRILL-6115](https://issues.apache.org/jira/browse/DRILL-6115))   
- Like operator optimization. [DRILL-5879](https://issues.apache.org/jira/browse/DRILL-5879)    
- User/Distribution-specific configuration checks during startup ([DRILL-5741](https://issues.apache.org/jira/browse/DRILL-5741)).    
    

## What's New in Apache Drill 1.12  

Drill 1.12 provides the following new features and improvements:  
 
- Kafka and OpenTSDB storage plugins (DRILL-4779, DRILL-5337)
- SSL/TLS support (DRILL-5431)
- Network encryption support (DRILL-5682)
- Queue-based memory assignment for buffering operators (DRILL-5716)
- A collection of networking functions that facilitate network analysis using Drill (DRILL-5834)
- Support for the libpam4j PAM authenticator (DRILL-5820)
- Filter pushdown for Parquet can handle files with multiple rowgroups (DRILL-5795)
- UTF-8 is enabled in the query string by default (DRILL-5772)
- IF NOT EXISTS support for CREATE TABLE and CREATE VIEWS (DRILL-5952)
- Geometry functions, `ST_AsGeoJSON` and `ST_AsJSON`, that return GeoJSON and JSON representations (DRILL-5962, DRILL-5960) 
- JMX metrics for failed and canceled queries (DRILL-5909)
- Syntax highlighting and error checking for storage plugin configurations (DRILL-5981)
- System options improvements, including a new internal system options table (DRILL-5723)
- Ability to prevent users from accessing a path outside the current workspace (DRILL-5964)
- Ability to put the server in quiescent mode for a graceful shutdown (DRILL-4286)
- The Drill Web UI lists the completion of successfully completed queries as "successful" (DRILL-5923)


## What's New in Apache Drill 1.11  

Drill 1.11 provides the following new features and improvements:  

- Cryptography-related functions. (DRILL-5634)
- Spill to disk for the hash aggregate operator. (DRILL-5457)
- Format plugin support for PCAP files. (DRILL-5432)
- Ability to change the HDFS block Size for Parquet files. (DRILL-5379)
- Ability to store query profiles in memory. (DRILL-5481)
- Configurable CTAS directory and file permissions option. (DRILL-5391)
- Support for network encryption. (DRILL-4335)
- Relative paths stored in the metadata file. (DRILL-3867)
- Support for ANSI_QUOTES. (DRILL-3510)  


## What's New in Apache Drill 1.10  

Drill 1.10 provides the following new features and improvements:  

* Support for the [CREATE TEMPORARY TABLE AS (CTTAS)]({{site.baseurl}}/docs/create-temporary-table-as-cttas//) command.
* A [JDBC connection option]({{site.baseurl}}/docs/using-the-jdbc-driver/#using-the-jdbc-url-format-for-a-direct-drillbit-connection) that improves fault tolerance when connecting directly to a Drill node from a client.
* The [Web UI]({{site.baseurl}}/docs/identifying-multiple-drill-versions-in-a-cluster) displays the Drill version and additional query profile statistics.
* Drill implicitly interprets the [INT96]({{site.baseurl}}/docs/parquet-format/#about-int96-support/) timestamp data type in Parquet files.
* Support for Kerberos authentication between the client and drillbit.  
  

## What's New in Apache Drill 1.9  

Drill 1.9 provides the following new features:  

* Asynchronous Parquet reader
* Parquet filter pushdown
* Dynamic UDF support
* HTTPD format plugin   

## What's New in Apache Drill 1.8  

Drill 1.8 provides the following new features and changes: 

* Metadata cache pruning
* IF EXISTS parameter with the DROP TABLE and DROP VIEW commands
* DESCRIBE SCHEMA command
* Multi-byte delimiter support
* New parameters for filter selectivity estimates  
* Changes to the configuration and launch scripts - See [Configuration and Launch Script Changes]({{site.baseurl}}/docs/apache-drill-1-8-0-release-notes/#configuration-and-launch-script-changes)

## What's New in Apache Drill 1.7  

Drill 1.7 provides the following new features:  

* Monitoring via JMX
* Hive CHAR data type support
* HBase 1.x support  

## What's New in Apache Drill 1.6  

Drill 1.6 provides the following new features:  

* Inbound impersonation 
* Additional custom window frames 

## What's New in Apache Drill 1.5  

Drill 1.5 provides the following new features:  

* Authentication and security for the Web interface and REST API
* Experimental query support for Apache Kudu (incubating)
* An improved memory allocator
* Configurable caching for Hive metadata

## What's New in Apache Drill 1.4

Drill 1.4 introduces the following improvements:

* [select with options]({{site.baseurl}}/docs/plugin-configuration-basics/#using-the-formats-attributes-as-table-function-parameters) that you use in queries to change storage plugin settings
* Improved behavior when parsing CSV file header names
* A variable to set non-pretty, such as compact, printing of JSON
* Better drillbit.log files that include query text

Drill 1.4 fixes an error that occurred when you query a Hive table using the HBaseStorageHandler ([DRILL-3739](https://issues.apache.org/jira/browse/DRILL-3739)). To successfully query a Hive table using the HBaseStorageHandler, you need to configure the Hive storage plugin as described in the [Hive storage plugin documentation]({{site.baseurl}}/docs/hive-storage-plugin/#connect-drill-to-the-hive-remote-metastore).

## What's New in Apache Drill 1.3 
This releases fix issues and add a number of enhancements, including the following ones:

* [Enhanced Amazon S3 support]({{site.baseurl}}/docs/s3-storage-plugin/)  
* Hetrogeneous types  
  Support for columns that evolve from one data type to another over time. 
* [Text file headers]({{site.baseurl}}/docs/text-files-csv-tsv-psv/#using-a-header-in-a-file)
* [Sequence files support]({{site.baseurl}}/docs/querying-sequence-files/)
* Enhancements related to querying Hive tables, MongoDB collections, and Avro files

## What's New in Apache Drill 1.2

This release of Drill fixes [many issues]({{site.baseurl}}/docs/apache-drill-1-2-0-release-notes/) and introduces a number of enhancements, including the following ones:

* Support for JDBC data sources, such as MySQL, through a [new JDBC Storage plugin](https://issues.apache.org/jira/browse/DRILL-3180)  
* Improvements in the Drill JDBC driver including inclusion of
Javadocs and better application dependency compatibility  
* Enhancements to Avro file formats  
  * [Support for complex data types](https://issues.apache.org/jira/browse/DRILL-3565), such as UNION and MAP  
  * [Optimized Avro file processing](https://issues.apache.org/jira/browse/DRILL-3720) (block-wise)  
* Partition pruning improvements
* A number of new [SQL window functions]({{site.baseurl}}/docs/sql-window-functions)  
  * NTILE  
  * LAG and LEAD  
  * FIRST_VALUE and LAST_VALUE  
* [HTTPS support]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) for Web UI operations  
* Performance improvements for [querying HBase]({{site.baseurl}}/docs/querying-hbase/#querying-big-endian-encoded-data), which includes leveraging [ordered byte encoding]({{site.baseurl}}/docs/querying-hbase/#leveraging-hbase-ordered-byte-encoding)  
* [Optimized reads]({{site.baseurl}}/docs/querying-hive/#optimizing-reads-of-parquet-backed-tables) of Parquet-backed, Hive tables  
* Read support for the [Parquet INT96 type]({{site.baseurl}}/docs/parquet-format/#about-int96-support) and a new TIMESTAMP_IMPALA type used with the [CONVERT_FROM]({{site.baseurl}}/docs/supported-data-types/#data-types-for-convert_to-and-convert_from-functions) function decodes a timestamp from Hive or Impala.  
* [Parquet metadata caching]({{site.baseurl}}/docs/optimizing-parquet-metadata-reading/) to improve query performance on a large number of files
* DROP TABLE command  
* Improved correlated subqueries  
* Union Distinct  
* Improved LIMIT processing

## What's New in Apache Drill 1.1

Many enhancements in Apache Drill 1.1 include the following key features:

* [SQL window functions]({{site.baseurl}}/docs/sql-window-functions)
* [Partitioning data]({{site.baseurl}}) using the new [PARTITION BY]({{site.baseurl}}/docs/partition-by-clause) clause in the CTAS command
* [Delegated Hive impersonation]({{site.baseurl}}/docs/configuring-user-impersonation-with-hive-authorization/)
* Support for UNION and UNION ALL and better optimized plans that include UNION.

## What's New in Apache Drill 1.0

Apache Drill 1.0 offers the following new features:

* Many performance planning and execution [improvements](/docs/performance-tuning-introduction/).
* Updated [Drill shell]({{site.baseurl}}/docs/configuring-the-drill-shell) now formats query results.
* [Query audit logging]({{site.baseurl}}/docs/getting-query-information/) for getting the query history on a Drillbit.
* Improved connection handling.
* New Errors tab in the Query Profiles UI that facilitates troubleshooting and distributed storing of profiles.
* Support for a new storage plugin input format: [Avro](http://avro.apache.org/docs/current/spec.html)

In this release, Drill disables the DECIMAL data type, including casting to DECIMAL and reading DECIMAL types from Parquet and Hive. You can [enable the DECIMAL type](docs/supported-data-types/#enabling-the-decimal-type), but this is not recommended.

## Apache Drill Key Features

Key features of Apache Drill are:

  * Low-latency SQL queries
  * Dynamic queries on self-describing data in files (such as JSON, Parquet, text) and HBase tables, without requiring metadata definitions in the Hive metastore.
  * ANSI SQL
  * Nested data support
  * Integration with Apache Hive (queries on Hive tables and views, support for all Hive file formats and Hive UDFs)
  * BI/SQL tool integration using standard JDBC/ODBC drivers

##Quick Links
If you've never used Drill, visit these links to get a jump start:

* [Drill in 10 Minutes]({{ site.baseurl }}/docs/drill-in-10-minutes/)
* [Query Files]({{ site.baseurl }}/docs/querying-a-file-system)
* [Query HBase]({{ site.baseurl }}/docs/querying-hbase)
* [SQL Support]({{ site.baseurl }}/docs/sql-reference-introduction/)
* [Drill Tutorials]({{ site.baseurl }}/docs/tutorials-introduction)

