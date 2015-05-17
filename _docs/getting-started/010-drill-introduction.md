---
title: "Drill Introduction"
parent: "Getting Started"
---
Drill is an Apache open-source SQL query engine for Big Data exploration.
Drill is designed from the ground up to support high-performance analysis on
the semi-structured and rapidly evolving data coming from modern Big Data
applications, while still providing the familiarity and ecosystem of ANSI SQL,
the industry-standard query language. Drill provides plug-and-play integration
with existing Apache Hive and Apache HBase deployments. 

Apache Drill 1.0 offers the following new features:

* Many performance planning and execution improvements, including a new text reader for faster join planning that complies with RFC 4180.
* Updated [Drill shell]({{site.baseurl}}/docs/configuring-the-drill-shell/#examples-of-configuring-the-drill-shell) and now formats query results having fewer than 70 characters in a column.
* [Query audit logging]({{site.baseurl}}/docs/getting-query-information/) for getting the query history on a Drillbit.
* Improved connection handling.
* New Errors tab in the Query Profiles UI that facilitates troubleshooting and distributed storing of profiles.
* Support for new storage plugin format: [Avro](http://avro.apache.org/docs/current/spec.html)

Key features of Apache Drill are:

  * Low-latency SQL queries
  * Dynamic queries on self-describing data in files (such as JSON, Parquet, text) and MapR-DB/HBase tables, without requiring metadata definitions in the Hive metastore.
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

## Videos

<table>
  <tr>
    <th><a href="http://www.youtube.com/watch?feature=player_embedded&v=HITzj3ihSUk
" target="_blank"><img src="http://img.youtube.com/vi/HITzj3ihSUk/0.jpg" 
alt="intro to Drill" width="240" height="180" border="10" />Introduction to Apache Drill</a></th>
    <th><a href="http://www.youtube.com/watch?feature=player_embedded&v=FkcegazNuio
" target="_blank"><img src="http://img.youtube.com/vi/FkcegazNuio/0.jpg" 
alt="Tableau and Drill" width="240" height="180" border="10" />Tableau + Drill</a</th>
  </tr>
  <tr>
    <td><a href="http://www.youtube.com/watch?feature=player_embedded&v=kG6vzsk8T7E
" target="_blank"><img src="http://img.youtube.com/vi/kG6vzsk8T7E/0.jpg" 
alt="drill config options" width="240" height="180" border="10" />Drill Configuration Options</a></td>
    <td><a href="http://www.youtube.com/watch?feature=player_embedded&v=XUIKlsX8yVM
" target="_blank"><img src="http://img.youtube.com/vi/XUIKlsX8yVM/0.jpg" 
alt="Self-service SQL Exploration on Mongo" width="240" height="180" border="10" />Self-service SQL Exploration on MongoDB</a></td>
  </tr>
  <tr>
    <td><a href="http://www.youtube.com/watch?feature=player_embedded&v=uyN9DDCNP8o
" target="_blank"><img src="http://img.youtube.com/vi/uyN9DDCNP8o/0.jpg" 
alt="Microstrategy and Drill" width="240" height="180" border="10" />Microstrategy + Drill</a></td>
    <td></td>
  </tr>
</table>

