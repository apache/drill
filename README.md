# Apache Drill Milestone 1 - (Drill Alpha)

Apache Drill is a distributed mpp query layer that supports SQL and alternative query languages against NoSQL and Hadoop data storage systems.  It was inspired in part by the [Google's Dremel](http://research.google.com/pubs/pub36632.html).  It is currently incubating under the Apache Foundation.


## Milestone 1
Milestone 1 is focused on providing a technology preview for developers.  It offers the ability to query JSON and Parquet columnar data storage formats using ANSI compliant SQL.  It also provides the ability to run queries in distributed execution mode utilizing Apache Zookeeper as a cluster coordination service.


## Quickstart
To build, you need to have Java 7, protoc 2.5.x and maven 3.0 installed on your build system.  Currently, the Apache Drill build process is known to work on Linux and OSX.

To Build:

    git clone https://github.com/apache/incubator-drill.git
    cd incubator-drill
    mvn clean install
    ./sqlline -u jdbc:drill:schema=parquet-local -n admin -p admin

This starts up the sqlline JDBC CLI interface.  From here you can run various types of queries such as: 

    SELECT 
      _MAP['R_REGIONKEY'] AS region_key, 
      _MAP['R_NAME'] AS name, _MAP['R_COMMENT'] AS comment
    FROM "sample-data/region.parquet";

## More Information
Please see the [Apache Drill Website](http://incubator.apache.org/drill/) for more information including:

 * Remote Execution Installation Instructions
 * Information about how to submit logical and distributed physical plans
 * More example queries and sample data
 * Find out ways to be involved or disuss Drill


## Join the community!
Apache Drill is an Apache Foundation project and is seeking all types of contributions.  Please say hello on the Apache Drill mailing list or join our weekly Google Hangouts for more information.  (More information can be found at the Apache Drill website).

