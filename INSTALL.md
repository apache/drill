# How to build and run Apache Drill

## Prerequisites

Currently, the Apache Drill build process is known to work on Linux, Windows and OSX.  To build, you need to have the following software installed on your system to successfully complete a build. 
  * Java 7+
  * Maven 3.0+

## Confirm settings
    # java -version
    java version "1.7.0_09"
    Java(TM) SE Runtime Environment (build 1.7.0_09-b05)
    Java HotSpot(TM) 64-Bit Server VM (build 23.5-b02, mixed mode)
    
    # mvn --version
    Apache Maven 3.0.3 (r1075438; 2011-02-28 09:31:09-0800)

## Checkout

    git clone https://github.com/apache/incubator-drill.git
    
## Build

    cd incubator-drill
    mvn clean install
    
## Start SQLLine

    ./sqlline -u jdbc:drill:zk=local -n admin -p admin

## Run a query

    SELECT 
      region_key, 
      r_comment
    FROM "sample-data/region.parquet";
    
## More information 

For more information including how to run a Apache Drill cluster, visit the [Apache Drill Wiki](https://cwiki.apache.org/confluence/display/DRILL/Apache+Drill+Wiki)

