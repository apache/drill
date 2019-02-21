# How to build and run Apache Drill

## Prerequisites

Currently, the Apache Drill build process is known to work on Linux, Windows and OSX.  To build, you need to have the following software installed on your system to successfully complete a build. 
  * Java 8
  * Maven 3.3.3 or greater

## Confirm settings
    # java -version
    java version "1.8.0_161"
    Java(TM) SE Runtime Environment (build 1.8.0_161-b12)
    Java HotSpot(TM) 64-Bit Server VM (build 25.161-b12, mixed mode)

    # mvn --version
    Apache Maven 3.3.3

## Checkout

    git clone https://github.com/apache/drill.git
    
## Build

    cd drill
    mvn clean install

## Explode tarball in installation directory
   
    mkdir /opt/drill
    tar xvzf distribution/target/*.tar.gz --strip=1 -C /opt/drill 

## Start SQLLine (which starts Drill in embedded mode)
    
    cd /opt/drill
    bin/sqlline -u jdbc:drill:zk=local -n admin -p admin

## Run a query

    SELECT 
      employee_id, 
      first_name
    FROM cp.`employee.json`; 
    
## More information 

For more information including how to run a Apache Drill cluster, visit the [Apache Drill Documentation](http://drill.apache.org/docs/)
