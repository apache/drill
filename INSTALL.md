# How to build and run Apache Drill

## Prerequisites

Currently, the Apache Drill build process is known to work on Linux, Windows and OSX.  To build, you need to have the following software installed on your system to successfully complete a build. 
  * Java 7
  * Maven 3.x

## Confirm settings
    # java -version
    java version "1.7.0_09"
    Java(TM) SE Runtime Environment (build 1.7.0_09-b05)
    Java HotSpot(TM) 64-Bit Server VM (build 23.5-b02, mixed mode)
    
    # mvn --version
    Apache Maven 3.0.3 (r1075438; 2011-02-28 09:31:09-0800)

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
