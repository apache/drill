# How to build and run Apache Drill

## Prerequisites

Currently, the Apache Drill build process is known to work on Linux, Windows and OSX.  To build, you need to have the following software installed on your system to successfully complete a build. 
  * Java 8
  * Maven 3.6.3 or greater

## Docker based build environment

The `start-build-env.sh` script in the root of the project source builds and starts a preconfigured environment
that contains all the tools needed to build Apache Drill from source.

This is known to work on Ubuntu 20.04 with Docker installed.
On other systems your success may vary. On Redhat/CentOS based systems no longer have Docker.

## Confirm settings
    # java -version
    java version "1.8.0_161"
    Java(TM) SE Runtime Environment (build 1.8.0_161-b12)
    Java HotSpot(TM) 64-Bit Server VM (build 25.161-b12, mixed mode)

    # mvn --version
    Apache Maven 3.6.3

## Formatter Configuration

Setting up IDE formatters is recommended and can be done by importing the following settings into your browser.
[Formatter File](../../dev-support/formatter)

## Checkout

    git clone https://github.com/apache/drill.git
    
## Build

    cd drill
    mvn clean install -DskipTests

## Build Quickly
This command works to build Drill in about 2 minutes for quick testing. 

    mvn install -T 4 -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Drat.skip=true -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dmaven.site.skip=true -Denforcer.skip=true -DskipIfEmpty=true -Dmaven.compiler.optimize=true

## Generate Dependency Report
    mvn clean site

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
