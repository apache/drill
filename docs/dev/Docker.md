# How to build, publish and run a Apache Drill Docker image

## Prerequisites

   To build an Apache Drill docker image, you need to have the following software installed on your system to successfully complete a build. 
  * [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
  * [Maven 3.3.3 or greater](https://maven.apache.org/download.cgi)
  * [Docker CE](https://store.docker.com/search?type=edition&offering=community)
  
   If you are using an older Mac or PC, additionally configure [docker-machine](https://docs.docker.com/machine/overview/#what-is-docker-machine) on your system

## Checkout
```
git clone https://github.com/apache/drill.git
```
## Build Drill
```
$ cd drill
$ mvn clean install
```   
## Build Docker Image
```
$ cd distribution
$ mvn dockerfile:build -Pdocker
```
## Push Docker Image
   
   By default, the docker image built above is configured to be pushed to [Drill Docker Hub](https://hub.docker.com/r/drill/apache-drill/) to create official Drill Docker images.
```   
$ cd distribution
$ mvn dockerfile:push -Pdocker
```    
  You can configure the repository in pom.xml to point to any private or public container registry, or specify it in your mvn command.
```  
$ cd distribution
$ mvn dockerfile:push -Pdocker -Pdocker.repository=<my_repo>
```
## Run Docker Container
   
   Running the Docker container should start Drill in embedded mode and connect to Sqlline. 
```    
$ docker run -i --name drill-1.14.0 -p 8047:8047 -t drill/apache-drill:1.14.0 /bin/bash
Jun 29, 2018 3:28:21 AM org.glassfish.jersey.server.ApplicationHandler initialize
INFO: Initiating Jersey application, version Jersey: 2.8 2014-04-29 01:25:26...
apache drill 1.14.0 
"json ain't no thang"
0: jdbc:drill:zk=local> select version from sys.version;
+------------+
|  version   |
+------------+
| 1.14.0     |
+------------+
1 row selected (0.28 seconds)
```  

   You can also run the container in detached mode and connect to sqlline using drill-localhost. 
```    
$ docker run -i --name drill-1.14.0 -p 8047:8047 --detach -t drill/apache-drill:1.14.0 /bin/bash
<displays container ID>

$ docker exec -it drill-1.14.0 bash
<connects to container>

$ /opt/drill/bin/drill-localhost
apache drill 1.14.0 
"json ain't no thang"
0: jdbc:drill:drillbit=localhost> select version from sys.version;
+------------+
|  version   |
+------------+
| 1.14.0     |
+------------+
1 row selected (0.28 seconds)
```

## Querying Data

   By default, you can only query files which are accessible within the container. For example, the sample data which ships with Drill. 
```
> select first_name, last_name from cp.`employee.json` limit 1;
+-------------+------------+
| first_name  | last_name  |
+-------------+------------+
| Sheri       | Nowmer     |
+-------------+------------+
1 row selected (0.256 seconds)
```

   To query files outside of the container, you can configure [docker volumes](https://docs.docker.com/storage/volumes/#start-a-service-with-volumes)
   
## Drill Web UI

   Drill web UI can be accessed using http://localhost:8047 once the Drill docker container is up and running. On Windows, you may need to specify the IP address of your system instead of 'localhost'.

## More information 

   For more information including how to run Apache Drill in a Docker container, visit the [Apache Drill Documentation](http://drill.apache.org/docs/)
