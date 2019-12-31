# How to build, publish and run a Apache Drill Docker image

## Prerequisites

   To build an Apache Drill docker image, you need to have the following software installed on your system to successfully complete a build. 
  * [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
  * [Maven 3.3.3 or greater](https://maven.apache.org/download.cgi)
  * [Docker CE](https://store.docker.com/search?type=edition&offering=community)

   If you are using an older Mac or PC, additionally configure [docker-machine](https://docs.docker.com/machine/overview/#what-is-docker-machine) on your system

## Drill Dockerfiles info

   Drill has two Dockerfiles:
   - Dockerfile placed in the project root is used for Automated Builds in DockerHub. It builds Drill when building Docker images.
   - Dockerfile placed in the distribution directory may be used during developing the project. It requires a pre-built Drill.

## Checkout
```
$ git clone https://github.com/apache/drill.git
```

## Build Drill
```
$ cd drill
$ mvn clean install
```   

## Build Docker Image
   To build a Docker image tagged with the current project version using dockerfile maven plugin using pre-built Drill, the following command may be used:
```
$ mvn dockerfile:build -Pdocker -pl distribution
```
## Push Docker Image
   By default, the docker image built above is configured to be pushed to [Drill Docker Hub](https://hub.docker.com/r/apache/drill/).
   **Please do not push images manually since it should be done using automated builds in DockerHub.**
```
$ mvn dockerfile:push -Pdocker -pl distribution
```
  You can configure the repository in `pom.xml` to point to any private or public container registry, or specify it in your mvn command.
```
$ mvn dockerfile:push -Pdocker -Ddocker.repository=<my_repo> -pl distribution
```
## Run Docker Container

   Running the Docker container should start Drill in embedded mode and connect to Sqlline. 
```
$ docker run --rm -i --name drill-1.17.0 -p 8047:8047 -t apache/drill:1.17.0 /bin/bash
Apache Drill 1.17.0
"JSON ain't no thang."
apache drill> select version from sys.version;
+---------+
| version |
+---------+
| 1.17.0  |
+---------+
1 row selected (1.65 seconds)
```

   You can also run the container in detached mode and connect to sqlline using drill-localhost. 
```
$ docker run -i --name drill-1.17.0 -p 8047:8047 --detach -t apache/drill:1.17.0 /bin/bash
<displays container ID>

$ docker exec -it drill-1.17.0 bash
<connects to container>

$ /opt/drill/bin/drill-localhost
Apache Drill 1.17.0
"JSON ain't no thang."
apache drill> select version from sys.version;
+---------+
| version |
+---------+
| 1.17.0  |
+---------+
1 row selected (1.65 seconds)
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

## Automated Builds in DockerHub

   Autobuild triggers a new build to [Drill Docker Hub](https://hub.docker.com/repository/docker/apache/drill) with every git push to Apache master repository or when new tags are added. It uses Dockerfile from project root directory which builds Drill when building docker image.

## More information 

   For more information including how to run Apache Drill in a Docker container, visit the [Apache Drill Documentation](http://drill.apache.org/docs/)
