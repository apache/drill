---
title: "Running Drill on Docker"
date: 2018-03-18 20:02:37 UTC
parent: "Installing Drill in Embedded Mode"
---  

Starting in Drill 1.14, you can run Drill in a [Docker container](https://www.docker.com/what-container#/package_software). Running Drill in a container is the simplest way to start using Drill; all you need is the Docker client installed on your machine. You simply run a Docker command, and your Docker client downloads the Drill Docker image from the apache-drill repository on [Docker Hub](https://docs.docker.com/docker-hub/) and then brings up a container with Apache Drill  running in embedded mode.

**Note:** Currently, you can only run Drill in embedded mode in a Docker container. Embedded mode is when a single instance of Drill runs on a node or in a container. You do not have to perform any configuration tasks when Drill runs in embedded mode.  

## Prerequisite  

You must have the Docker client (version 18 or later) installed on your machine.  

- [Docker for Mac](https://www.docker.com/docker-mac)  
- [Docker for Windows](https://www.docker.com/docker-windows)  
- [Docker for Oracle Linux](https://www.docker.com/docker-oracle-linux)  

## Running Drill in a Docker Container  

You can start and run a Docker container in “detached” mode or “foreground” mode. Foreground is the default mode. Foreground mode runs the Drill process in the container and attaches the console to Drill’s standard input, output, and standard error. Detached mode runs the container in the background.

Whether you run the Docker container in detached or foreground mode, you start Drill in a container by issuing the docker run command with some options. 

The following table describes the options:  

| Option                       | Description                                                                                                                                                                                                                                                                                                              |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-i`                           | Keeps STDIN open. STDIN is standard input, an   input stream where data is sent to and read by a program.                                                                                                                                                                                                                |
| `-t`                           | Allocates a pseudo-tty (a shell).                                                                                                                                                                                                                                                                                        |
| `--name`                       | Identifies the container. If you do not use this   option to identify a name for the container, the daemon generates a random   string name for you. When you use this option to identify a container name,   you can use the name to reference the container within a Docker network in   foreground or detached mode.  |
| -p                           | The TCP port for the Drill Web UI. If needed, you can   change this port using the `drill.exec.http.port` [start-up option]({{site.baseurl}}/docs/start-up-options/).                                                                                                                                                                                                 |
| `drill/apache-drill:<version>` | The GitHub repository and tag. In the following   example, `drill/apache-drill` is   the repository and `1.14.0`   is the tag:     `drill/apache-drill:1.14.0`     The tag correlates with the version of Drill. When a new version of Drill   is available, you can use the new version as the tag.                           |
| `bin/bash`                     | Runs the Drill image downloaded from the   `apache-drill` repository.                                                                                                                                                                                                                                                      |  

### Running the Drill Docker Container in Foreground Mode  

Open a terminal window (Command Prompt or PowerShell, but not PowerShell ISE) and then issue the following command and opitons to connect to SQLLine (the Drill shell):   

       docker run -i --name drill-1.14.0 -p 8047:8047 -t drill/apache-drill:1.14.0 /bin/bash  

When you issue the docker run command, the Drill process starts in a container. SQLLine prints a message, and the prompt appears:  

       Jun 29, 2018 3:28:21 AM org.glassfish.jersey.server.ApplicationHandler initialize
       INFO: Initiating Jersey application, version Jersey: 2.8 2014-04-29 01:25:26...
       apache drill 1.14.0 
       "json ain't no thang"
       0: jdbc:drill:zk=local>  

At the prompt, you can enter the following simple query to verify that Drill is running:  

       SELECT version FROM sys.version;  

### Running the Drill Docker Container in Detached Mode  

Open a terminal window (Command Prompt or PowerShell, but not PowerShell ISE) and then issue the following commands and options to connect to SQLLine (the Drill shell):  

**Note:** When you run the Drill Docker container in Detached mode, you connect to SQLLine (the Drill shell) using drill-localhost.  

       $ docker run -i --name drill-1.14.0 -p 8047:8047 --detach -t drill/apache-drill:1.14.0 /bin/bash
       <displays container ID>

       $ docker exec -it drill-1.14.0 bash
       <connects to container>

       $ /opt/drill/bin/drill-localhost  

After you issue the commands, the Drill process starts in a container. SQLLine prints a message, and the prompt appears:  

       apache drill 1.14.0 
       "json ain't no thang"
       0: jdbc:drill:drillbit=localhost>  

At the prompt, you can enter the following simple query to verify that Drill is running:  

       SELECT version FROM sys.version;  

## Querying Data  

By default, you can only query files that are accessible within the container. For example, you can query the sample data packaged with Drill, as shown:  

       SELECT first_name, last_name FROM cp.`employee.json` LIMIT 1;
       +-------------+------------+
       | first_name  | last_name  |
       +-------------+------------+
       | Sheri       | Nowmer     |
       +-------------+------------+
       1 row selected (0.256 seconds)  

To query files outside of the container, you can configure [Docker volumes](https://docs.docker.com/storage/volumes/#start-a-service-with-volumes).  

## Drill Web UI  

You can access the Drill web UI at `http://localhost:8047` when the Drill Docker container is running. On Windows, you may need to specify the IP address of your system instead of using "localhost".






