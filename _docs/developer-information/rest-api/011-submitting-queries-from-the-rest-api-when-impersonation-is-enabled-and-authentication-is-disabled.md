---
title: "Submitting Queries from the REST API when Impersonation is Enabled and Authentication is Disabled"
date: 2018-12-08
parent: "REST API"
---  


As of Drill 1.12, a user can enter a username to successfully run queries from the REST API when impersonation is enabled and authentication is disabled.  

Prior to Drill 1.12, there was no way to provide a username when running queries from the REST API if impersonation was enabled and the user issuing the query was not authenticated. The queries ran as an “anonymous” user and failed because “anonymous” does not exist. 
 
This feature only works when impersonation is enabled and authentication is disabled. If impersonation and authentication are both disabled, a user cannot enter a username and all queries run as the anonymous user.  

A user can issue queries through the Drill Web UI, SQLLine, using curl commands, or Java code, as follows:  

##Drill Web UI  
You can submit a query through the Query page in the Drill Web UI after entering a valid username in the Username field. The Drill Web UI is accessible through the URL http(s)://<ip-address>:8047.

To re-run a query from the Profiles page in the Drill Web UI, you must submit a username prior to re-running the query.
  

##SQLLine  

You can submit a query through SQLLine if you include a username when starting SQLLine, as follows:  

`./drill-localhost -n <username> `  

**Note:** The startup scripts are located in the `../bin` directory of the Drill installation. 

##Curl  

When making curl requests, you must include the User-Name header with the username in the command, as follows:  

`curl -v -H "Content-Type: application/json" -H "User-Name: user1" -d '{"queryType":"SQL", "query": "select * from sys.version"}' http://localhost:8047/query.json`  

**Note:** The User-Name header is required for POST requests to /query or /query.json. Failure to include the User-Name header results in an error. For other types of requests, the User-Name header is not required. If you do not include the User-Name header (or it is null or empty), the default principal “anonymous” is used.  


##Java 
 
When using an application to submit a query, you must include the User-Name header with the username in the connection request, as follows:  

       String url = "http://localhost:8047/query.json";
              URLConnection connection = new URL(url).openConnection();
              connection.setDoOutput(true); // Triggers POST.
              connection.addRequestProperty("User-Name", "user1");
              connection.setRequestProperty("Content-Type", "application/json");

              String data = "{\"queryType\":\"SQL\", \"query\": \"select * from sys.version\"}"; 
              try (OutputStream output = connection.getOutputStream()) {
                output.write(data.getBytes(StandardCharsets.UTF_8.name()));
              }  

              try (InputStream response = connection.getInputStream()) {
                String result = IOUtils.toString(response);
                System.out.println(result);
              }
         

**Note:** You can also use the Apache HttpClient.

  





