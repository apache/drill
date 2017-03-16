---
title: "Roles and Privileges"
date: 2017-03-15 00:30:47 UTC
parent: "Securing Drill"
---
Drill has two roles that perform different functions: 

* User (USER) role
* Administrator (ADMIN) role 

## User Role

Users can execute queries on data that he/she has access to. Each storage plugin manages the read/write permissions. Users can create views on top of data to provide granular access to that data.  

## Administrator Role

When authentication is enabled, only Drill users who are assigned Drill cluster administrator privileges can perform the following tasks:

- Change system-level options by issuing the ALTER SYSTEM command.
- Update a storage plugin configuration through the REST API or Web Console. 
- Users and administrators have different navigation bars in the web console. Various tabs are shown based on privilege. For example,  only administrators can see the Storage tab and create/read/update/delete storage plugin configuration.
- View profiles of all queries that all users have run or are currently running in a cluster.
- Cancel running queries that were launched by any user in the cluster.

See [Configuring Web Console and REST API Security]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security/) for more information.




