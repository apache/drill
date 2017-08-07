---
title: "Secure Communication Paths"
date: 2017-08-07 19:02:29 UTC
parent: "Securing Drill"
---
As illustrated in the following figure, Drill features five secure communication paths. Drill 1.11 introduces encryption between a Drill client and Drillbit. 


1. [Web Client to Drillbit]({{site.baseurl}}/docs/server-communication-paths/#web-client-to-drillbit)
1. [C++ Client to Drillbit]({{site.baseurl}}/docs/server-communication-paths/#c++-client-to-drillbit)
1. [Java Client to Drillbit]({{site.baseurl}}/docs/server-communication-paths/#java-client-to-drillbit)
1. [Java Client and Drillbit to ZooKeeper]({{site.baseurl}}/docs/server-communication-paths/#java-client-and-drillbit-to-zookeeper)
1. [Drillbit to Hive Storage Plugin]({{site.baseurl}}/docs/server-communication-paths/#drillbit-to-hive-storage-plugin)

![secure comm paths]({{ site.baseurl }}/docs/img/secure-communication-paths.png)

## Web Client to Drillbit

The Web Console and REST API clients are web clients. Web clients can:

- Submit and monitor queries
- Configure storage plugins

---
**Note**

Impersonation and authorization are available through the web clients only when authentication is enabled. Otherwise, the user identity is unknown.

---

| Security Capability | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Reference                                                                                                                                                                                                        |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Authentication      | Users authenticate to a drillbit using a username and password form authenticator. By default, authentication is disabled.                                                                                                                                                                                                                                                                                                                                                      | [Configuring Web Console and REST API Security]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security)                                                                                             |
| Encryption          | Drill usese SSL for HTTPS access to the Web Console.                                                                                                                                                                                                                                                                                                                                                                                                                            | [Configuring Web Console and REST API Security]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security)                                                                                             |
| Impersonation       | Drill acts on behalf of the user on the session. This is usually the connection user (or the user that authenticates). This user can impersonate another user, which is allowed if the connection user is authorized to impersonate the target user based on the inbound impersonation policies (USER role).  By default, impersonation is disabled.                                                                                                                            | [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation/#impersonation-and-views) and [Configuring Inbound Impersonation]({{site.baseurl}}/docs/configuring-inbound-impersonation) |
| Authorization       | Queries execute on behalf of the web user. Users and administrators have different navigation bars. Various tabs are shown based on privileges. For example, only administrators can see the Storage tab and create/read/update/delete storage plugin configuration.                                                                                                                                                                                                            | [Configuring Web Console and REST API Security]({{site.baseurl}}/docs/configuring-web-console-and-rest-api-security)                                                                                             |         

## Java and C++ Client to Drillbit

Java (native or JDBC) and C++ (native or ODBC) clients submit queries to Drill. BI tools use the ODBC or JDBC API.

| Security Capability | Description                                                                                                                                                                                                                                                                                                                                                                                                                                             | Reference                                                            |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| Authentication      | Users authenticate to a drillbit using Kerberos, Plain (username and password through PAM), and Custom authenticator (username and password). By default, user authentication is disabled.                                                                                                                                                                                                                                                              | [Configuring User Security]({{site.baseurl}}/docs/configuring-user-security) | 
| Encryption          | Drill 1.11 supports encryption between a Drill client and a Drillbit with the Kerberos mechanism over a Java SASL framework. Encrypting the client-to-drillbit communication path ensures data integrity and privacy and prevents data tampering and snooping. If encryption is enabled on a drillbit, it will not allow a client without encryption capabilities to connect. By default, encryption is disabled.                                              | [Configuring Kerberos Security]({{site.baseurl}}/docs/configuring-kerberos-security)                                      |
| Impersonation       | Drill acts on behalf of the user on the session. This is usually the connection user (or the user that authenticates). This user can impersonate another user. This is allowed if the connection user is authorized to impersonate the target user based on the inbound impersonation policies (USER role).  By default, impersonation is disabled.                                                                                                     | [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation) and [Configuring Inbound Impersonation]({{site.baseurl}}/docs/configuring-inbound-impersonation) |
| Authorization       | A user can execute queries on data that he/she has access to. Each storage plugin manages the read/write permissions. Users can create views on top of data to provide granular access to that data. The user sets read permissions to appropriate users and/or groups.  System-level options can only be changed by administrators (USER role). By default, only the process user is an administrator. This is available if authentication is enabled. | [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation)               |

## Drill Client and Drillbit to ZooKeeper 

Drill clients and drillbits communicate with ZooKeeper to obtain the list of active drillbits. Drillbits store system-level options and running query profiles.

| Security Capability | Description                                         | Reference                       |
|---------------------|-----------------------------------------------------|---------------------------------|
| Authentication      | Not fully supported.                                | [Configuring User Security]({{site.baseurl}}/docs/configuring-user-security) |
| Authorization       | Drill does not set ACLs on ZooKeeper nodes (znode). |                                 |
| Encryption          | Not fully supported.                                | [ZooKeeper SSL User Guide](https://cwiki.apache.org/confluence/display/ZOOKEEPER/ZooKeeper+SSL+User+Guide "ZooKeeper SSL User Guide")        |

## Drillbit to Hive Storage Plugin

The planner accesses the Hive Metastore for metadata. During execution, query fragments scan data from Hive using the Hive storage plugin.

| Security Capability 	| Description 	| Reference 	|
|---------------------	|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| Authentication 	| Drillbit is a client to the Hive Metastore. Authentication options   include Kerberos and DIGEST. By default, authentication is disabled. 	| Kerberos (if Hive impersonation is disabled and Kerberos principal is   mentioned) and DIGEST (the only supported mechanism when Hive impersonation   is enabled and SASL is enabled). 	|
| Impersonation 	| While accessing Hive Metastore, Hive impersonation setting in the storage   plugin configuration overrides Drill’s impersonation setting. While scanning   data in Hive, Drill impersonation is applied. 	| [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation) 	|
| Authorization 	| Drill supports SQL standard-based authorization and storage-based   authorization. 	| [Configuring User Impersonation with Hive Authorization]({{site.baseurl}}/docs/configuring-user-impersonation-with-hive-authorization) 	|

