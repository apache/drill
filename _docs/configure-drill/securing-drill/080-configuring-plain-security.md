---
title: "Configuring Plain Security"
date: 2017-12-19 00:54:51 UTC
parent: "Securing Drill"
---
Linux PAM provides a Plain (username and password) authentication module that interfaces with any installed PAM authentication entity, such as the local operating system password file (`/etc/passwd`) or LDAP. 
 
When using PAM for authentication, each user that has permission to run Drill queries must exist in the list of users that resides on each Drill node in the cluster. The username (including the `uid`) and password for each user must be identical across all Drill nodes. 

If you use PAM with `/etc/passwd` for authentication, verify that users with permission to start the Drill process belong to the shadow user group on all nodes in the cluster. This enables Drill to read the `/etc/shadow` file for authentication.

This section includes the following topics:

- [Authentication Process]({{site.baseurl}}/docs/configuring-plain-security/#authentication-process)
- [Connecting with SQLLine]({{site.baseurl}}/docs/configuring-plain-security/#connecting-with-sqlline)
- [Connecting with BI Tools]({{site.baseurl}}/docs/configuring-plain-security/#connecting-with-bi-tools)
- [Configuring Plain Security]({{site.baseurl}}/docs/configuring-plain-security/#configuring-plain-security)

## Authentication Process

The following image illustrates the PAM user authentication process in Drill.  The client passes a username and password to the Drillbit as part of the connection request, which then passes the credentials to PAM.  If PAM authenticates the user, the connection request passes the authentication phase and the connection is established. The user will be authorized to access Drill and issue queries against the file system or other storage plugins, such as Hive or HBase.  

![plain auth process]({{ site.baseurl }}/docs/img/plain-auth-process.png)

If PAM cannot authenticate the user, the connection request will not pass the authentication phase, and the user will not be authorized to access Drill. The connection is terminated as `AUTH_FAILED`.

For more PAM information (including a *JPAM User Guide*), see [JPAM](http://jpam.sourceforge.net/ "JPAM").

## Connecting with SQLLine

When Plain user authentication is enabled with PAM, each user that accesses the Drillbit process through a client, such as SQLLine, must provide username and password credentials for access. Users can include the `–n` and `–p` parameters with their username and password when launching SQLLine. 

**Example**

    sqlline –u jdbc:drill:zk=10.10.11.112:5181 –n bridget –p mypw007!)pwmy

Alternatively, a user can launch SQLLine and then issue the !connect command to hide the password.

1. Start SQLLine on Linux by running the sqlline script. 

	    bridgetsmachine:~$ /etc/drill/bin/sqlline
      	apache drill 1.10.0
      	"a drill in the hand is better than two in the bush"

1. At the SQLLine prompt, enter the !connect command followed by:
	`jdbc:drill:zk=zk=<zk name>[:<port>][,<zk name2>[:<port>]... `]`
	
	**Example**

        `sqlline> !connect jdbc:drill:zk=localhost:2181 scan complete in 1385m`s

1. When prompted, enter a username and password. The password is hidden as it is typed.
    
       	Enter username for jdbc:drill:zk=localhost:2181: bridget
      	Enter password for jdbc:drill:zk=localhost:2181: ************* 

## Connecting with BI Tools

When you connect to Drill from a BI tool, such as Tableau, the ODBC driver prompts you for the authentication type, username, and password. For PAM, select **Basic Authentication** in the Authentication Type drop-down menu.

![User Auth BI Tools](http://i.imgur.com/J5X1Tds.png)

##Configuring Plain Security

As of Drill 1.12, the libpam4j module is packaged with Drill. There is no download or external dependency required to use libpam4j. You can either use jpam or libpam4j as the PAM authenticator with Drill. Optionally, you can build and implement a custom authenticator.  



- To configure Drill to use libpam4j as the PAM authenticator, see [Using libpam4j as the PAM Authenticator]({{site.baseurl}}/docs/using-libpam4j-as-the-pam-authenticator/).
- To configure Drill to use jpam as the PAM authenticator, see [Using jpam as the PAM Authenticator]({{site.baseurl}}/docs/using-jpam-as-the-pam-authenticator/).  



