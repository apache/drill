---
title: "Configuring Plain Authentication"
date: 2017-05-17 01:38:50 UTC
parent: "Securing Drill"
---
Linux PAM provides a Plain (or username and password) authentication module that interface with any installed PAM authentication entity, such as the local operating system password file (`/etc/passwd`) or LDAP. 
When using PAM for authentication, each user that has permission to run Drill queries must exist in the list of users that resides on each Drill node in the cluster. The username (including `uid`) and password for each user must be identical across all Drill nodes. 

If you use PAM with `/etc/passwd` for authentication, verify that the users with permission to start the Drill process are part of the shadow user group on all nodes in the cluster. This enables Drill to read the `/etc/shadow` file for authentication.

This section includes the following topics:

- [Authentication Process]({{site.baseurl}}/docs/configuring-plain-authentication/#authentication-process)
- [Connecting with SQLLine]({{site.baseurl}}/docs/configuring-plain-authentication/#connecting-with-sqlline)
- [Connecting with BI Tools]({{site.baseurl}}/docs/configuring-plain-authentication/#connecting-with-bi-tools)
- [Installing and Configuring Plain Authentication]({{site.baseurl}}/docs/configuring-plain-authentication/#installing-and-configuring-plain-authentication)

## Authentication Process

The following image illustrates the PAM user authentication process in Drill.  The client passes a username and password to the drillbit as part of the connection request, which then passes the credentials to PAM.  If PAM authenticates the user, the connection request passes the authentication phase and the connection is established. The user will be authorized to access Drill and issue queries against the file system or other storage plugins, such as Hive or HBase.  

![plain auth process]({{ site.baseurl }}/docs/img/plain-auth-process.png)

If PAM cannot authenticate the user, the connection request will not pass the authentication phase, and the user will not be authorized to access Drill. The connection is terminated as `AUTH_FAILED`.

For more PAM information (including a *JPAM User Guide*), see [JPAM](http://jpam.sourceforge.net/ "JPAM").

## Connecting with SQLLine

When Plain user authentication is enabled with PAM, each user that accesses the drillbit process through a client, such as SQLLine, must provide username and password credentials for access. Users can include the `–n` and `–p` parameters with their username and password when launching SQLLine. 

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

To connect to a Drill from a BI tool, such as Tableau, the ODBC driver prompts you for the authentication type, username, and password. For PAM, select **Basic Authentication** in the Authentication Type drop-down menu.

![User Auth BI Tools](http://i.imgur.com/J5X1Tds.png)

##Installing and Configuring Plain Authentication

Install and configure the provided Drill PAM for Plain (or username and password) authentication. Drill only supports the PAM provided here. Optionally, you can build and implement a custom authenticator.  

{% include startnote.html %}Do not point to an existing directory where other Hadoop components are installed. Other file system libraries can conflict with the Drill libraries and cause system errors.{% include endnote.html %}


Complete the following steps to install and configure PAM for Drill:

1. Download the `tar.gz` file for the Linux platform:

	`http://sourceforge.net/projects/jpam/files/jpam/jpam-1.1`/

1. Untar the file, and copy the `libjpam.so` file into a directory that does not contain other Hadoop components. For example, `/opt/pam/`


1. Add the following line to `<DRILL_HOME>/conf/drill-env.sh`, including the directory where the `libjpam.s`o file is located: 

    `export DRILLBIT_JAVA_OPTS="-Djava.library.path=<directory>"` 

	**Example**

    	`export DRILLBIT_JAVA_OPTS="-Djava.library.path=/opt/pam/"` 

1. Add the following configuration to the drill.exec block in `<DRILL_HOME>/conf/drill-override.conf`: 
		
              drill.exec: {
                cluster-id: "drillbits1",
                zk.connect: "qa102-81.qa.lab:5181,qa102-82.qa.lab:5181,qa102-83.qa.lab:5181",
                impersonation: {
                  enabled: true,
                  max_chained_user_hops: 3
                },
                security: {          
                        auth.mechanisms : ["PLAIN"],
                         },
                security.user.auth: {
                        enabled: true,
                        packages += "org.apache.drill.exec.rpc.user.security",
                        impl: "pam",
                        pam_profiles: [ "sudo", "login" ]
                 }
               }

1. (Optional) To add or remove different PAM profiles, add or delete the profile names in the “pam_profiles” array shown above. 

1. Restart the Drillbit process on each Drill node. 

    `<DRILLINSTALL_HOME>/bin/drillbit.sh restart`






