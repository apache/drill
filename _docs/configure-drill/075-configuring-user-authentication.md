---
title: "Configuring User Authentication"
date: 2016-03-02 00:30:47 UTC
parent: "Configure Drill"
---
Authentication is the process of proving a user’s identity to access a process running on a system. Drill currently supports username/password based authentication through the use of the Linux Pluggable Authentication Module (PAM). The authentication option is available through JDBC and ODBC interfaces. Linux PAM provides authentication modules that interface with any installed PAM authentication entity, such as the local operating system password file (passwd or login) or LDAP. 
 
If user impersonation is enabled, Drill executes the client requests as the authenticated user. Otherwise, Drill executes client requests as the user that started the Drillbit process. You can enable both authorization and impersonation to improve Drill security. See [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation/).

When using PAM for authentication, each user that has permission to run Drill queries must exist in the list of users that resides on each Drill node in the cluster. The username (including uid) and password for each user must be identical across all of the Drill nodes. 

If you use PAM with /etc/passwd for authentication, verify that the users with permission to start the Drill process are part of the shadow user group on all nodes in the cluster. This enables Drill to read the /etc/shadow file for authentication.  

## Administrator Privileges

When authentication is enabled, only Drill users who are assigned Drill cluster administrator privileges can perform the following tasks:

* Change a system-level option by issuing an ALTER SYSTEM command
* Update a storage plugin configuration through the REST API or Web Console
* View profiles of all queries that all users have run or are currently running in a cluster
* Cancel running queries that were launched by any user in the cluster

## User Authentication Process

When user authentication is enabled, each user that accesses the Drillbit process through a client, such as SQLLine, must provide their username and password for access. 

A user can include the `–n` and `–p` parameters with their username and password when launching SQLLine, as shown in the following example:  
       `sqlline –u jdbc:drill:zk=10.10.11.112:5181 –n bob –p bobdrill`

Alternatively, a user can launch SQLLine and then issue the `!connect` command to hide the password, as shown in the following procedure:  

1. Start SQLLine, by running the sqlline script. On Linux, for example:  

              bobsmachine:~$ /etc/drill/bin/sqlline
              apache drill 1.2.0
              "a drill in the hand is better than two in the bush"  

2. At the sqlline prompt, enter the `!connect` command followed by `jdbc:drill:zk=zk=<zk name>[:<port>][,<zk name2>[:<port>]... ]`. For example:
3. 
              sqlline> !connect jdbc:drill:zk=localhost:2181
              scan complete in 1385ms
3. At the prompts, enter a user name and password.
4. 
              Enter username for jdbc:drill:zk=localhost:2181: bob
              Enter password for jdbc:drill:zk=localhost:2181: *************
       
       The password is hidden as it is typed.

  
When a user connects to Drill from a BI tool, such as Tableau, the MapR Drill ODBC driver prompts the user for their username and password:

![ODBC Driver]({{site.baseurl}}/docs/img/UserAuth_ODBC_Driver.png)

The client passes the username and password to a Drillbit as part of the connection request, which then passes the credentials to PAM. If PAM can verify that the user is authorized to access Drill, the connection is successful, and the user can issues queries against the file system or other storage plugins, such as Hive or HBase. However, if PAM cannot verify that the user is authorized to access Drill, the connection is terminated as AUTH_FAILED.
 
The following image illustrates the user authentication process in Drill:

![]({{site.baseurl}}/docs/img/UserAuthProcess.PNG)

### Installing and Configuring PAM

Install and configure the provided Drill PAM. Drill only supports the PAM provided here. Optionally, you can [build and implement a custom authenticator]({{ site.baseurl }}/docs/configuring-user-authentication/#implementing-and-configuring-a-custom-authenticator). 

{% include startnote.html %}Do not point to an existing directory where other Hadoop components are installed. Other file system libraries can conflict with the Drill libraries and cause system errors.{% include endnote.html %}
 
Complete the following steps to install and configure PAM for Drill:

1. Download the `tar.gz` file for the Linux platform:  
   [http://sourceforge.net/projects/jpam/files/jpam/jpam-1.1/](http://sourceforge.net/projects/jpam/files/jpam/jpam-1.1/)
2. Untar the file, and copy the `libjpam.so` file into a directory that does not contain other Hadoop components.  
   Example:` /opt/pam/`
3. Add the following line to `<DRILL_HOME>/conf/drill-env.sh`, including the directory where the `libjpam.so` file is located:  
  
     `export DRILLBIT_JAVA_OPTS="-Djava.library.path=<directory>"`  

      Example: `export DRILLBIT_JAVA_OPTS="-Djava.library.path=/opt/pam/"`  

4. Add the following configuration to the `drill.exec` block in `<DRILL_HOME>/conf/drill-override.conf`:  

          drill.exec {
           security.user.auth {
                 enabled: true,
                 packages += "org.apache.drill.exec.rpc.user.security",
                 impl: "pam",
                 pam_profiles: [ "sudo", "login" ]
           } 
          }

5. (Optional) To add or remove different PAM profiles, add or delete the profile names in the `“pam_profiles”` array shown above.  
6. Restart the Drillbit process on each Drill node. 
 
        <DRILLINSTALL_HOME>/bin/drillbit.sh restart

### Implementing and Configuring a Custom Authenticator

Administrators can use the template provided here to develop and implement a custom username/password based authenticator.

Complete the following steps to build and implement a custom authenticator:

1. Build the following Java file into a JAR file: 
 
           MyCustomDrillUserAuthenticatorImpl.java 
           
           package myorg.dept.drill.security;
           
           import org.apache.drill.common.config.DrillConfig;
           import org.apache.drill.exec.exception.DrillbitStartupException;
           
           import java.io.IOException;
           
           /*
           * Implement {@link org.apache.drill.exec.rpc.user.security.UserAuthenticator} for illustraing how to develop a custom authenticator and use it in Drill
           */
           @UserAuthenticatorTemplate(type = “myCustomAuthenticatorType”)
           public class MyCustomDrillUserAuthenticatorImpl implements UserAuthenticator {
           
            public static final String TEST_USER_1 = "testUser1";
            public static final String TEST_USER_2 = "testUser2";
            public static final String TEST_USER_1_PASSWORD = "testUser1Password";
            public static final String TEST_USER_2_PASSWORD = "testUser2Password";
           
           /**
           * Setup for authenticating user credentials.
           */
            @Override
            public void setup(DrillConfig drillConfig) throws DrillbitStartupException {
              // If the authenticator has any setup such as making sure authenticator provider servers are up and running or 
              // needed libraries are available, it should be added here.
            }
           
           /**
           * Authenticate the given <i>user</i> and <i>password</i> combination.
           *
           * @param userName
           * @param password
           * @throws UserAuthenticationException if authentication fails for given user and password.
           */
            @Override
            public void authenticate(String userName, String password) throws UserAuthenticationException {
           
              if (!(TEST_USER_1.equals(user) && TEST_USER_1_PASSWORD.equals(password)) &&
              !(TEST_USER_2.equals(user) && TEST_USER_2_PASSWORD.equals(password))) {
            throw new UserAuthenticationException(“custom failure message if the admin wants to show it to user”);
              }
            }
           
           /**
           * Close the authenticator. Used to release resources. Ex. LDAP authenticator opens connections to LDAP server,
           * such connections resources are released in a safe manner as part of close.
           *
           * @throws IOException
           */
            @Override
            public void close() throws IOException {
              // Any clean up such as releasing files/network resources should be done here
            }
           }  


2. Create a file named `drill-module.conf` with the following configuration code and then add this file to the root of the JAR file: 
         
              drill {
                classpath.scanning {
                  packages += "myorg.dept.drill.security"
                }
              }  
This enables the custom classpath scanner to locate the new class. 
3. Add the JAR file that you built to the following directory on each Drill node:  
   ` <DRILLINSTALL_HOME>/jars`
3. Add the following configuration to the `drill.exec` block in the `drill-override.conf` file located in `<DRILLINSTALL_HOME>/conf/`:  

              drill.exec {
               security.user.auth {
                	enabled: true,
                	packages += "myorg.dept.drill.security",
                	impl: "myCustomAuthenticatorType"
               }
              }  
4. Restart the Drillbit process on each Drill node.
 
        <DRILLINSTALL_HOME>/bin/drillbit.sh restart
       











