---
title: "Creating Custom Authenticators"
date: 2017-06-22 20:42:14 UTC
parent: "Adding Custom Functions to Drill"
---


Administrators can use the template provided here to develop and implement a custom username/password based authenticator.

Complete the following steps to build and implement a custom authenticator:

1. Build the following Java file into a JAR file: 
 
           MyCustomDrillUserAuthenticatorImpl.java 
           
           package myorg.dept.drill.security;     
           
           import org.apache.drill.common.config.DrillConfig;
           import org.apache.drill.exec.exception.DrillbitStartupException;
           import org.apache.drill.exec.rpc.user.security.UserAuthenticator;
           import org.apache.drill.exec.rpc.user.security.UserAuthenticationException;
           import org.apache.drill.exec.rpc.user.security.UserAuthenticatorTemplate;
           
           import java.io.IOException;
           
           /*
           * Implement {@link org.apache.drill.exec.rpc.user.security.UserAuthenticator} for illustrating how to develop a custom authenticator and use it in Drill
           */
           @UserAuthenticatorTemplate(type = "myCustomAuthenticatorType")
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
           
              if (!(TEST_USER_1.equals(userName) && TEST_USER_1_PASSWORD.equals(password)) &&
              !(TEST_USER_2.equals(userName) && TEST_USER_2_PASSWORD.equals(password))) {
            throw new UserAuthenticationException("custom failure message if the admin wants to show it to user");
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
       











