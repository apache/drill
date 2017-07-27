---
title: "Configuring Inbound Impersonation"
date: 2017-07-27 02:19:28 UTC
parent: "Securing Drill"
---  

Drill supports [user impersonation]({{site.baseurl}}/docs/configuring-user-impersonation/)  where queries run as the user that created a connection. However, this user is not necessarily the end user who submits the queries. For example, in a classic three-tier architecture, the end user interacts with Tableau Desktop, which communicates with a Tableau Server, which in turn communicates with a Drill cluster. In this scenario, a proxy user creates a connection, and the queries are submitted to Drill by the proxy user on behalf of the end user, and not by the end user directly. In this particular case, the query needs to be run as the end user.  

As of Drill 1.6, an administrator can define inbound impersonation policies to impersonate the end user. The proxy user needs to be authorized to submit queries on behalf of the specified end user. Otherwise, any user can impersonate another user. Then, the query runs as the end user, and data authorization is based on this user’s access permissions. Note that without [authentication]({{site.baseurl}}/docs/configuring-user-authentication/) enabled in both communication channels, a user can impersonate any other user.

Drill trusts proxy users to provide the correct end user identity information. Drill does not authenticate the end user. The proxy user (application) is responsible for end user authentication, which is usually enabled.

![]({{ site.baseurl }}/docs/img/inboundImpersonation.PNG)  

This image shows how identity is propagated through various layers (with authentication enabled). The flow on the left is Drill with user impersonation enabled, and the flow on the right is Drill with user impersonation and inbound impersonation enabled. `t:euser` is a property on the connection (`u` is `username`, `p`is `password`, `t` is `impersonation_target`).  


##Configuring Inbound Impersonation
You must be an administrator to configure inbound impersonation. You can define administrative users through the `security.admin.user_groups` and `security.admin.users` options. See [Configuration Options]({{site.baseurl}}/docs/configuration-options-introduction/#system-options). 

[User impersonation]({{site.baseurl}}/docs/configuring-user-impersonation/) must be enabled before you can configure inbound impersonation.

Complete the following steps to enable inbound impersonation:  

1. In `drill-override.conf`, set user impersonation to true:
  
              {
              drill.exec.impersonation.enabled: true,
              ...
              }

2. Define inbound impersonation policies. For example, the following ALTER SYSTEM statement authorizes:
       * `puser1` to impersonate any user (use * as a wildcard character)
       * `puser2` to impersonate `euser1` and all users in `egroup2` 
       * all users in `pgroup3` to impersonate all users in `egroup3`  
      
              ALTER SYSTEM SET `exec.impersonation.inbound_policies`=‘[
              { proxy_principals : { users: [“puser1”] },
                target_principals: { users: [“*”] } },
              { proxy_principals : { users: [“puser2”] }, 
                target_principals: { users: [“euser1”], groups :  [“egroup2”] } },
              { proxy_principals : { groups: [“pgroup3”] },
                target_principals: { groups: [“egroup3”] } } ]’;  
Policy format:

              { proxy_principals : { users : [“...”, “...”], groups : [“...”, “...”] },
              target_principals: { users : [“...”, “...”], groups : [“...”, “...”] } }

3. Ensure that the proxy user (application) passes the username of the impersonation target user to Drill when creating a connection through the `impersonation_target` connection property. The following examples show you how to do this for JDBC and ODBC:  
 
       
       *  For JDBC, through SQLLine:

               bin/sqlline –u “jdbc:drill:schema=dfs;zk=myclusterzk;impersonation_target=euser1” -n puser1 -p ppass1    
In this example, `puser1` is the user submitting the queries. This user is authenticated. Since this user is authorized to impersonate any user, queries through the established connection are run as `euser1`.

           

       * For ODBC on Linux or Mac, you can pass the username through the `DelegationUID` property in the odbc.ini file. See [Configuring ODBC on Linux]({{site.baseurl}}/docs/configuring-odbc-on-linux/) for more information.  
       
              DelegationUID=euser1  
If you are using ODBC on Windows, you can use the **ODBC Data Source Administrator** to provide the username through the `Delegation UID` field in the MapR Drill ODBC Driver DSN Setup dialog box. See [Configuring ODBC on Windows]({{site.baseurl}}/docs/configuring-odbc-on-windows/) for more information.
