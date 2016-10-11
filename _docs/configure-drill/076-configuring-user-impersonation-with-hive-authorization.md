---
title: "Configuring User Impersonation with Hive Authorization"
date: 2016-01-05
parent: "Configure Drill"
---
As of Drill 1.1, you can enable impersonation in Drill and configure authorization in Hive version 1.0 to authorize access to metadata in the Hive metastore repository and data in the Hive warehouse. Impersonation allows a service to act on behalf of a client while performing the action requested by the client. See [Configuring User Impersonation]({{site.baseurl}}/docs/configuring-user-impersonation).

There are two types of Hive authorizations that you can configure to work with impersonation in Drill: SQL standard based and storage based authorization.  

## SQL Standard Based Authorization  

You can configure Hive SQL standard based authorization in Hive version 1.0 to work with impersonation in Drill 1.1. The SQL standard based authorization model can control which users have access to columns, rows, and views. Users with the appropriate permissions can issue the GRANT and REVOKE statements to manage privileges from Hive.

For more information, see [SQL Standard Based Hive Authorization](https://cwiki.apache.org/confluence/display/HIVE/SQL+Standard+Based+Hive+Authorization).  

## Storage Based Authorization  
  
You can configure Hive storage based authorization in Hive version 1.0 to work with impersonation in Drill 1.1. Hive storage based authorization is a remote metastore server security feature that uses the underlying file system permissions to determine permissions on databases, tables, and partitions. The unit style read/write permissions or ACLs that a user or group has on directories in the file system determine access to data. Because the file system controls access at the directory and file level, storage based authorization cannot control access to data at the column or view level.

You manage user and group privileges through permissions and ACLs in the distributed file system. You manage storage based authorization through the remote metastore server to authorize access to data and metadata.

DDL statements that manage permissions, such as GRANT and REVOKE, do not affect permissions in the storage based authorization model.

For more information, see [Storage Based Authorization in the Metastore Server](https://cwiki.apache.org/confluence/display/Hive/Storage+Based+Authorization+in+the+Metastore+Server).  

## Configuration  

Once you determine the Hive authorization model that you want to implement, enable impersonation in Drill, update the `hive-site.xml` file with the relevant parameters for the authorization type, and modify the Hive storage plugin configuration in Drill with the relevant properties for the authorization type.  

### Prerequisites  

* Hive 1.0 installed
* Drill 1.1 or later installed
* Hive remote metastore repository configured  

## Step 1: Enabling Drill Impersonation  

Modify `<DRILL_HOME>/conf/drill-override.conf` on each Drill node to include the required properties, set the [maximum number of chained user hops]({{site.baseurl}}/docs/configuring-user-impersonation/#chained-impersonation), and restart the Drillbit process.

1. Add the following properties to the `drill.exec` block in `drill-override.conf`:  

          drill.exec: {
           cluster-id: "<drill_cluster_name>",
           zk.connect: "<hostname>:<port>,<hostname>:<port>,<hostname>:<port>"
           impersonation: {
                 enabled: true,
                 max_chained_user_hops: 3
            }
           }  

2. Issue the following command to restart the Drillbit process on each Drill node:  
`<DRILLINSTALL_HOME>/bin/drillbit.sh restart`  

##  Step 2:  Updating hive-site.xml  

Update hive-site.xml with the parameters specific to the type of authorization that you are configuring and then restart Hive.  

### Storage Based Authorization  

Add the following required authorization parameters in hive-site.xml to configure storage based authentication:  

**hive.metastore.pre.event.listeners**  
**Description:** Enables metastore security.  
**Value:** org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener  

**hive.security.metastore.authorization.manager**  
**Description:** Tells Hive which metastore-side authorization provider to use. The default setting uses DefaultHiveMetastoreAuthorizationProvider, which implements the standard Hive grant/revoke model. To use an HDFS permission-based model (recommended) for authorization, use StorageBasedAuthorizationProvider.  
**Value:** org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider  

**hive.security.metastore.authenticator.manager**  
**Description:** The authenticator manager class name in the metastore for authentication.  
**Value:** org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator  

**hive.security.metastore.authorization.auth.reads**  
**Description:** When enabled, Hive metastore authorization checks for read access.  
**Value:** true  

**hive.metastore.execute.setugi**  
**Description:** When enabled, this property causes the metastore to execute DFS operations using the client's reported user and group permissions. This property must be set on both the client and server sides. If the cient and server settings differ, the client setting is ignored.  
**Value:** true 

**hive.server2.enable.doAs**  
**Description:** Tells HiveServer2 to execute Hive operations as the user submitting the query. Must be set to true for the storage based model.  
**Value:** true



### Example of hive-site.xml configuration with the required properties for storage based authorization 

       <configuration>
         <property>
           <name>hive.metastore.uris</name>
           <value>thrift://10.10.100.120:9083</value>    
         </property>  
       
         <property>
           <name>javax.jdo.option.ConnectionURL</name>
           <value>jdbc:derby:;databaseName=/opt/hive/hive-1.0/bin/metastore_db;create=true</value>    
         </property>
       
         <property>
           <name>javax.jdo.option.ConnectionDriverName</name>
           <value>org.apache.derby.jdbc.EmbeddedDriver</value>    
         </property>
       
         <property>
           <name>hive.metastore.pre.event.listeners</name>
           <value>org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener</value>
         </property>
       
         <property>
           <name>hive.security.metastore.authenticator.manager</name>
           <value>org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator</value>
         </property>
       
         <property>
           <name>hive.security.metastore.authorization.manager</name>
           <value>org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider</value>
         </property>
       
         <property>
           <name>hive.security.metastore.authorization.auth.reads</name>
           <value>true</value>
         </property>
       
         <property>
           <name>hive.metastore.execute.setugi</name>
           <value>true</value>
         </property>
       
         <property>
           <name>hive.server2.enable.doAs</name>
           <value>true</value>
         </property>
       </configuration>


## SQL Standard Based Authorization  

Add the following required authorization parameters in hive-site.xml to configure SQL standard based authentication:  

**hive.security.authorization.enabled**  
**Description:** Enables Hive security authorization.   
**Value:** true 

**hive.security.authenticator.manager**  
**Description:** Class that implements HiveAuthenticationProvider to provide the client’s username and groups.  
**Value:** org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator  

**hive.security.authorization.manager**  
**Description:** The Hive client authorization manager class name.   
**Value:** org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory  

**hive.server2.enable.doAs**  
**Description:** Tells HiveServer2 to execute Hive operations as the user submitting the query. Must be set to false for the SQL standard based model. 
**Value:** false

**hive.users.in.admin.role**  
**Description:** A comma separated list of users which gets added to the ADMIN role when the metastore starts up. You can add more uses at any time. Note that a user who belongs to the admin role needs to run the "set role" command before getting the privileges of the admin role, as this role is not in the current roles by default.  
**Value:** Set to the list of comma-separated users who need to be added to the admin role. 

**hive.metastore.execute.setugi**  
**Description:** In unsecure mode, setting this property to true causes the metastore to execute DFS operations using the client's reported user and group permissions. Note: This property must be set on both the client and server sides. This is a best effort property. If the client is set to true and the server is set to false, the client setting is ignored.  
**Value:** false  
  

### Example of hive-site.xml configuration with the required properties for SQL standard based authorization         
        
       <configuration>
         <property>
           <name>hive.metastore.uris</name>
           <value>thrift://10.10.100.120:9083</value>    
         </property> 

         <property>
           <name>javax.jdo.option.ConnectionURL</name>
           <value>jdbc:derby:;databaseName=/opt/hive/hive-1.0/bin/metastore_db;create=true</value>    
         </property>
       
         <property>
           <name>javax.jdo.option.ConnectionDriverName</name>
           <value>org.apache.derby.jdbc.EmbeddedDriver</value>    
         </property>  

         <property>
           <name>hive.security.authorization.enabled</name>
           <value>true</value>
         </property>
       
         <property>
           <name>hive.security.authenticator.manager</name>
           <value>org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator</value>
         </property>       
       
         <property>
           <name>hive.security.authorization.manager</name>   
           <value>org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory</value>
         </property>
       
         <property>
           <name>hive.server2.enable.doAs</name>
           <value>false</value>
         </property>
       
         <property>
           <name>hive.users.in.admin.role</name>
           <value>user</value>
         </property>
       
         <property>
           <name>hive.metastore.execute.setugi</name>
           <value>false</value>
         </property>    
        </configuration>

## Step 3: Modifying the Hive Storage Plugin  

Modify the Hive storage plugin configuration in the Drill Web Console to include specific authorization settings. The Drillbit that you use to access the Web Console must be running.  

Complete the following steps to modify the Hive storage plugin:  

1.  Navigate to `http://<drillbit_hostname>:8047`, and select the **Storage tab**.  
2.  Click **Update** next to "hive."  
3.  In the configuration window, add the configuration properties for the authorization type.
  
   * For storage based authorization, add the following properties:  

              {
               type:"hive",
               enabled: true,
               configProps : {
                 "hive.metastore.uris" : "thrift://<metastore_host>:<port>",
                 "fs.default.name" : "hdfs://<host>:<port>/",
                 "hive.metastore.sasl.enabled" : "false",
                 "hive.server2.enable.doAs" : "true",
                 "hive.metastore.execute.setugi" : "true"
               }
              }  
   * For SQL standard based authorization, add the following properties:  

              {
               type:"hive",
               enabled: true,
               configProps : {
                 "hive.metastore.uris" : "thrift://<metastore_host>:9083",
                 "fs.default.name" : "hdfs://<host>:<port>/",
                 "hive.security.authorization.enabled" : "true",
                 "hive.security.authenticator.manager" : "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator",
                 "hive.security.authorization.manager" : "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory",
                 "hive.metastore.sasl.enabled" : "false",
                 "hive.server2.enable.doAs" : "false",
                 "hive.metastore.execute.setugi" : "false"
               }
              }
              













