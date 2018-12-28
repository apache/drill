---
title: "Configuring Custom ACLs to Secure znodes"
date: 2018-12-28
parent: "Securing Drill"
---  

Drill uses the Curator Framework to interact with ZooKeeper to discover services in a cluster. In addition to discovering services, Drill uses ZooKeeper to store certain cluster-level configuration and query profile information in znodes. A znode is an internal data tree in ZooKeeper that stores coordination- and execution-related information. If information in the znodes is not properly secured, cluster privacy and/or security is compromised.   

Drill allows users to create a custom ACL (access control list) on the znodes to secure data. ACLs specify sets of ids and permissions that are associated with the ids. ZooKeeper uses ACLs to control access to znodes and secure the information they store.   

Prior to Drill 1.15, ZooKeeper ACLs in secure and unsecure clusters were set to [world:all], meaning that all users had create, delete, read, write, and administrator access to the zknodes. Starting in Drill 1.15, ACLs in unsecure clusters are set to [world:all]. ACLs in secure clusters are set to [authid: all], which provides only the authenticated user that created the znode with full access. Discovery znodes (znodes with the list of Drillbits) have an additional ACL set to [world:read] making the list of Drillbits readable by any user.   

- View the [drill-override-example.conf](https://github.com/apache/drill/blob/master/distribution/src/resources/drill-override-example.conf) file to see example ACL configurations.

  
##Securing znodes
Complete the following steps to create a custom ACL and secure znodes:  

1. Write a class that implements the `ZKACLProvider` interface. This class will contain the ACLs that need to be set on the znodes. You can use  the [`ZKSecureACLProvider` class as a sample reference](https://github.com/apache/drill/blob/master/exec/java-exec/src/main/java/org/apache/drill/exec/coord/zk/ZKSecureACLProvider.java).  
2. Add the following dependency to the `pom` file of the project module created:  

		<groupId>org.apache.drill.exec</groupId>
		<artifactId>drill-java-exec</artifactId>  
3. Refer to the steps listed at [https://drill.apache.org/docs/manually-adding-custom-functions-to-drill/]({{site.baseurl}}/docs/manually-adding-custom-functions-to-drill/) to create a JAR and then add the JAR to Drill's classpath.  
4. In `$DRILL_HOME/conf/drill-override.conf`, set `zk.acl_provider` to the `ZKACLProviderTemplate` type.  
5. Restart Drill.
  
When you restart Drill, the ACL, as mentioned in your custom class, is applied to the znode created when Drill starts.  

***
**NOTE**  
Existing ACLs for persistent znodes will not be affected if a Drillbit is restarted with a different ACL setting. ACLs are applied only at znode creation time, and Drill does not recreate any znode that is already present. If you want to change an ACL for existing znodes, connect to the ZooKeeper server using zkCli and then use option a or b, as described:  

- a) Shutdown Drillbits, delete the persistent znodes, change the ACL settings and then restart the Drillbit   

- b) Manually change the ACLs on the existing znodes to reflect the new ACL settings, using the setAcl command in the zkCli.

For either option to work, an authenticated connection between the zkCli and ZooKeeper Server must be established. 

***

For additional information, refer to:  

- [https://zookeeper.apache.org/doc/trunk/zookeeperOver.html](https://zookeeper.apache.org/doc/trunk/zookeeperOver.html)  
- [https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_ZooKeeperAccessControl](https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)  
- [https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL)


