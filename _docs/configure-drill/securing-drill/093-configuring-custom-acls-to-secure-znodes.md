---
title: "Configuring Custom ACLs to Secure znodes"
date: 2018-12-08
parent: "Securing Drill"
---  

Drill uses ZooKeeper for dynamic service discovery in a cluster; ZooKeeper uses the Curator framework and Service Discovery recipe to discover services. In addition to discovering services, Drill uses ZooKeeper to store certain cluster-level configuration and query profile information in znodes. A znode is an internal data tree in ZooKeeper that stores coordination- and execution-related information. Each time a Drillbit starts up and establish a new session with Zookeeper (using the Curator framework), a new znode is created. If information in the znodes is not properly secured, cluster privacy and/or security is compromised. You can create a custom ACL on the znodes to secure data.

ZooKeeper uses ACLs (access control lists) to control access to znodes and secure the information they store. ACLs specify sets of ids and permissions that are associated with the ids. The ZooKeeper ACLs are set such that only the Drillbit process user can access (create, delete, read, write, administer) all the ZooKeeper nodes in a Drill cluster, except for the service discovery znodes. When a Drillbit shuts-down, the ZooKeeper session ends and the znode is removed.  

Prior to Drill 1.15, ZooKeeper ACLs in secure and unsecure clusters were set to [world:all], meaning that all users had create, delete, read, write, and administrator access to the zknodes. Starting in Drill 1.15, ACLs in unsecure clusters are set to [world:all]. ACLs in secure clusters are set to [authid: all], which provides only the authenticated user that created the znode with full access. Discovery znodes (znodes with the list of Drillbits) have an additional ACL set to [world:read] making the list of Drillbits readable by any user.

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

For additional information, refer to:  

- [https://zookeeper.apache.org/doc/trunk/zookeeperOver.html](https://zookeeper.apache.org/doc/trunk/zookeeperOver.html)  
- [https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_ZooKeeperAccessControl](https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)  
- [https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL)


