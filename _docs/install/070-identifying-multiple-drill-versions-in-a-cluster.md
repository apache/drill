---
title: "Identifying Multiple Drill Versions in a Cluster"
date: 2018-02-08 00:38:57 UTC
parent: Install Drill
---

As of Drill 1.10, the Web Console displays the Drill version running on each Drill node in the cluster, as shown in the following image:  

![](http://i.imgur.com/42otmKQ.jpg)  

You can also retrieve the version information by running the following query:  

       SELECT * FROM sys.drillbits;  

If the version of Drill differs between nodes, a warning message appears. The nodes running the current version have a green label, while the nodes running another version have a red label, as shown in the image above.  
 
The Drill node from which you access the Web Console defines the current version. For example, assume you have two Drill nodes in a cluster with the following IP addresses, versions, and Web Console access:  

| Drill   Node | Drill Version | Web Console               |
|--------------|---------------|---------------------------|
| 10.10.123.88 | 1.9.0         | http:// 10.10.123.88:8047 |
| 10.10.136.25 | 1.10.0        | http://10.10.136.25:8047  |  

Accessing the Web Console for Drill node 10.10.123.88 displays Drill version 1.9.0 as the current version with a green label, while also displaying the Drill version for Drill node 10.10.136.25, but with a red label. Accessing the Web Console for Drill node 10.10.136.25 displays 1.10.0 as the current version with a green label, while also displaying the Drill version for Drill node 10.10.123.88, but with a red label. In both cases, the Web Console generates a warning to state that the Drill versions do not match.  

The Web Console sorts the Drill nodes by version, starting with the current Drill node, followed by Drill nodes with Drill versions that match the current version, followed by Drill nodes that do not match the current version. Drill nodes marked as having an “undefined” version may be incorrectly defined or have a pre-1.10.0 version of Drill installed. 
