---
title: "Review the Java Stack Trace"
date: 2018-11-02
parent: "Log and Debug"
---

If a query is failing, you can use jstack to print the Java thread stack traces for the Drillbit process on the executing nodes to determine which part of the query is causing the failure. Drill labels threads based on query coordinates. For example, in the following thread you can see the QueryID, MajorFragementID, and MinorFragmentID, respectively:

       "2ae226e1-f4c5-6253-e9ff-22ac1204935f:frag:3:2" daemon prio=10 tid=0x00007fa81167b800 nid=0x1431 waiting on condition [0x00007fa7aa3b3000]

When you use jstack, grep for frag to see the fragments executing the query. To see the stack trace for a Drillbit process, you must have the Drillbit process ID. You can run jps to get the ID. 

## Use jstack to Review Stack Traces
The following example shows you how to run jps to get the Drillbit process ID and grep for frag in the stack trace:

       [root@drillats2 ~]# jps
       17455 jps
       1975 Drillbit
       31801 HRegionServer
       28858 TaskTracker
       22603 WardenMain

In this example, the Drillbit process ID is 1975. Alternatively, you can grep for the Java processes running to identify the Drillbit process ID:

       [root@drillats2 ~]# ps -el|grep java

Once you have the Drillbit process ID, you can use jstack to view the stack trace of threads.
       
       [root@drillats2 ~]# jstack 1975 | grep frag:
       "2ae22585-bb7b-b482-0542-4ebd5225a249:frag:0:0" daemon prio=10 tid=0x00007fa782796800 nid=0x37a2 runnable [0x00007fa7866e4000]
       "2ae225ab-c37a-31dc-227e-5f165e057f5f:frag:0:0" daemon prio=10 tid=0x00007fa7a004f000 nid=0x2946 runnable [0x00007fa7868e6000]
       "2ae22598-2a77-5361-363b-291301fc44e8:frag:0:0" daemon prio=10 tid=0x00007fa782280800 nid=0x1bd4 runnable [0x00007fa7a4293000]
       "2ae22582-c517-ec7a-8438-c81b7f5fdbce:frag:0:0" daemon prio=10 tid=0x0000000004651800 nid=0x144f runnable [0x00007fa7a53a4000]
       "2ae226e1-8a2d-c079-a1d6-1f27c5a003bb:frag:13:2" daemon prio=10 tid=0x00007fa811ec3000 nid=0x143d waiting on condition [0x00007fa7a4799000]
       "2ae226e1-8a2d-c079-a1d6-1f27c5a003bb:frag:9:2" daemon prio=10 tid=0x0000000004583800 nid=0x143c waiting on condition [0x00007fa7a4596000]
       "2ae226e1-8a2d-c079-a1d6-1f27c5a003bb:frag:15:2" daemon prio=10 tid=0x00007fa81323f800 nid=0x1439 waiting on condition [0x00007fa7a4fa0000]
       "2ae226e1-8a2d-c079-a1d6-1f27c5a003bb:frag:15:5" daemon prio=10 tid=0x0000000004582800 nid=0x1438 waiting on condition [0x00007fa7a4e9f000]
       "2ae226e1-8a2d-c079-a1d6-1f27c5a003bb:frag:17:2" daemon prio=10 tid=0x00007fa813240800 nid=0x1435 waiting on condition [0x00007fa7a4496000]
       "2ae226e1-f4c5-6253-e9ff-22ac1204935f:frag:3:2" daemon prio=10 tid=0x00007fa81167b800 nid=0x1431 waiting on condition [0x00007fa7aa3b3000]
       "2ae226e1-f4c5-6253-e9ff-22ac1204935f:frag:2:2" daemon prio=10 tid=0x0000000004582000 nid=0x1430 waiting on condition [0x00007fa7a7bae000]
       …
       
