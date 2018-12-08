---
title: "Error Messages"
date: 2018-12-08
parent: "Log and Debug"
---

Drill produces several types of error messages. You can ignore issues that contain any of the following syntax:  

   * InterruptedException
   * ChannelClosedException
   * Connection reset by peer

These issues typically result from a problem outside of the query process. However, if you encounter a java.lang.OutOfMemoryError error, take action and give Drill as much memory as possible to resolve the issue. See [Configuring Drill Memory]({{ site.baseurl }}/docs/configuring-drill-memory/).

Drill assigns an ErrorId to each error that occurs. An ErrorID is a unique identifier for a particular error that tells you which node assigned the error. For example,
[ 1ee8e004-2fce-420f-9790-5c6f8b7cad46 on 10.1.1.109:31010 ]. You can log into the node that assigned the error and grep the Drill log for the ErrorId to get more information about the error.

Thread names in the Drill logs also provide important information, including the query and fragment IDs. If a query fails, you can view the Java stack trace for the executing nodes to determine which part of the query is failing. An example thread, [42d7545c-c89b-47ab-9e38-2787bd200d4e:frag:1:173] includes QueryID 42d7545c-c89b-47ab-9e38-2787bd200d4e, MajorFragmentID 1, and MinorFragmentID 173.

The following table provides descriptions for the IDs included in a thread:

| ID Type         | Description                                                                                                                                                                                                                                  |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| QueryID         | The identifier assigned to the query. You can locate a query in Drill Web UI by the QueryID and then cancel the query if needed. See [Query Profiles]({{ site.baseurl }}/docs/query-profiles/).                                                                    |
| MajorFragmentID | The identifier assigned to a major fragment. Major fragments map to the physical plan. You can see major fragment activity for a query in the Drill Web UI. See [Query Profiles]({{ site.baseurl }}/docs/query-profiles) for more information. |
| MinorFragmentID | The identifier assigned to the minor fragment. Minor fragments map to the parallelization of major fragments. See [Query Profiles]({{ site.baseurl }}/docs/query-profiles).                                                                                       |
