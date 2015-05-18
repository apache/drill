---
title: "Core Modules"
parent: "Architecture"
---
The following image represents components within each Drillbit:

![drill query flow]({{ site.baseurl }}/docs/img/DrillbitModules.png)

The following list describes the key components of a Drillbit:

  * **RPC end point**: Drill exposes a low overhead protobuf-based RPC protocol to communicate with the clients. Additionally, a C++ and Java API layers are also available for the client applications to interact with Drill. Clients can communicate to a specific Drillbit directly or go through a ZooKeeper quorum to discover the available Drillbits before submitting queries. It is recommended that the clients always go through ZooKeeper to shield clients from the intricacies of cluster management, such as the addition or removal of nodes. 

  * **SQL parser**: Drill uses Optiq, the open source framework, to parse incoming queries. The output of the parser component is a language agnostic, computer-friendly logical plan that represents the query. 
  * **Storage plugin interfaces**: Drill serves as a query layer on top of several data sources. Storage plugins in Drill represent the abstractions that Drill uses to interact with the data sources. Storage plugins provide Drill with the following information:
    * Metadata available in the source
    * Interfaces for Drill to read from and write to data sources
    * Location of data and a set of optimization rules to help with efficient and faster execution of Drill queries on a specific data source 

    In the context of Hadoop, Drill provides storage plugins for files and
HBase/M7. Drill also integrates with Hive as a storage plugin since Hive
provides a metadata abstraction layer on top of files, HBase/M7, and provides
libraries to read data and operate on these sources (Serdes and UDFs).

    When users query files and HBase/M7 with Drill, they can do it directly or go
through Hive if they have metadata defined there. Drill integration with Hive
is only for metadata. Drill does not invoke the Hive execution engine for any
requests.

  * **Distributed cache**: Drill uses a distributed cache to manage metadata (not the data) and configuration information across various nodes. Sample metadata information that is stored in the cache includes query plan fragments, intermediate state of the query execution, and statistics. Drill uses Infinispan as its cache technology.
  * 
  Please check if Drill still uses distributed caches. ASAIK, currently Drill neither uses Hazelcast of Infinispan as distributed caches. See https://issues.apache.org/jira/browse/DRILL-1436#
  If that's the case, plese distributed cache from the aarchitecture diagram. 
