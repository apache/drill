---
title: "Configure Drill Introduction"
date: 2016-11-21 22:42:09 UTC
parent: "Configure Drill"
---

This section briefly describes the following key Drill configuration tasks and provides links to configuration procedures:

* Memory Configuration
* Multitenancy Configuration
* Performance and Functionality Configuration
* Query Profile Data Storage Configuration 

## Memory Configuration

When using Drill, you need to make sufficient memory available Drill when running Drill alone or along side other workloads on the cluster. The next section, ["Configuring Drill Memory"]({{site.baseurl}}/docs/configuring-drill-memory) describes how to configure memory for a Drill cluster. 

## Multitenancy Configuration

You can configure resources for [multitenancy clusters]({{site.baseurl}}/docs/configuring-multitenant-resources) or for [sharing a Drillbit]({{site.baseurl}}/docs/configuring-a-shared-drillbit) on a cluster.

## Performance and Functionality Configuration

You can also modify options for performance or functionality. For example, changing the default storage format is a typical functional change. The default storage format for CTAS
statements is Parquet. Using a configuration option, you can modify Drill to store the output data in CSV or JSON format. The section, ["Configuration Options Introduction"]({{site.baseurl}}/docs/configuration-options-introduction) summarizes the many options you can configure. 

## Query Profile Data Storage Configuration

To avoid problems working with the Web Console, you need to [configure the ZooKeeper PStore]({{site.baseurl}}/docs/persistent-configuration-storage/#configuring-zookeeper-pstore).
