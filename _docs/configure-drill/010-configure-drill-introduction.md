---
title: "Configure Drill Introduction"
parent: "Configure Drill"
---
When using Drill, you need to make sufficient memory available Drill and other workloads running on the cluster. You might want to modify options for performance or functionality. For example, the default storage format for CTAS
statements is Parquet. Using a configuration option, you can modify the default setting so that output data
is stored in CSV or JSON format. The section covers the many options you can configure and how to configure memory resources for Drill running along side other workloads. This section also includes ports used by Drill.
