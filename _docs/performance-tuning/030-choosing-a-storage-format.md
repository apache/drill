---
title: "Choosing a Storage Format"
date: 2018-11-02
parent: "Performance Tuning"
--- 
Drill supports several file formats for data including CSV, TSV, PSV, JSON, and Parquet. Changing the default format is a typical functional change that can optimize performance. Drill runs fastest against Parquet files because Parquet data representation is almost identical to how Drill represents data.

Optimized for working with large files, Parquet arranges data in columns, putting related values in close proximity to each other to optimize query performance, minimize I/O, and facilitate compression. Parquet detects and encodes the same or similar data using a technique that conserves resources.

When using Parquet as the storage format, balance the number of files against the file size to achieve maximum parallelization. See [Configuring the Size of Parquet Files]({{ site.baseurl }}/docs/parquet-format/#configuring-the-size-of-parquet-files).  

When a read of Parquet data occurs, Drill loads only the necessary columns of data, which reduces I/O. Reading only a small piece of the Parquet data from a data file or table, Drill can examine and analyze all values for a column across multiple files.
 
Because SQL does not support all Parquet data types, to prevent Drill from inferring a type other than the one you want, you can use the [CAST or CONVERT functions]({{ site.baseurl }}/docs/data-type-conversion/#cast). See [Data Type Conversion]({{ site.baseurl }}/docs/data-type-conversion/).
 
See [Parquet Format]({{ site.baseurl }}/docs/parquet-format/) for more information about Parquet with Drill. You may also be interested in the [JSON Data Model]({{ site.baseurl }}/docs/json-data-model/), [Data Sources and File Formats Introduction]({{ site.baseurl }}/docs/data-sources-and-file-formats-introduction/), and [Supported Data Types]({{ site.baseurl }}/docs/supported-data-types/).

