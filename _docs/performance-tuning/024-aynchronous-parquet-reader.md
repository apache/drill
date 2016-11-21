---
title: "Asynchronous Parquet Reader"
date: 2016-11-21 21:25:59 UTC
parent: "Performance Tuning"
---

Drill 1.9 introduces an asynchronous Parquet reader option that you can enable to improve the performance of the Parquet Scan operator. The Parquet Scan operator reads Parquet data. Reading Parquet data involves scanning the disk, decompressing and decoding the data, and writing data to internal memory structures (value vectors).  

When the asynchronous parquet reader option is enabled, the speed at which the Parquet reader scans, decompresses, and decodes the data increases. The scan operation uses a buffering read strategy that allows the file system to perform larger, sequential reads, which significantly improves query performance.  

Typically, the Drill default settings provide the best performance for a wide variety of use cases. However, specific cases that require a high level of performance can benefit from tuning the Parquet Scan operator.  

##Tuning the Parquet Scan Operator  
The `store.parquet.reader.pagereader.async` option turns the asynchronous Parquet reader on or off. The option is turned on by default. You can use the [ALTER SESSION SET command]({{site.baseurl}}/docs/alter-session-command/) to enable the asynchronous Parquet reader option, as well as the options that control buffering and parallel decoding.  

When the asynchronous Page reader option is enabled, the Parquet Scan operator no longer reports operator wait time. Instead, it reports additional operator metrics that you can view in the query profile in the Drill Web Console.  

The `drill.exec.scan.threadpool_size` and `drill.exec.scan.decode_threadpool_size` parameters in the `drill-override.conf` file control the size of the threadpools that read and decode Parquet data when the asynchronous Parquet reader is enabled.  

For more information, see the [functional specification](https://github.com/parthchandra/drill/wiki/Parquet-file-reading-performance-improvement).  

The following sections provide the configuration options and details:  

###Asynchronous Parquet Reader Options  

The following table lists and describes the asynchronous Parquet reader options that you can enable or disable using the [ALTER SESSION SET command]({{site.baseurl}}/docs/alter-session-command/):  

|       Option                                 | Description                                                                                                                                                                                                                                                                                                                                                          | Type    | Default     |
|----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|-------------|
| store.parquet.reader.pagereader.async        | Enable the asynchronous page reader. This   pipelines the reading of data from disk for high performance.                                                                                                                                                                                                                                                            | BOOLEAN | TRUE        |
| store.parquet.reader.pagereader.bufferedread | Enable buffered page reading. Can improve disk   scan speeds by buffering data, but increases memory usage. This option is   less useful when the number of columns increases.                                                                                                                                                                                       | BOOLEAN | TRUE        |
| store.parquet.reader.pagereader.buffersize   | The size of the buffer (in bytes) to use if   bufferedread is true. Has no effect otherwise.                                                                                                                                                                                                                                                                         | LONG    | 4194304     |
| store.parquet.reader.pagereader.usefadvise   | If the file system supports it, the Parquet file   reader issues an fadvise call to enable file server side sequential reading   and caching. Since many HDFS implementations do not support this and because   this may have no effect in conditions of high concurrency, the option is set   to false. Useful for benchmarks and for performance critical queries. | BOOLEAN | FALSE       |
| store.parquet.reader.columnreader.async      | Turn on parallel decoding of column data from   Parquet to the in memory format. This increases CPU usage and is most useful   for compressed fixed width data. With increasing concurrency, this option may   cause queries to run slower and should be turned on only for performance   critical queries.                                                          | BOOLEAN | FALSE       |  

###Drillbit Configuration Parameters
The following table lists and describes the drillbit configuration parameters in `drill-override.conf` that control the size of the threadpools used by the asynchronous Parquet reader:  

**Note:** You must restart the drillbit for these configuration changes to take effect.  

|       Configuration Option             | Description                                                                                                                                                                                                                                                                                                                                                                                                                       | Default               |
|----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|
| drill.exec.scan.threadpool_size        | The size of the thread pool used for reading   data from disk. Currently used only by the Parquet reader. This number should   ideally be a small multiple of the number of disks on the node. The   pipelining of the scan operator is very sensitive to the scan thread pool   size. For the best performance, set the number to 1-2 times the number of   disks on the node that are available to the distributed file system. | 8                     |
| drill.exec.scan.decode_threadpool_size | The size of the thread pool used for decoding   Parquet data.                                                                                                                                                                                                                                                                                                                                                                     | (number of cores+1)/2 |  

###Operator Metrics
When the asynchronous Parquet reader option is enabled, Drill provides the following additional operator metrics, which you can access in the query profile from the Drill Web Console:  

**Note:** Time is measured in nanoseconds.   

|       Metric                  | Description                                                                                                                                                                                                                                                           |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NUM_DICT_PAGE_LOADS           | Number of dictionary pages read.                                                                                                                                                                                                                                      |
| NUM_DATA_PAGE_lOADS           | Number of data pages read.                                                                                                                                                                                                                                            |
| NUM_DATA_PAGES_DECODED        | Number of data pages decoded.                                                                                                                                                                                                                                         |
| NUM_DICT_PAGES_DECOMPRESSED   | Number of dictionary pages decompressed.                                                                                                                                                                                                                              |
| NUM_DATA_PAGES_DECOMPRESSED   | Number of data pages decompressed.                                                                                                                                                                                                                                    |
| TOTAL_DICT_PAGE_READ_BYTES    | Total bytes read from disk for dictionary pages.                                                                                                                                                                                                                      |
| TOTAL_DATA_PAGE_READ_BYTES    | Total bytes read from disk for data pages.                                                                                                                                                                                                                            |
| TOTAL_DICT_DECOMPRESSED_BYTES | Total bytes decompressed for dictionary pages.   Same as compressed bytes on disk.                                                                                                                                                                                    |
| TOTAL_DATA_DECOMPRESSED_BYTES | Total bytes decompressed for data pages. Same as   compressed bytes on disk.                                                                                                                                                                                          |
| TIME_DICT_PAGE_LOADS          | Time spent reading dictionary pages   from disk.                                                                                                                                                                                                                      |
| TIME_DATA_PAGE_LOADS          | Time spent reading data pages from   disk.                                                                                                                                                                                                                            |
| TIME_DATA_PAGE_DECODE         | Time spend decoding data pages.                                                                                                                                                                                                                                       |
| TIME_DICT_PAGE_DECODE         | Time spent decoding dictionary pages.                                                                                                                                                                                                                                 |
| TIME_DICT_PAGES_DECOMPRESSED  | Time spent decompressing dictionary   pages.                                                                                                                                                                                                                          |
| TIME_DATA_PAGES_DECOMPRESSED  | Time spent decompressing data pages.                                                                                                                                                                                                                                  |
| TIME_DISK_SCAN_WAIT           | The total time spent by the   Parquet Scan operator waiting for the data to be read from disk (completion   of an asynchronous disk read to complete). In general, if TIME_DISK_SCAN_WAIT   is high, then the query is disk bound and may benefit from faster drives. |
| TIME_DISK_SCAN                | The time that the Parquet Scan   operator spent reading data from the disk (or more accurately, from the   filesystem). TIME_DISK_SCAN is the equivalent metric to the operator wait   time reported by the synchronous version of the Parquet reader.                |  

##Limitation
The asynchronous Parquet reader option can increase the amount of memory required to read a single column of Parquet data up to 8MB. When the data in a column is less than 8MB, the reader uses less memory. Therefore, if a Parquet file has many columns (hundreds of columns), each column should have less than 8MB of data in each column.  




