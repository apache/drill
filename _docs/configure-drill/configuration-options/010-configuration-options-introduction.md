---
title: "Configuration Options Introduction"
parent: "Configuration Options"
---
Drill provides many configuration options that you can enable, disable, or
modify. Modifying certain configuration options can impact Drill’s
performance. Many of Drill's configuration options reside in the `drill-
env.sh` and `drill-override.conf` files. Drill stores these files in the
`/conf` directory. Drill sources` /etc/drill/conf` if it exists. Otherwise,
Drill sources the local `<drill_installation_directory>/conf` directory.

The sys.options table in Drill contains information about boot (start-up) and system options. The section, ["Start-up Options"]({{site.baseurl}}/docs/start-up-options), covers how to configure and view key boot options. The sys.options table also contains many system options, some of which are described in detail the section, ["Planning and Execution Options"]({{site.baseurl}}/docs/planning-and-execution-options). The following table lists the options in alphabetical order and provides a brief description of supported options:

## System Options
The sys.options table lists the following options that you can set at the session or system level as described in the section, ["Planning and Execution Options"]({{site.baseurl}}/docs/planning-and-execution-options) 

<table>
  <tr>
    <th>Name</th>
    <th>Default</th>
    <th>Comments</th>
  </tr>
  <tr>
    <td>drill.exec.functions.cast_empty_string_to_null</td>
    <td>FALSE</td>
    <td>Not supported in this release.</td>
  </tr>
  <tr>
    <td>drill.exec.storage.file.partition.column.label</td>
    <td>dir</td>
    <td>Accepts a string input.</td>
  </tr>
  <tr>
    <td>exec.errors.verbose</td>
    <td>FALSE</td>
    <td>Toggles verbose output of executable error messages</td>
  </tr>
  <tr>
    <td>exec.java_compiler</td>
    <td>DEFAULT</td>
    <td>Switches between DEFAULT, JDK, and JANINO mode for the current session. Uses Janino by default for generated source code of less than exec.java_compiler_janino_maxsize; otherwise, switches to the JDK compiler.</td>
  </tr>
  <tr>
    <td>exec.java_compiler_debug</td>
    <td>TRUE</td>
    <td>Toggles the output of debug-level compiler error messages in runtime generated code.</td>
  </tr>
  <tr>
    <td>exec.java_compiler_janino_maxsize</td>
    <td>262144</td>
    <td>See the exec.java_compiler option comment. Accepts inputs of type LONG.</td>
  </tr>
  <tr>
    <td>exec.max_hash_table_size</td>
    <td>1073741824</td>
    <td>Ending size for hash tables. Range: 0 - 1073741824</td>
  </tr>
  <tr>
    <td>exec.min_hash_table_size</td>
    <td>65536</td>
    <td>Starting size for hash tables. Increase according to available memory to improve performance. Range: 0 - 1073741824</td>
  </tr>
  <tr>
    <td>exec.queue.enable</td>
    <td>FALSE</td>
    <td>Changes the state of query queues to control the number of queries that run simultaneously.</td>
  </tr>
  <tr>
    <td>exec.queue.large</td>
    <td>10</td>
    <td>Sets the number of large queries that can run concurrently in the cluster.
Range: 0-1000</td>
  </tr>
  <tr>
    <td>exec.queue.small</td>
    <td>100</td>
    <td>Sets the number of small queries that can run concurrently in the cluster. Range: 0-1001</td>
  </tr>
  <tr>
    <td>exec.queue.threshold</td>
    <td>30000000</td>
    <td>Sets the cost threshold, which depends on the complexity of the queries in queue, for determining whether query is large or small. Complex queries have higher thresholds. Range: 0-9223372036854775807</td>
  </tr>
  <tr>
    <td>exec.queue.timeout_millis</td>
    <td>300000</td>
    <td>Indicates how long a query can wait in queue before the query fails. Range: 0-9223372036854775807</td>
  </tr>
  <tr>
    <td>planner.add_producer_consumer</td>
    <td>FALSE</td>
    <td>Increase prefetching of data from disk. Disable for in-memory reads.</td>
  </tr>
  <tr>
    <td>planner.affinity_factor</td>
    <td>1.2</td>
    <td>Accepts inputs of type DOUBLE.</td>
  </tr>
  <tr>
    <td>planner.broadcast_factor</td>
    <td>1</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.broadcast_threshold</td>
    <td>10000000</td>
    <td>The maximum number of records allowed to be broadcast as part of a query. After one million records, Drill reshuffles data rather than doing a broadcast to one side of the join. Range: 0-2147483647</td>
  </tr>
  <tr>
    <td>planner.disable_exchanges</td>
    <td>FALSE</td>
    <td>Toggles the state of hashing to a random exchange.</td>
  </tr>
  
  <tr>
    <td>planner.enable_broadcast_join</td>
    <td>TRUE</td>
    <td>Changes the state of aggregation and join operators. Do not disable.</td>
  </tr>
  <tr>
    <td>planner.enable_constant_folding</td>
    <td>TRUE</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.enable_demux_exchange</td>
    <td>FALSE</td>
    <td>Toggles the state of hashing to a demulitplexed exchange.</td>
  </tr>
  <tr>
    <td>planner.enable_hash_single_key</td>
    <td>TRUE</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.enable_hashagg</td>
    <td>TRUE</td>
    <td>Enable hash aggregation; otherwise, Drill does a sort-based aggregation. Does not write to disk. Enable is recommended.</td>
  </tr>
  <tr>
    <td>planner.enable_hashjoin</td>
    <td>TRUE</td>
    <td>Enable the memory hungry hash join. Drill assumes that a query with have adequate memory to complete and tries to use the fastest operations possible to complete the planned inner, left, right, or full outer joins using a hash table. Does not write to disk. Disabling hash join allows Drill to manage arbitrarily large data in a small memory footprint.</td>
  </tr>
  <tr>
    <td>planner.enable_hashjoin_swap</td>
    <td>TRUE</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.enable_mergejoin</td>
    <td>TRUE</td>
    <td>Sort-based operation. A merge join is used for inner join, left and right outer joins.  Inputs to the merge join must be sorted. It reads the sorted input streams from both sides and finds matching rows. Writes to disk.</td>
  </tr>
  <tr>
    <td>planner.enable_multiphase_agg</td>
    <td>TRUE</td>
    <td>Each minor fragment does a local aggregation in phase 1, distributes on a hash basis using GROUP-BY keys partially aggregated results to other fragments, and all the fragments perform a total aggregation using this data.  
 </td>
  </tr>
  <tr>
    <td>planner.enable_mux_exchange</td>
    <td>TRUE</td>
    <td>Toggles the state of hashing to a multiplexed exchange.</td>
  </tr>
  <tr>
    <td>planner.enable_nestedloopjoin</td>
    <td>TRUE</td>
    <td>Sort-based operation. Writes to disk.</td>
  </tr>
  <tr>
    <td>planner.enable_nljoin_for_scalar_only</td>
    <td>TRUE</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.enable_streamagg</td>
    <td>TRUE</td>
    <td>Sort-based operation. Writes to disk.</td>
  </tr>
  <tr>
    <td>planner.identifier_max_length</td>
    <td>1024</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.join.hash_join_swap_margin_factor</td>
    <td>10</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.join.row_count_estimate_factor</td>
    <td>1</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.memory.average_field_width</td>
    <td>8</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.memory.enable_memory_estimation</td>
    <td>FALSE</td>
    <td>Toggles the state of memory estimation and re-planning of the query.
When enabled, Drill conservatively estimates memory requirements and typically excludes these operators from the plan and negatively impacts performance.</td>
  </tr>
  <tr>
    <td>planner.memory.hash_agg_table_factor</td>
    <td>1.1</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.memory.hash_join_table_factor</td>
    <td>1.1</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.memory.max_query_memory_per_node</td>
    <td>2147483648</td>
    <td>Sets the maximum estimate of memory for a query per node. If the estimate is too low, Drill re-plans the query without memory-constrained operators.</td>
  </tr>
  <tr>
    <td>planner.memory.non_blocking_operators_memory</td>
    <td>64</td>
    <td>Range: 0-2048</td>
  </tr>
  <tr>
    <td>planner.nestedloopjoin_factor</td>
    <td>100</td>
    <td></td>
  </tr>
    <tr>
    <td>planner.partitioner_sender_max_threads</td>
    <td>8</td>
    <td>Upper limit of threads for outbound queuing.</td>
  </tr>
  <tr>
    <td>planner.partitioner_sender_set_threads</td>
    <td>-1</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.partitioner_sender_threads_factor</td>
    <td>1</td>
    <td></td>
  </tr>
  <tr>
    <td>planner.producer_consumer_queue_size</td>
    <td>10</td>
    <td>How much data to prefetch from disk (in record batches) out of band of query execution</td>
  </tr>
  <tr>
    <td>planner.slice_target</td>
    <td>100000</td>
    <td>The number of records manipulated within a fragment before Drill parallelizes operations.</td>
  </tr>
  <tr>
    <td>planner.width.max_per_node</td>
    <td>3</td>
    <td>Maximum number of threads that can run in parallel for a query on a node. A slice is an individual thread. This number indicates the maximum number of slices per query for the query’s major fragment on a node.</td>
  </tr>
  <tr>
    <td>planner.width.max_per_query</td>
    <td>1000</td>
    <td>Same as max per node but applies to the query as executed by the entire cluster.</td>
  </tr>
  <tr>
    <td>store.format</td>
    <td>parquet</td>
    <td>Output format for data written to tables with the CREATE TABLE AS (CTAS) command. Allowed values are parquet, json, or text. Allowed values: 0, -1, 1000000</td>
  </tr>
  <tr>
    <td>store.json.all_text_mode</a></td>
    <td>FALSE</td>
    <td>Drill reads all data from the JSON files as VARCHAR. Prevents schema change errors.</td>
  </tr>
  <tr>
    <td>store.mongo.all_text_mode</td>
    <td>FALSE</td>
    <td>Similar to store.json.all_text_mode for MongoDB.</td>
  </tr>
  <tr>
    <td>store.parquet.block-size</a></td>
    <td>536870912</td>
    <td>Sets the size of a Parquet row group to the number of bytes less than or equal to the block size of MFS, HDFS, or the file system.</td>
  </tr>
  <tr>
    <td>store.parquet.compression</td>
    <td>snappy</td>
    <td>Compression type for storing Parquet output. Allowed values: snappy, gzip, none</td>
  </tr>
  <tr>
    <td>store.parquet.enable_dictionary_encoding*</td>
    <td>FALSE</td>
    <td>Do not change.</td>
  </tr>
  <tr>
    <td>store.parquet.use_new_reader</td>
    <td>FALSE</td>
    <td>Not supported</td>
  </tr>
  <tr>
    <td>window.enable*</td>
    <td>FALSE</td>
    <td>Coming soon.</td>
  </tr>
</table>

\* Not supported in this release.



