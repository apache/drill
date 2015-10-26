/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec;

import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators.AdminOptionValidator;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.DoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.EnumeratedStringValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.PositiveLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.PowerOfTwoLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeDoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.util.ImpersonationUtil;

public interface ExecConstants {
  public static final String ZK_RETRY_TIMES = "drill.exec.zk.retry.count";
  public static final String ZK_RETRY_DELAY = "drill.exec.zk.retry.delay";
  public static final String ZK_CONNECTION = "drill.exec.zk.connect";
  public static final String ZK_TIMEOUT = "drill.exec.zk.timeout";
  public static final String ZK_ROOT = "drill.exec.zk.root";
  public static final String ZK_REFRESH = "drill.exec.zk.refresh";
  public static final String BIT_RETRY_TIMES = "drill.exec.rpc.bit.server.retry.count";
  public static final String BIT_RETRY_DELAY = "drill.exec.rpc.bit.server.retry.delay";
  public static final String BIT_TIMEOUT = "drill.exec.bit.timeout" ;
  public static final String SERVICE_NAME = "drill.exec.cluster-id";
  public static final String INITIAL_BIT_PORT = "drill.exec.rpc.bit.server.port";
  public static final String BIT_RPC_TIMEOUT = "drill.exec.rpc.bit.timeout";
  public static final String INITIAL_USER_PORT = "drill.exec.rpc.user.server.port";
  public static final String USER_RPC_TIMEOUT = "drill.exec.rpc.user.timeout";
  public static final String METRICS_CONTEXT_NAME = "drill.exec.metrics.context";
  public static final String USE_IP_ADDRESS = "drill.exec.rpc.use.ip";
  public static final String METRICS_JMX_OUTPUT_ENABLED = "drill.exec.metrics.jmx.enabled";
  public static final String METRICS_LOG_OUTPUT_ENABLED = "drill.exec.metrics.log.enabled";
  public static final String METRICS_LOG_OUTPUT_INTERVAL = "drill.exec.metrics.log.interval";
  public static final String CLIENT_RPC_THREADS = "drill.exec.rpc.user.client.threads";
  public static final String BIT_SERVER_RPC_THREADS = "drill.exec.rpc.bit.server.threads";
  public static final String USER_SERVER_RPC_THREADS = "drill.exec.rpc.user.server.threads";
  public static final String TRACE_DUMP_DIRECTORY = "drill.exec.trace.directory";
  public static final String TRACE_DUMP_FILESYSTEM = "drill.exec.trace.filesystem";
  public static final String TEMP_DIRECTORIES = "drill.exec.tmp.directories";
  public static final String TEMP_FILESYSTEM = "drill.exec.tmp.filesystem";
  public static final String INCOMING_BUFFER_IMPL = "drill.exec.buffer.impl";
  /** incoming buffer size (number of batches) */
  public static final String INCOMING_BUFFER_SIZE = "drill.exec.buffer.size";
  public static final String SPOOLING_BUFFER_DELETE = "drill.exec.buffer.spooling.delete";
  public static final String SPOOLING_BUFFER_MEMORY = "drill.exec.buffer.spooling.size";
  public static final String BATCH_PURGE_THRESHOLD = "drill.exec.sort.purge.threshold";
  public static final String EXTERNAL_SORT_TARGET_BATCH_SIZE = "drill.exec.sort.external.batch.size";
  public static final String EXTERNAL_SORT_TARGET_SPILL_BATCH_SIZE = "drill.exec.sort.external.spill.batch.size";
  public static final String EXTERNAL_SORT_SPILL_GROUP_SIZE = "drill.exec.sort.external.spill.group.size";
  public static final String EXTERNAL_SORT_SPILL_THRESHOLD = "drill.exec.sort.external.spill.threshold";
  public static final String EXTERNAL_SORT_SPILL_DIRS = "drill.exec.sort.external.spill.directories";
  public static final String EXTERNAL_SORT_SPILL_FILESYSTEM = "drill.exec.sort.external.spill.fs";
  public static final String EXTERNAL_SORT_MSORT_MAX_BATCHSIZE = "drill.exec.sort.external.msort.batch.maxsize";
  public static final String TEXT_LINE_READER_BATCH_SIZE = "drill.exec.storage.file.text.batch.size";
  public static final String TEXT_LINE_READER_BUFFER_SIZE = "drill.exec.storage.file.text.buffer.size";
  public static final String HAZELCAST_SUBNETS = "drill.exec.cache.hazel.subnets";
  public static final String TOP_LEVEL_MAX_ALLOC = "drill.exec.memory.top.max";
  public static final String HTTP_ENABLE = "drill.exec.http.enabled";
  public static final String HTTP_PORT = "drill.exec.http.port";
  public static final String HTTP_ENABLE_SSL = "drill.exec.http.ssl_enabled";
  public static final String HTTP_KEYSTORE_PATH = "javax.net.ssl.keyStore";
  public static final String HTTP_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  public static final String HTTP_TRUSTSTORE_PATH = "javax.net.ssl.trustStore";
  public static final String HTTP_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  public static final String SYS_STORE_PROVIDER_CLASS = "drill.exec.sys.store.provider.class";
  public static final String SYS_STORE_PROVIDER_LOCAL_PATH = "drill.exec.sys.store.provider.local.path";
  public static final String SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE = "drill.exec.sys.store.provider.local.write";
  public static final String IMPERSONATION_ENABLED = "drill.exec.impersonation.enabled";
  public static final String IMPERSONATION_MAX_CHAINED_USER_HOPS = "drill.exec.impersonation.max_chained_user_hops";
  public static final String USER_AUTHENTICATION_ENABLED = "drill.exec.security.user.auth.enabled";
  public static final String USER_AUTHENTICATOR_IMPL = "drill.exec.security.user.auth.impl";
  public static final String PAM_AUTHENTICATOR_PROFILES = "drill.exec.security.user.auth.pam_profiles";
  public static final String ERROR_ON_MEMORY_LEAK = "drill.exec.debug.error_on_leak";
  /** Size of JDBC batch queue (in batches) above which throttling begins. */
  public static final String JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD =
      "drill.jdbc.batch_queue_throttling_threshold";

  /**
   * Currently if a query is cancelled, but one of the fragments reports the status as FAILED instead of CANCELLED or
   * FINISHED we report the query result as CANCELLED by swallowing the failures occurred in fragments. This BOOT
   * setting allows the user to see the query status as failure. Useful for developers/testers.
   */
  public static final String RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS =
      "drill.exec.debug.return_error_for_failure_in_cancelled_fragments";

  /** Fragment memory planning */
  public static final String ENABLE_FRAGMENT_MEMORY_LIMIT = "drill.exec.memory.enable_frag_limit";
  public static final String FRAGMENT_MEM_OVERCOMMIT_FACTOR = "drill.exec.memory.frag_mem_overcommit_factor";

  public static final String USE_LINUX_EPOLL = "drill.exec.enable-epoll";

  public static final String CLIENT_SUPPORT_COMPLEX_TYPES = "drill.client.supports-complex-types";

  public static final String OUTPUT_FORMAT_OPTION = "store.format";
  public static final OptionValidator OUTPUT_FORMAT_VALIDATOR = new StringValidator(OUTPUT_FORMAT_OPTION, "parquet");
  public static final String PARQUET_BLOCK_SIZE = "store.parquet.block-size";
  public static final OptionValidator PARQUET_BLOCK_SIZE_VALIDATOR = new LongValidator(PARQUET_BLOCK_SIZE, 512*1024*1024);
  public static final String PARQUET_PAGE_SIZE = "store.parquet.page-size";
  public static final OptionValidator PARQUET_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_PAGE_SIZE, 1024*1024);
  public static final String PARQUET_DICT_PAGE_SIZE = "store.parquet.dictionary.page-size";
  public static final OptionValidator PARQUET_DICT_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_DICT_PAGE_SIZE, 1024*1024);
  public static final String PARQUET_WRITER_COMPRESSION_TYPE = "store.parquet.compression";
  public static final OptionValidator PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR = new EnumeratedStringValidator(
      PARQUET_WRITER_COMPRESSION_TYPE, "snappy", "snappy", "gzip", "none");
  public static final String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING = "store.parquet.enable_dictionary_encoding";
  public static final OptionValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR = new BooleanValidator(
      PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, false);

  public static final String PARQUET_VECTOR_FILL_THRESHOLD = "store.parquet.vector_fill_threshold";
  public static final OptionValidator PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR = new PositiveLongValidator(PARQUET_VECTOR_FILL_THRESHOLD, 99l, 85l);
  public static final String PARQUET_VECTOR_FILL_CHECK_THRESHOLD = "store.parquet.vector_fill_check_threshold";
  public static final OptionValidator PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR = new PositiveLongValidator(PARQUET_VECTOR_FILL_CHECK_THRESHOLD, 100l, 10l);
  public static String PARQUET_NEW_RECORD_READER = "store.parquet.use_new_reader";
  public static OptionValidator PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR = new BooleanValidator(PARQUET_NEW_RECORD_READER, false);

  public static OptionValidator COMPILE_SCALAR_REPLACEMENT = new BooleanValidator("exec.compile.scalar_replacement", false);

  public static String JSON_ALL_TEXT_MODE = "store.json.all_text_mode";
  public static BooleanValidator JSON_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(JSON_ALL_TEXT_MODE, false);
  public static final BooleanValidator JSON_EXTENDED_TYPES = new BooleanValidator("store.json.extended_types", false);
  public static final DoubleValidator TEXT_ESTIMATED_ROW_SIZE = new RangeDoubleValidator(
      "store.text.estimated_row_size_bytes", 1, Long.MAX_VALUE, 100.0);

  /**
   * The column label (for directory levels) in results when querying files in a directory
   * E.g.  labels: dir0   dir1
   *    structure: foo
   *                |-    bar  -  a.parquet
   *                |-    baz  -  b.parquet
   */
  public static final String FILESYSTEM_PARTITION_COLUMN_LABEL = "drill.exec.storage.file.partition.column.label";
  public static final OptionValidator FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR = new StringValidator(FILESYSTEM_PARTITION_COLUMN_LABEL, "dir");

  public static String JSON_READ_NUMBERS_AS_DOUBLE = "store.json.read_numbers_as_double";
  public static OptionValidator JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(JSON_READ_NUMBERS_AS_DOUBLE, false);

  public static String MONGO_ALL_TEXT_MODE = "store.mongo.all_text_mode";
  public static OptionValidator MONGO_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(MONGO_ALL_TEXT_MODE, false);
  public static String MONGO_READER_READ_NUMBERS_AS_DOUBLE = "store.mongo.read_numbers_as_double";
  public static OptionValidator MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(MONGO_READER_READ_NUMBERS_AS_DOUBLE, false);

  // TODO: We need to add a feature that enables storage plugins to add their own options. Currently we have to declare
  // in core which is not right. Move this option and above two mongo plugin related options once we have the feature.
  public static String HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS = "store.hive.optimize_scan_with_native_readers";
  public static OptionValidator HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR =
      new BooleanValidator(HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS, false);

  public static final String SLICE_TARGET = "planner.slice_target";
  public static final long SLICE_TARGET_DEFAULT = 100000l;
  public static final OptionValidator SLICE_TARGET_OPTION = new PositiveLongValidator(SLICE_TARGET, Long.MAX_VALUE, SLICE_TARGET_DEFAULT);

  public static final String CAST_TO_NULLABLE_NUMERIC = "drill.exec.functions.cast_empty_string_to_null";
  public static final OptionValidator CAST_TO_NULLABLE_NUMERIC_OPTION = new BooleanValidator(CAST_TO_NULLABLE_NUMERIC, false);

  /**
   * HashTable runtime settings
   */
  public static final String MIN_HASH_TABLE_SIZE_KEY = "exec.min_hash_table_size";
  public static final OptionValidator MIN_HASH_TABLE_SIZE = new PositiveLongValidator(MIN_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.DEFAULT_INITIAL_CAPACITY);
  public static final String MAX_HASH_TABLE_SIZE_KEY = "exec.max_hash_table_size";
  public static final OptionValidator MAX_HASH_TABLE_SIZE = new PositiveLongValidator(MAX_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.MAXIMUM_CAPACITY);

  /**
   * Limits the maximum level of parallelization to this factor time the number of Drillbits
   */
  public static final String MAX_WIDTH_PER_NODE_KEY = "planner.width.max_per_node";
  public static final OptionValidator MAX_WIDTH_PER_NODE = new PositiveLongValidator(MAX_WIDTH_PER_NODE_KEY, Integer.MAX_VALUE, (long) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.70));

  /**
   * The maximum level or parallelization any stage of the query can do. Note that while this
   * might be the number of active Drillbits, realistically, this could be well beyond that
   * number of we want to do things like speed results return.
   */
  public static final String MAX_WIDTH_GLOBAL_KEY = "planner.width.max_per_query";
  public static final OptionValidator MAX_WIDTH_GLOBAL = new PositiveLongValidator(MAX_WIDTH_GLOBAL_KEY, Integer.MAX_VALUE, 1000);

  /**
   * Factor by which a node with endpoint affinity will be favored while creating assignment
   */
  public static final String AFFINITY_FACTOR_KEY = "planner.affinity_factor";
  public static final OptionValidator AFFINITY_FACTOR = new DoubleValidator(AFFINITY_FACTOR_KEY, 1.2d);

  public static final String ENABLE_MEMORY_ESTIMATION_KEY = "planner.memory.enable_memory_estimation";
  public static final OptionValidator ENABLE_MEMORY_ESTIMATION = new BooleanValidator(ENABLE_MEMORY_ESTIMATION_KEY, false);

  /**
   * Maximum query memory per node (in MB). Re-plan with cheaper operators if memory estimation exceeds this limit.
   * <p/>
   * DEFAULT: 2048 MB
   */
  public static final String MAX_QUERY_MEMORY_PER_NODE_KEY = "planner.memory.max_query_memory_per_node";
  public static final LongValidator MAX_QUERY_MEMORY_PER_NODE = new RangeLongValidator(
      MAX_QUERY_MEMORY_PER_NODE_KEY, 1024 * 1024, Long.MAX_VALUE, 2 * 1024 * 1024 * 1024L);

  /**
   * Extra query memory per node for non-blocking operators.
   * NOTE: This option is currently used only for memory estimation.
   * <p/>
   * DEFAULT: 64 MB
   * MAXIMUM: 2048 MB
   */
  public static final String NON_BLOCKING_OPERATORS_MEMORY_KEY = "planner.memory.non_blocking_operators_memory";
  public static final OptionValidator NON_BLOCKING_OPERATORS_MEMORY = new PowerOfTwoLongValidator(
    NON_BLOCKING_OPERATORS_MEMORY_KEY, 1 << 11, 1 << 6);

  public static final String HASH_JOIN_TABLE_FACTOR_KEY = "planner.memory.hash_join_table_factor";
  public static final OptionValidator HASH_JOIN_TABLE_FACTOR = new DoubleValidator(HASH_JOIN_TABLE_FACTOR_KEY, 1.1d);

  public static final String HASH_AGG_TABLE_FACTOR_KEY = "planner.memory.hash_agg_table_factor";
  public static final OptionValidator HASH_AGG_TABLE_FACTOR = new DoubleValidator(HASH_AGG_TABLE_FACTOR_KEY, 1.1d);

  public static final String AVERAGE_FIELD_WIDTH_KEY = "planner.memory.average_field_width";
  public static final OptionValidator AVERAGE_FIELD_WIDTH = new PositiveLongValidator(AVERAGE_FIELD_WIDTH_KEY, Long.MAX_VALUE, 8);

  public static final BooleanValidator ENABLE_QUEUE = new BooleanValidator("exec.queue.enable", false);
  public static final LongValidator LARGE_QUEUE_SIZE = new PositiveLongValidator("exec.queue.large", 1000, 10);
  public static final LongValidator SMALL_QUEUE_SIZE = new PositiveLongValidator("exec.queue.small", 100000, 100);
  public static final LongValidator QUEUE_THRESHOLD_SIZE = new PositiveLongValidator("exec.queue.threshold",
      Long.MAX_VALUE, 30000000);
  public static final LongValidator QUEUE_TIMEOUT = new PositiveLongValidator("exec.queue.timeout_millis",
      Long.MAX_VALUE, 60 * 1000 * 5);

  public static final String ENABLE_VERBOSE_ERRORS_KEY = "exec.errors.verbose";
  public static final OptionValidator ENABLE_VERBOSE_ERRORS = new BooleanValidator(ENABLE_VERBOSE_ERRORS_KEY, false);

  public static final String ENABLE_NEW_TEXT_READER_KEY = "exec.storage.enable_new_text_reader";
  public static final OptionValidator ENABLE_NEW_TEXT_READER = new BooleanValidator(ENABLE_NEW_TEXT_READER_KEY, true);

  public static final String BOOTSTRAP_STORAGE_PLUGINS_FILE = "bootstrap-storage-plugins.json";
  public static final String MAX_LOADING_CACHE_SIZE_CONFIG = "drill.exec.compile.cache_max_size";

  public static final String DRILL_SYS_FILE_SUFFIX = ".sys.drill";

  public static final String ENABLE_WINDOW_FUNCTIONS = "window.enable";
  public static final OptionValidator ENABLE_WINDOW_FUNCTIONS_VALIDATOR = new BooleanValidator(ENABLE_WINDOW_FUNCTIONS, true);

  public static final String DRILLBIT_CONTROL_INJECTIONS = "drill.exec.testing.controls";
  public static final OptionValidator DRILLBIT_CONTROLS_VALIDATOR =
    new ExecutionControls.ControlsOptionValidator(DRILLBIT_CONTROL_INJECTIONS, ExecutionControls.DEFAULT_CONTROLS, 1);

  public static final String NEW_VIEW_DEFAULT_PERMS_KEY = "new_view_default_permissions";
  public static final OptionValidator NEW_VIEW_DEFAULT_PERMS_VALIDATOR =
      new StringValidator(NEW_VIEW_DEFAULT_PERMS_KEY, "700");

  public static final String USE_OLD_ASSIGNMENT_CREATOR = "exec.schedule.assignment.old";
  public static final OptionValidator USE_OLD_ASSIGNMENT_CREATOR_VALIDATOR = new BooleanValidator(USE_OLD_ASSIGNMENT_CREATOR, false);

  public static final String CTAS_PARTITIONING_HASH_DISTRIBUTE = "store.partition.hash_distribute";
  public static final BooleanValidator CTAS_PARTITIONING_HASH_DISTRIBUTE_VALIDATOR = new BooleanValidator(CTAS_PARTITIONING_HASH_DISTRIBUTE, false);

  /**
   * Option whose value is a comma separated list of admin usernames. Admin users are users who have special privileges
   * such as changing system options.
   */
  String ADMIN_USERS_KEY = "security.admin.users";
  StringValidator ADMIN_USERS_VALIDATOR =
      new AdminOptionValidator(ADMIN_USERS_KEY, ImpersonationUtil.getProcessUserName());

  /**
   * Option whose value is a comma separated list of admin usergroups.
   */
  String ADMIN_USER_GROUPS_KEY = "security.admin.user_groups";
  StringValidator ADMIN_USER_GROUPS_VALIDATOR = new AdminOptionValidator(ADMIN_USER_GROUPS_KEY, "");
}
