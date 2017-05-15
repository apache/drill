/*
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
import org.apache.drill.exec.rpc.user.InboundImpersonationManager;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.DoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.EnumeratedStringValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.PositiveLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.PowerOfTwoLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeDoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.util.ImpersonationUtil;

public interface ExecConstants {
  String ZK_RETRY_TIMES = "drill.exec.zk.retry.count";
  String ZK_RETRY_DELAY = "drill.exec.zk.retry.delay";
  String ZK_CONNECTION = "drill.exec.zk.connect";
  String ZK_TIMEOUT = "drill.exec.zk.timeout";
  String ZK_ROOT = "drill.exec.zk.root";
  String ZK_REFRESH = "drill.exec.zk.refresh";
  String BIT_RETRY_TIMES = "drill.exec.rpc.bit.server.retry.count";
  String BIT_RETRY_DELAY = "drill.exec.rpc.bit.server.retry.delay";
  String BIT_TIMEOUT = "drill.exec.bit.timeout" ;
  String SERVICE_NAME = "drill.exec.cluster-id";
  String INITIAL_BIT_PORT = "drill.exec.rpc.bit.server.port";
  String INITIAL_DATA_PORT = "drill.exec.rpc.bit.server.dataport";
  String BIT_RPC_TIMEOUT = "drill.exec.rpc.bit.timeout";
  String INITIAL_USER_PORT = "drill.exec.rpc.user.server.port";
  String USER_RPC_TIMEOUT = "drill.exec.rpc.user.timeout";
  String METRICS_CONTEXT_NAME = "drill.exec.metrics.context";
  String USE_IP_ADDRESS = "drill.exec.rpc.use.ip";
  String CLIENT_RPC_THREADS = "drill.exec.rpc.user.client.threads";
  String BIT_SERVER_RPC_THREADS = "drill.exec.rpc.bit.server.threads";
  String USER_SERVER_RPC_THREADS = "drill.exec.rpc.user.server.threads";
  String TRACE_DUMP_DIRECTORY = "drill.exec.trace.directory";
  String TRACE_DUMP_FILESYSTEM = "drill.exec.trace.filesystem";
  String TEMP_DIRECTORIES = "drill.exec.tmp.directories";
  String TEMP_FILESYSTEM = "drill.exec.tmp.filesystem";
  String INCOMING_BUFFER_IMPL = "drill.exec.buffer.impl";
  /** incoming buffer size (number of batches) */
  String INCOMING_BUFFER_SIZE = "drill.exec.buffer.size";
  String SPOOLING_BUFFER_DELETE = "drill.exec.buffer.spooling.delete";
  String SPOOLING_BUFFER_MEMORY = "drill.exec.buffer.spooling.size";
  String BATCH_PURGE_THRESHOLD = "drill.exec.sort.purge.threshold";

  // External Sort Boot configuration

  String EXTERNAL_SORT_TARGET_SPILL_BATCH_SIZE = "drill.exec.sort.external.spill.batch.size";
  String EXTERNAL_SORT_SPILL_GROUP_SIZE = "drill.exec.sort.external.spill.group.size";
  String EXTERNAL_SORT_SPILL_THRESHOLD = "drill.exec.sort.external.spill.threshold";
  String EXTERNAL_SORT_SPILL_DIRS = "drill.exec.sort.external.spill.directories";
  String EXTERNAL_SORT_SPILL_FILESYSTEM = "drill.exec.sort.external.spill.fs";
  String EXTERNAL_SORT_SPILL_FILE_SIZE = "drill.exec.sort.external.spill.file_size";
  String EXTERNAL_SORT_MSORT_MAX_BATCHSIZE = "drill.exec.sort.external.msort.batch.maxsize";
  String EXTERNAL_SORT_DISABLE_MANAGED = "drill.exec.sort.external.disable_managed";
  String EXTERNAL_SORT_MERGE_LIMIT = "drill.exec.sort.external.merge_limit";
  String EXTERNAL_SORT_SPILL_BATCH_SIZE = "drill.exec.sort.external.spill.spill_batch_size";
  String EXTERNAL_SORT_MERGE_BATCH_SIZE = "drill.exec.sort.external.spill.merge_batch_size";
  String EXTERNAL_SORT_MAX_MEMORY = "drill.exec.sort.external.mem_limit";

  // Used only by the "unmanaged" sort.
  String EXTERNAL_SORT_BATCH_LIMIT = "drill.exec.sort.external.batch_limit";

  // External Sort Runtime options

  BooleanValidator EXTERNAL_SORT_DISABLE_MANAGED_OPTION = new BooleanValidator("exec.sort.disable_managed", false);


  String TEXT_LINE_READER_BATCH_SIZE = "drill.exec.storage.file.text.batch.size";
  String TEXT_LINE_READER_BUFFER_SIZE = "drill.exec.storage.file.text.buffer.size";
  String HAZELCAST_SUBNETS = "drill.exec.cache.hazel.subnets";
  String HTTP_ENABLE = "drill.exec.http.enabled";
  String HTTP_MAX_PROFILES = "drill.exec.http.max_profiles";
  String HTTP_PORT = "drill.exec.http.port";
  String HTTP_ENABLE_SSL = "drill.exec.http.ssl_enabled";
  String HTTP_CORS_ENABLED = "drill.exec.http.cors.enabled";
  String HTTP_CORS_ALLOWED_ORIGINS = "drill.exec.http.cors.allowedOrigins";
  String HTTP_CORS_ALLOWED_METHODS = "drill.exec.http.cors.allowedMethods";
  String HTTP_CORS_ALLOWED_HEADERS = "drill.exec.http.cors.allowedHeaders";
  String HTTP_CORS_CREDENTIALS = "drill.exec.http.cors.credentials";
  String HTTP_SESSION_MAX_IDLE_SECS = "drill.exec.http.session_max_idle_secs";
  String HTTP_KEYSTORE_PATH = "javax.net.ssl.keyStore";
  String HTTP_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  String HTTP_TRUSTSTORE_PATH = "javax.net.ssl.trustStore";
  String HTTP_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  String SYS_STORE_PROVIDER_CLASS = "drill.exec.sys.store.provider.class";
  String SYS_STORE_PROVIDER_LOCAL_PATH = "drill.exec.sys.store.provider.local.path";
  String SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE = "drill.exec.sys.store.provider.local.write";
  String PROFILES_STORE_INMEMORY = "drill.exec.profiles.store.inmemory";
  String PROFILES_STORE_CAPACITY = "drill.exec.profiles.store.capacity";
  String IMPERSONATION_ENABLED = "drill.exec.impersonation.enabled";
  String IMPERSONATION_MAX_CHAINED_USER_HOPS = "drill.exec.impersonation.max_chained_user_hops";
  String AUTHENTICATION_MECHANISMS = "drill.exec.security.auth.mechanisms";
  String USER_AUTHENTICATION_ENABLED = "drill.exec.security.user.auth.enabled";
  String USER_AUTHENTICATOR_IMPL = "drill.exec.security.user.auth.impl";
  String PAM_AUTHENTICATOR_PROFILES = "drill.exec.security.user.auth.pam_profiles";
  String BIT_AUTHENTICATION_ENABLED = "drill.exec.security.bit.auth.enabled";
  String BIT_AUTHENTICATION_MECHANISM = "drill.exec.security.bit.auth.mechanism";
  String USE_LOGIN_PRINCIPAL = "drill.exec.security.bit.auth.use_login_principal";
  /** Size of JDBC batch queue (in batches) above which throttling begins. */
  String JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD =
      "drill.jdbc.batch_queue_throttling_threshold";
  // Thread pool size for scan threads. Used by the Parquet scan.
  String SCAN_THREADPOOL_SIZE = "drill.exec.scan.threadpool_size";
  // The size of the thread pool used by a scan to decode the data. Used by Parquet
  String SCAN_DECODE_THREADPOOL_SIZE = "drill.exec.scan.decode_threadpool_size";

  /**
   * Currently if a query is cancelled, but one of the fragments reports the status as FAILED instead of CANCELLED or
   * FINISHED we report the query result as CANCELLED by swallowing the failures occurred in fragments. This BOOT
   * setting allows the user to see the query status as failure. Useful for developers/testers.
   */
  String RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS =
      "drill.exec.debug.return_error_for_failure_in_cancelled_fragments";

  String CLIENT_SUPPORT_COMPLEX_TYPES = "drill.client.supports-complex-types";

  /**
   * Configuration properties connected with dynamic UDFs support
   */
  String UDF_RETRY_ATTEMPTS = "drill.exec.udf.retry-attempts";
  String UDF_DIRECTORY_LOCAL = "drill.exec.udf.directory.local";
  String UDF_DIRECTORY_FS = "drill.exec.udf.directory.fs";
  String UDF_DIRECTORY_ROOT = "drill.exec.udf.directory.root";
  String UDF_DIRECTORY_STAGING = "drill.exec.udf.directory.staging";
  String UDF_DIRECTORY_REGISTRY = "drill.exec.udf.directory.registry";
  String UDF_DIRECTORY_TMP = "drill.exec.udf.directory.tmp";
  String UDF_DISABLE_DYNAMIC = "drill.exec.udf.disable_dynamic";

  /**
   * Local temporary directory is used as base for temporary storage of Dynamic UDF jars.
   */
  String DRILL_TMP_DIR = "drill.tmp-dir";

  /**
   * Temporary tables can be created ONLY in default temporary workspace.
   */
  String DEFAULT_TEMPORARY_WORKSPACE = "drill.exec.default_temporary_workspace";

  String OUTPUT_FORMAT_OPTION = "store.format";
  OptionValidator OUTPUT_FORMAT_VALIDATOR = new StringValidator(OUTPUT_FORMAT_OPTION, "parquet");
  String PARQUET_BLOCK_SIZE = "store.parquet.block-size";
  OptionValidator PARQUET_BLOCK_SIZE_VALIDATOR = new LongValidator(PARQUET_BLOCK_SIZE, 512*1024*1024);
  String PARQUET_PAGE_SIZE = "store.parquet.page-size";
  OptionValidator PARQUET_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_PAGE_SIZE, 1024*1024);
  String PARQUET_DICT_PAGE_SIZE = "store.parquet.dictionary.page-size";
  OptionValidator PARQUET_DICT_PAGE_SIZE_VALIDATOR = new LongValidator(PARQUET_DICT_PAGE_SIZE, 1024*1024);
  String PARQUET_WRITER_COMPRESSION_TYPE = "store.parquet.compression";
  OptionValidator PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR = new EnumeratedStringValidator(
      PARQUET_WRITER_COMPRESSION_TYPE, "snappy", "snappy", "gzip", "none");
  String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING = "store.parquet.enable_dictionary_encoding";
  OptionValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR = new BooleanValidator(
      PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, false);

  String PARQUET_VECTOR_FILL_THRESHOLD = "store.parquet.vector_fill_threshold";
  OptionValidator PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR = new PositiveLongValidator(PARQUET_VECTOR_FILL_THRESHOLD, 99l, 85l);
  String PARQUET_VECTOR_FILL_CHECK_THRESHOLD = "store.parquet.vector_fill_check_threshold";
  OptionValidator PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR = new PositiveLongValidator(PARQUET_VECTOR_FILL_CHECK_THRESHOLD, 100l, 10l);
  String PARQUET_NEW_RECORD_READER = "store.parquet.use_new_reader";
  OptionValidator PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR = new BooleanValidator(PARQUET_NEW_RECORD_READER, false);
  String PARQUET_READER_INT96_AS_TIMESTAMP = "store.parquet.reader.int96_as_timestamp";
  OptionValidator PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR = new BooleanValidator(PARQUET_READER_INT96_AS_TIMESTAMP, false);

  String PARQUET_PAGEREADER_ASYNC = "store.parquet.reader.pagereader.async";
  OptionValidator PARQUET_PAGEREADER_ASYNC_VALIDATOR = new BooleanValidator(PARQUET_PAGEREADER_ASYNC, true);

  // Number of pages the Async Parquet page reader will read before blocking
  String PARQUET_PAGEREADER_QUEUE_SIZE = "store.parquet.reader.pagereader.queuesize";
  OptionValidator PARQUET_PAGEREADER_QUEUE_SIZE_VALIDATOR = new  PositiveLongValidator(PARQUET_PAGEREADER_QUEUE_SIZE, Integer.MAX_VALUE, 2);

  String PARQUET_PAGEREADER_ENFORCETOTALSIZE = "store.parquet.reader.pagereader.enforceTotalSize";
  OptionValidator PARQUET_PAGEREADER_ENFORCETOTALSIZE_VALIDATOR = new BooleanValidator(PARQUET_PAGEREADER_ENFORCETOTALSIZE, false);

  String PARQUET_COLUMNREADER_ASYNC = "store.parquet.reader.columnreader.async";
  OptionValidator PARQUET_COLUMNREADER_ASYNC_VALIDATOR = new BooleanValidator(PARQUET_COLUMNREADER_ASYNC, false);

  // Use a buffering reader for parquet page reader
  String PARQUET_PAGEREADER_USE_BUFFERED_READ = "store.parquet.reader.pagereader.bufferedread";
  OptionValidator PARQUET_PAGEREADER_USE_BUFFERED_READ_VALIDATOR = new  BooleanValidator(PARQUET_PAGEREADER_USE_BUFFERED_READ, true);

  // Size in MiB of the buffer the Parquet page reader will use to read from disk. Default is 1 MiB
  String PARQUET_PAGEREADER_BUFFER_SIZE = "store.parquet.reader.pagereader.buffersize";
  OptionValidator PARQUET_PAGEREADER_BUFFER_SIZE_VALIDATOR = new  LongValidator(PARQUET_PAGEREADER_BUFFER_SIZE, 1*1024*1024);

  // try to use fadvise if available
  String PARQUET_PAGEREADER_USE_FADVISE = "store.parquet.reader.pagereader.usefadvise";
  OptionValidator PARQUET_PAGEREADER_USE_FADVISE_VALIDATOR = new  BooleanValidator(PARQUET_PAGEREADER_USE_FADVISE, false);

  OptionValidator COMPILE_SCALAR_REPLACEMENT = new BooleanValidator("exec.compile.scalar_replacement", false);

  String JSON_ALL_TEXT_MODE = "store.json.all_text_mode";
  BooleanValidator JSON_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(JSON_ALL_TEXT_MODE, false);
  BooleanValidator JSON_EXTENDED_TYPES = new BooleanValidator("store.json.extended_types", false);
  BooleanValidator JSON_WRITER_UGLIFY = new BooleanValidator("store.json.writer.uglify", false);
  BooleanValidator JSON_WRITER_SKIPNULLFIELDS = new BooleanValidator("store.json.writer.skip_null_fields", true);
  String JSON_READER_SKIP_INVALID_RECORDS_FLAG = "store.json.reader.skip_invalid_records";
  BooleanValidator JSON_SKIP_MALFORMED_RECORDS_VALIDATOR = new BooleanValidator(JSON_READER_SKIP_INVALID_RECORDS_FLAG, false);
  String JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG = "store.json.reader.print_skipped_invalid_record_number";
  BooleanValidator JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR = new BooleanValidator(JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG, false);
  DoubleValidator TEXT_ESTIMATED_ROW_SIZE = new RangeDoubleValidator(
      "store.text.estimated_row_size_bytes", 1, Long.MAX_VALUE, 100.0);

  /**
   * The column label (for directory levels) in results when querying files in a directory
   * E.g.  labels: dir0   dir1
   *    structure: foo
   *                |-    bar  -  a.parquet
   *                |-    baz  -  b.parquet
   */
  String FILESYSTEM_PARTITION_COLUMN_LABEL = "drill.exec.storage.file.partition.column.label";
  OptionValidator FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR = new StringValidator(FILESYSTEM_PARTITION_COLUMN_LABEL, "dir");

  /**
   * Implicit file columns
   */
  String IMPLICIT_FILENAME_COLUMN_LABEL = "drill.exec.storage.implicit.filename.column.label";
  OptionValidator IMPLICIT_FILENAME_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_FILENAME_COLUMN_LABEL, "filename");
  String IMPLICIT_SUFFIX_COLUMN_LABEL = "drill.exec.storage.implicit.suffix.column.label";
  OptionValidator IMPLICIT_SUFFIX_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_SUFFIX_COLUMN_LABEL, "suffix");
  String IMPLICIT_FQN_COLUMN_LABEL = "drill.exec.storage.implicit.fqn.column.label";
  OptionValidator IMPLICIT_FQN_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_FQN_COLUMN_LABEL, "fqn");
  String IMPLICIT_FILEPATH_COLUMN_LABEL = "drill.exec.storage.implicit.filepath.column.label";
  OptionValidator IMPLICIT_FILEPATH_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_FILEPATH_COLUMN_LABEL, "filepath");

  String JSON_READ_NUMBERS_AS_DOUBLE = "store.json.read_numbers_as_double";
  BooleanValidator JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(JSON_READ_NUMBERS_AS_DOUBLE, false);

  String MONGO_ALL_TEXT_MODE = "store.mongo.all_text_mode";
  OptionValidator MONGO_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(MONGO_ALL_TEXT_MODE, false);
  String MONGO_READER_READ_NUMBERS_AS_DOUBLE = "store.mongo.read_numbers_as_double";
  OptionValidator MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(MONGO_READER_READ_NUMBERS_AS_DOUBLE, false);
  String MONGO_BSON_RECORD_READER = "store.mongo.bson.record.reader";
  OptionValidator MONGO_BSON_RECORD_READER_VALIDATOR = new BooleanValidator(MONGO_BSON_RECORD_READER, true);

  BooleanValidator ENABLE_UNION_TYPE = new BooleanValidator("exec.enable_union_type", false);

  // TODO: We need to add a feature that enables storage plugins to add their own options. Currently we have to declare
  // in core which is not right. Move this option and above two mongo plugin related options once we have the feature.
  String HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS = "store.hive.optimize_scan_with_native_readers";
  OptionValidator HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR =
      new BooleanValidator(HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS, false);

  String SLICE_TARGET = "planner.slice_target";
  long SLICE_TARGET_DEFAULT = 100000l;
  PositiveLongValidator SLICE_TARGET_OPTION = new PositiveLongValidator(SLICE_TARGET, Long.MAX_VALUE,
      SLICE_TARGET_DEFAULT);

  String CAST_TO_NULLABLE_NUMERIC = "drill.exec.functions.cast_empty_string_to_null";
  BooleanValidator CAST_TO_NULLABLE_NUMERIC_OPTION = new BooleanValidator(CAST_TO_NULLABLE_NUMERIC, false);

  /**
   * HashTable runtime settings
   */
  String MIN_HASH_TABLE_SIZE_KEY = "exec.min_hash_table_size";
  PositiveLongValidator MIN_HASH_TABLE_SIZE = new PositiveLongValidator(MIN_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.DEFAULT_INITIAL_CAPACITY);
  String MAX_HASH_TABLE_SIZE_KEY = "exec.max_hash_table_size";
  PositiveLongValidator MAX_HASH_TABLE_SIZE = new PositiveLongValidator(MAX_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY, HashTable.MAXIMUM_CAPACITY);

  /**
   * Limits the maximum level of parallelization to this factor time the number of Drillbits
   */
  String MAX_WIDTH_PER_NODE_KEY = "planner.width.max_per_node";
  OptionValidator MAX_WIDTH_PER_NODE = new PositiveLongValidator(MAX_WIDTH_PER_NODE_KEY, Integer.MAX_VALUE, (long) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.70));

  /**
   * The maximum level or parallelization any stage of the query can do. Note that while this
   * might be the number of active Drillbits, realistically, this could be well beyond that
   * number of we want to do things like speed results return.
   */
  String MAX_WIDTH_GLOBAL_KEY = "planner.width.max_per_query";
  OptionValidator MAX_WIDTH_GLOBAL = new PositiveLongValidator(MAX_WIDTH_GLOBAL_KEY, Integer.MAX_VALUE, 1000);

  /**
   * Factor by which a node with endpoint affinity will be favored while creating assignment
   */
  String AFFINITY_FACTOR_KEY = "planner.affinity_factor";
  OptionValidator AFFINITY_FACTOR = new DoubleValidator(AFFINITY_FACTOR_KEY, 1.2d);

  String EARLY_LIMIT0_OPT_KEY = "planner.enable_limit0_optimization";
  BooleanValidator EARLY_LIMIT0_OPT = new BooleanValidator(EARLY_LIMIT0_OPT_KEY, false);

  String ENABLE_MEMORY_ESTIMATION_KEY = "planner.memory.enable_memory_estimation";
  OptionValidator ENABLE_MEMORY_ESTIMATION = new BooleanValidator(ENABLE_MEMORY_ESTIMATION_KEY, false);

  /**
   * Maximum query memory per node (in MB). Re-plan with cheaper operators if memory estimation exceeds this limit.
   * <p/>
   * DEFAULT: 2048 MB
   */
  String MAX_QUERY_MEMORY_PER_NODE_KEY = "planner.memory.max_query_memory_per_node";
  LongValidator MAX_QUERY_MEMORY_PER_NODE = new RangeLongValidator(
      MAX_QUERY_MEMORY_PER_NODE_KEY, 1024 * 1024, Long.MAX_VALUE, 2 * 1024 * 1024 * 1024L);

  /**
   * Extra query memory per node for non-blocking operators.
   * NOTE: This option is currently used only for memory estimation.
   * <p/>
   * DEFAULT: 64 MB
   * MAXIMUM: 2048 MB
   */
  String NON_BLOCKING_OPERATORS_MEMORY_KEY = "planner.memory.non_blocking_operators_memory";
  OptionValidator NON_BLOCKING_OPERATORS_MEMORY = new PowerOfTwoLongValidator(
    NON_BLOCKING_OPERATORS_MEMORY_KEY, 1 << 11, 1 << 6);

  String HASH_JOIN_TABLE_FACTOR_KEY = "planner.memory.hash_join_table_factor";
  OptionValidator HASH_JOIN_TABLE_FACTOR = new DoubleValidator(HASH_JOIN_TABLE_FACTOR_KEY, 1.1d);

  String HASH_AGG_TABLE_FACTOR_KEY = "planner.memory.hash_agg_table_factor";
  OptionValidator HASH_AGG_TABLE_FACTOR = new DoubleValidator(HASH_AGG_TABLE_FACTOR_KEY, 1.1d);

  String AVERAGE_FIELD_WIDTH_KEY = "planner.memory.average_field_width";
  OptionValidator AVERAGE_FIELD_WIDTH = new PositiveLongValidator(AVERAGE_FIELD_WIDTH_KEY, Long.MAX_VALUE, 8);

  BooleanValidator ENABLE_QUEUE = new BooleanValidator("exec.queue.enable", false);
  LongValidator LARGE_QUEUE_SIZE = new PositiveLongValidator("exec.queue.large", 1000, 10);
  LongValidator SMALL_QUEUE_SIZE = new PositiveLongValidator("exec.queue.small", 100000, 100);
  LongValidator QUEUE_THRESHOLD_SIZE = new PositiveLongValidator("exec.queue.threshold",
      Long.MAX_VALUE, 30000000);
  LongValidator QUEUE_TIMEOUT = new PositiveLongValidator("exec.queue.timeout_millis",
      Long.MAX_VALUE, 60 * 1000 * 5);

  String ENABLE_VERBOSE_ERRORS_KEY = "exec.errors.verbose";
  OptionValidator ENABLE_VERBOSE_ERRORS = new BooleanValidator(ENABLE_VERBOSE_ERRORS_KEY, false);

  String ENABLE_NEW_TEXT_READER_KEY = "exec.storage.enable_new_text_reader";
  OptionValidator ENABLE_NEW_TEXT_READER = new BooleanValidator(ENABLE_NEW_TEXT_READER_KEY, true);

  String BOOTSTRAP_STORAGE_PLUGINS_FILE = "bootstrap-storage-plugins.json";

  String DRILL_SYS_FILE_SUFFIX = ".sys.drill";

  String ENABLE_WINDOW_FUNCTIONS = "window.enable";
  OptionValidator ENABLE_WINDOW_FUNCTIONS_VALIDATOR = new BooleanValidator(ENABLE_WINDOW_FUNCTIONS, true);

  String DRILLBIT_CONTROL_INJECTIONS = "drill.exec.testing.controls";
  OptionValidator DRILLBIT_CONTROLS_VALIDATOR =
    new ExecutionControls.ControlsOptionValidator(DRILLBIT_CONTROL_INJECTIONS, ExecutionControls.DEFAULT_CONTROLS, 1);

  String NEW_VIEW_DEFAULT_PERMS_KEY = "new_view_default_permissions";
  OptionValidator NEW_VIEW_DEFAULT_PERMS_VALIDATOR =
      new StringValidator(NEW_VIEW_DEFAULT_PERMS_KEY, "700");

  String CTAS_PARTITIONING_HASH_DISTRIBUTE = "store.partition.hash_distribute";
  BooleanValidator CTAS_PARTITIONING_HASH_DISTRIBUTE_VALIDATOR = new BooleanValidator(CTAS_PARTITIONING_HASH_DISTRIBUTE, false);

  String ENABLE_BULK_LOAD_TABLE_LIST_KEY = "exec.enable_bulk_load_table_list";
  BooleanValidator ENABLE_BULK_LOAD_TABLE_LIST = new BooleanValidator(ENABLE_BULK_LOAD_TABLE_LIST_KEY, false);

  /**
   * When getting Hive Table information with exec.enable_bulk_load_table_list set to true,
   * use the exec.bulk_load_table_list.bulk_size to determine how many tables to fetch from HiveMetaStore
   * at a time. (The number of tables can get to be quite large.)
   */
  String BULK_LOAD_TABLE_LIST_BULK_SIZE_KEY = "exec.bulk_load_table_list.bulk_size";
  PositiveLongValidator BULK_LOAD_TABLE_LIST_BULK_SIZE = new PositiveLongValidator(BULK_LOAD_TABLE_LIST_BULK_SIZE_KEY, Integer.MAX_VALUE, 1000);

  /**
   * Option whose value is a comma separated list of admin usernames. Admin users are users who have special privileges
   * such as changing system options.
   */
  String ADMIN_USERS_KEY = "security.admin.users";
  StringValidator ADMIN_USERS_VALIDATOR = new StringValidator(ADMIN_USERS_KEY, ImpersonationUtil.getProcessUserName(), true);

  /**
   * Option whose value is a comma separated list of admin usergroups.
   */
  String ADMIN_USER_GROUPS_KEY = "security.admin.user_groups";
  StringValidator ADMIN_USER_GROUPS_VALIDATOR = new StringValidator(ADMIN_USER_GROUPS_KEY, "", true);
  /**
   * Option whose value is a string representing list of inbound impersonation policies.
   *
   * Impersonation policy format:
   * [
   *   {
   *    proxy_principals : { users : [“...”], groups : [“...”] },
   *    target_principals : { users : [“...”], groups : [“...”] }
   *   },
   *   ...
   * ]
   */
  String IMPERSONATION_POLICIES_KEY = "exec.impersonation.inbound_policies";
  StringValidator IMPERSONATION_POLICY_VALIDATOR =
      new InboundImpersonationManager.InboundImpersonationPolicyValidator(IMPERSONATION_POLICIES_KEY, "[]");

  /**
   * Web settings
   */
  String WEB_LOGS_MAX_LINES = "web.logs.max_lines";
  OptionValidator WEB_LOGS_MAX_LINES_VALIDATOR = new PositiveLongValidator(WEB_LOGS_MAX_LINES, Integer.MAX_VALUE, 10000);

  String CODE_GEN_EXP_IN_METHOD_SIZE = "exec.java.compiler.exp_in_method_size";
  LongValidator CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR = new LongValidator(CODE_GEN_EXP_IN_METHOD_SIZE, 50);

  /**
   * Timeout for create prepare statement request. If the request exceeds this timeout, then request is timed out.
   * Default value is 10mins.
   */
  String CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS = "prepare.statement.create_timeout_ms";
  OptionValidator CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS_VALIDATOR =
      new PositiveLongValidator(CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS, Integer.MAX_VALUE, 10000);

  String DYNAMIC_UDF_SUPPORT_ENABLED = "exec.udf.enable_dynamic_support";
  BooleanValidator DYNAMIC_UDF_SUPPORT_ENABLED_VALIDATOR = new BooleanValidator(DYNAMIC_UDF_SUPPORT_ENABLED, true, true);

  /**
   * Option to save query profiles. If false, no query profile will be saved
   * for any query.
   */
  String ENABLE_QUERY_PROFILE_OPTION = "exec.query_profile.save";
  BooleanValidator ENABLE_QUERY_PROFILE_VALIDATOR = new BooleanValidator(
      ENABLE_QUERY_PROFILE_OPTION, true, false);

  /**
   * Profiles are normally written after the last client message to reduce latency.
   * When running tests, however, we want the profile written <i>before</i> the
   * return so that the client can immediately read the profile for test
   * verification.
   */
  String QUERY_PROFILE_DEBUG_OPTION = "exec.query_profile.debug_mode";
  BooleanValidator QUERY_PROFILE_DEBUG_VALIDATOR = new BooleanValidator(
      QUERY_PROFILE_DEBUG_OPTION, false, false);

  String USE_DYNAMIC_UDFS_KEY = "exec.udf.use_dynamic";
  BooleanValidator USE_DYNAMIC_UDFS = new BooleanValidator(USE_DYNAMIC_UDFS_KEY, true);


  String QUERY_TRANSIENT_STATE_UPDATE_KEY = "exec.query.progress.update";
  BooleanValidator QUERY_TRANSIENT_STATE_UPDATE = new BooleanValidator(QUERY_TRANSIENT_STATE_UPDATE_KEY, true);

  String PERSISTENT_TABLE_UMASK = "exec.persistent_table.umask";
  StringValidator PERSISTENT_TABLE_UMASK_VALIDATOR = new StringValidator(PERSISTENT_TABLE_UMASK, "002");

}
