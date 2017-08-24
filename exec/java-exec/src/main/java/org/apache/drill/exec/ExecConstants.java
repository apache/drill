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
import org.apache.drill.exec.server.options.TypeValidators.MaxWidthValidator;
import org.apache.drill.exec.server.options.TypeValidators.PositiveLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.PowerOfTwoLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeDoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;
import org.apache.drill.exec.server.options.TypeValidators.MaxWidthValidator;
import org.apache.drill.exec.server.options.TypeValidators.AdminUsersValidator;
import org.apache.drill.exec.server.options.TypeValidators.AdminUserGroupsValidator;
import org.apache.drill.exec.testing.ExecutionControls;

public final class ExecConstants {
  private ExecConstants() {
    // Don't allow instantiation
  }

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
  public static final String INITIAL_DATA_PORT = "drill.exec.rpc.bit.server.dataport";
  public static final String BIT_RPC_TIMEOUT = "drill.exec.rpc.bit.timeout";
  public static final String INITIAL_USER_PORT = "drill.exec.rpc.user.server.port";
  public static final String USER_RPC_TIMEOUT = "drill.exec.rpc.user.timeout";
  public static final String METRICS_CONTEXT_NAME = "drill.exec.metrics.context";
  public static final String USE_IP_ADDRESS = "drill.exec.rpc.use.ip";
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

  // Spill boot-time Options common to all spilling operators
  // (Each individual operator may override the common options)

  public static final String SPILL_FILESYSTEM = "drill.exec.spill.fs";
  public static final String SPILL_DIRS = "drill.exec.spill.directories";

  // External Sort Boot configuration

  public static final String EXTERNAL_SORT_TARGET_SPILL_BATCH_SIZE = "drill.exec.sort.external.spill.batch.size";
  public static final String EXTERNAL_SORT_SPILL_GROUP_SIZE = "drill.exec.sort.external.spill.group.size";
  public static final String EXTERNAL_SORT_SPILL_THRESHOLD = "drill.exec.sort.external.spill.threshold";
  public static final String EXTERNAL_SORT_SPILL_DIRS = "drill.exec.sort.external.spill.directories";
  public static final String EXTERNAL_SORT_SPILL_FILESYSTEM = "drill.exec.sort.external.spill.fs";
  public static final String EXTERNAL_SORT_SPILL_FILE_SIZE = "drill.exec.sort.external.spill.file_size";
  public static final String EXTERNAL_SORT_MSORT_MAX_BATCHSIZE = "drill.exec.sort.external.msort.batch.maxsize";
  public static final String EXTERNAL_SORT_DISABLE_MANAGED = "drill.exec.sort.external.disable_managed";
  public static final String EXTERNAL_SORT_MERGE_LIMIT = "drill.exec.sort.external.merge_limit";
  public static final String EXTERNAL_SORT_SPILL_BATCH_SIZE = "drill.exec.sort.external.spill.spill_batch_size";
  public static final String EXTERNAL_SORT_MERGE_BATCH_SIZE = "drill.exec.sort.external.spill.merge_batch_size";
  public static final String EXTERNAL_SORT_MAX_MEMORY = "drill.exec.sort.external.mem_limit";
  public static final String EXTERNAL_SORT_BATCH_LIMIT = "drill.exec.sort.external.batch_limit";

  // External Sort Runtime options

  public static final BooleanValidator EXTERNAL_SORT_DISABLE_MANAGED_OPTION = new BooleanValidator("exec.sort.disable_managed");

  // Hash Aggregate Options
  public static final String HASHAGG_NUM_PARTITIONS_KEY = "exec.hashagg.num_partitions";
  public static final LongValidator HASHAGG_NUM_PARTITIONS_VALIDATOR = new RangeLongValidator(HASHAGG_NUM_PARTITIONS_KEY, 1, 128); // 1 means - no spilling
  public static final String HASHAGG_MAX_MEMORY_KEY = "exec.hashagg.mem_limit";
  public static final LongValidator HASHAGG_MAX_MEMORY_VALIDATOR = new RangeLongValidator(HASHAGG_MAX_MEMORY_KEY, 0, Integer.MAX_VALUE);
  // min batches is used for tuning (each partition needs so many batches when planning the number of partitions,
  // or reserve this number when calculating whether the remaining available memory is too small and requires a spill.)
  // Low value may OOM (e.g., when incoming rows become wider), higher values use fewer partitions but are safer
  public static final String HASHAGG_MIN_BATCHES_PER_PARTITION_KEY = "exec.hashagg.min_batches_per_partition";
  public static final LongValidator HASHAGG_MIN_BATCHES_PER_PARTITION_VALIDATOR = new RangeLongValidator(HASHAGG_MIN_BATCHES_PER_PARTITION_KEY, 1, 5);
  // Can be turned off mainly for testing. Memory prediction is used to decide on when to spill to disk; with this option off,
  // spill would be triggered only by another mechanism -- "catch OOMs and then spill".
  public static final String HASHAGG_USE_MEMORY_PREDICTION_KEY = "exec.hashagg.use_memory_prediction";
  public static final BooleanValidator HASHAGG_USE_MEMORY_PREDICTION_VALIDATOR = new BooleanValidator(HASHAGG_USE_MEMORY_PREDICTION_KEY);

  public static final String HASHAGG_SPILL_DIRS = "drill.exec.hashagg.spill.directories";
  public static final String HASHAGG_SPILL_FILESYSTEM = "drill.exec.hashagg.spill.fs";
  public static final String HASHAGG_FALLBACK_ENABLED_KEY = "drill.exec.hashagg.fallback.enabled";
  public static final BooleanValidator HASHAGG_FALLBACK_ENABLED_VALIDATOR = new BooleanValidator(HASHAGG_FALLBACK_ENABLED_KEY);

  public static final String SSL_PROVIDER = "drill.exec.ssl.provider"; // valid values are "JDK", "OPENSSL" // default JDK
  public static final String SSL_PROTOCOL = "drill.exec.ssl.protocol"; // valid values are SSL, SSLV2, SSLV3, TLS, TLSV1, TLSv1.1, TLSv1.2(default)
  public static final String SSL_KEYSTORE_TYPE = "drill.exec.ssl.keyStoreType";
  public static final String SSL_KEYSTORE_PATH = "drill.exec.ssl.keyStorePath";     // path to keystore. default : $JRE_HOME/lib/security/keystore.jks
  public static final String SSL_KEYSTORE_PASSWORD = "drill.exec.ssl.keyStorePassword"; // default: changeit
  public static final String SSL_KEY_PASSWORD = "drill.exec.ssl.keyPassword"; //
  public static final String SSL_TRUSTSTORE_TYPE = "drill.exec.ssl.trustStoreType"; // valid values are jks(default), jceks, pkcs12
  public static final String SSL_TRUSTSTORE_PATH = "drill.exec.ssl.trustStorePath"; // path to keystore. default : $JRE_HOME/lib/security/cacerts.jks
  public static final String SSL_TRUSTSTORE_PASSWORD = "drill.exec.ssl.trustStorePassword"; // default: changeit
  public static final String SSL_USE_HADOOP_CONF = "drill.exec.ssl.useHadoopConfig"; // Initialize ssl params from hadoop if not provided by drill. default: true
  public static final String SSL_HANDSHAKE_TIMEOUT = "drill.exec.security.user.encryption.ssl.handshakeTimeout"; // Default 10 seconds

  public static final String TEXT_LINE_READER_BATCH_SIZE = "drill.exec.storage.file.text.batch.size";
  public static final String TEXT_LINE_READER_BUFFER_SIZE = "drill.exec.storage.file.text.buffer.size";
  public static final String HAZELCAST_SUBNETS = "drill.exec.cache.hazel.subnets";
  public static final String HTTP_ENABLE = "drill.exec.http.enabled";
  public static final String HTTP_MAX_PROFILES = "drill.exec.http.max_profiles";
  public static final String HTTP_PORT = "drill.exec.http.port";
  public static final String HTTP_PORT_HUNT = "drill.exec.http.porthunt";
  public static final String HTTP_ENABLE_SSL = "drill.exec.http.ssl_enabled";
  public static final String HTTP_CORS_ENABLED = "drill.exec.http.cors.enabled";
  public static final String HTTP_CORS_ALLOWED_ORIGINS = "drill.exec.http.cors.allowedOrigins";
  public static final String HTTP_CORS_ALLOWED_METHODS = "drill.exec.http.cors.allowedMethods";
  public static final String HTTP_CORS_ALLOWED_HEADERS = "drill.exec.http.cors.allowedHeaders";
  public static final String HTTP_CORS_CREDENTIALS = "drill.exec.http.cors.credentials";
  public static final String HTTP_SESSION_MEMORY_RESERVATION = "drill.exec.http.session.memory.reservation";
  public static final String HTTP_SESSION_MEMORY_MAXIMUM = "drill.exec.http.session.memory.maximum";
  public static final String HTTP_SESSION_MAX_IDLE_SECS = "drill.exec.http.session_max_idle_secs";
  public static final String HTTP_KEYSTORE_PATH = SSL_KEYSTORE_PATH;
  public static final String HTTP_KEYSTORE_PASSWORD = SSL_KEYSTORE_PASSWORD;
  public static final String HTTP_TRUSTSTORE_PATH = SSL_TRUSTSTORE_PATH;
  public static final String HTTP_TRUSTSTORE_PASSWORD = SSL_TRUSTSTORE_PASSWORD;
  public static final String SYS_STORE_PROVIDER_CLASS = "drill.exec.sys.store.provider.class";
  public static final String SYS_STORE_PROVIDER_LOCAL_PATH = "drill.exec.sys.store.provider.local.path";
  public static final String SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE = "drill.exec.sys.store.provider.local.write";
  public static final String PROFILES_STORE_INMEMORY = "drill.exec.profiles.store.inmemory";
  public static final String PROFILES_STORE_CAPACITY = "drill.exec.profiles.store.capacity";
  public static final String IMPERSONATION_ENABLED = "drill.exec.impersonation.enabled";
  public static final String IMPERSONATION_MAX_CHAINED_USER_HOPS = "drill.exec.impersonation.max_chained_user_hops";
  public static final String AUTHENTICATION_MECHANISMS = "drill.exec.security.auth.mechanisms";
  public static final String USER_AUTHENTICATION_ENABLED = "drill.exec.security.user.auth.enabled";
  public static final String USER_AUTHENTICATOR_IMPL = "drill.exec.security.user.auth.impl";
  public static final String PAM_AUTHENTICATOR_PROFILES = "drill.exec.security.user.auth.pam_profiles";
  public static final String BIT_AUTHENTICATION_ENABLED = "drill.exec.security.bit.auth.enabled";
  public static final String BIT_AUTHENTICATION_MECHANISM = "drill.exec.security.bit.auth.mechanism";
  public static final String USE_LOGIN_PRINCIPAL = "drill.exec.security.bit.auth.use_login_principal";
  public static final String USER_ENCRYPTION_SASL_ENABLED = "drill.exec.security.user.encryption.sasl.enabled";
  public static final String USER_ENCRYPTION_SASL_MAX_WRAPPED_SIZE = "drill.exec.security.user.encryption.sasl.max_wrapped_size";

  public static final String USER_SSL_ENABLED = "drill.exec.security.user.encryption.ssl.enabled";
  public static final String BIT_ENCRYPTION_SASL_ENABLED = "drill.exec.security.bit.encryption.sasl.enabled";
  public static final String BIT_ENCRYPTION_SASL_MAX_WRAPPED_SIZE = "drill.exec.security.bit.encryption.sasl.max_wrapped_size";

  /** Size of JDBC batch queue (in batches) above which throttling begins. */
  public static final String JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD =
      "drill.jdbc.batch_queue_throttling_threshold";
  // Thread pool size for scan threads. Used by the Parquet scan.
  public static final String SCAN_THREADPOOL_SIZE = "drill.exec.scan.threadpool_size";
  // The size of the thread pool used by a scan to decode the data. Used by Parquet
  public static final String SCAN_DECODE_THREADPOOL_SIZE = "drill.exec.scan.decode_threadpool_size";

  /**
   * Currently if a query is cancelled, but one of the fragments reports the status as FAILED instead of CANCELLED or
   * FINISHED we report the query result as CANCELLED by swallowing the failures occurred in fragments. This BOOT
   * setting allows the user to see the query status as failure. Useful for developers/testers.
   */
  public static final String RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS = "drill.exec.debug.return_error_for_failure_in_cancelled_fragments";

  public static final String CLIENT_SUPPORT_COMPLEX_TYPES = "drill.client.supports-complex-types";

  /**
   * Configuration properties connected with dynamic UDFs support
   */
  public static final String UDF_RETRY_ATTEMPTS = "drill.exec.udf.retry-attempts";
  public static final String UDF_DIRECTORY_LOCAL = "drill.exec.udf.directory.local";
  public static final String UDF_DIRECTORY_FS = "drill.exec.udf.directory.fs";
  public static final String UDF_DIRECTORY_ROOT = "drill.exec.udf.directory.root";
  public static final String UDF_DIRECTORY_STAGING = "drill.exec.udf.directory.staging";
  public static final String UDF_DIRECTORY_REGISTRY = "drill.exec.udf.directory.registry";
  public static final String UDF_DIRECTORY_TMP = "drill.exec.udf.directory.tmp";
  public static final String UDF_DISABLE_DYNAMIC = "drill.exec.udf.disable_dynamic";

  /**
   * Local temporary directory is used as base for temporary storage of Dynamic UDF jars.
   */
  public static final String DRILL_TMP_DIR = "drill.tmp-dir";

  /**
   * Temporary tables can be created ONLY in default temporary workspace.
   */
  public static final String DEFAULT_TEMPORARY_WORKSPACE = "drill.exec.default_temporary_workspace";

  public static final String OUTPUT_FORMAT_OPTION = "store.format";
  public static final OptionValidator OUTPUT_FORMAT_VALIDATOR = new StringValidator(OUTPUT_FORMAT_OPTION);
  public static final String PARQUET_BLOCK_SIZE = "store.parquet.block-size";
  public static final String PARQUET_WRITER_USE_SINGLE_FS_BLOCK = "store.parquet.writer.use_single_fs_block";
  public static final OptionValidator PARQUET_WRITER_USE_SINGLE_FS_BLOCK_VALIDATOR = new BooleanValidator(
    PARQUET_WRITER_USE_SINGLE_FS_BLOCK);
  public static final OptionValidator PARQUET_BLOCK_SIZE_VALIDATOR = new PositiveLongValidator(PARQUET_BLOCK_SIZE, Integer.MAX_VALUE);
  public static final String PARQUET_PAGE_SIZE = "store.parquet.page-size";
  public static final OptionValidator PARQUET_PAGE_SIZE_VALIDATOR = new PositiveLongValidator(PARQUET_PAGE_SIZE, Integer.MAX_VALUE);
  public static final String PARQUET_DICT_PAGE_SIZE = "store.parquet.dictionary.page-size";
  public static final OptionValidator PARQUET_DICT_PAGE_SIZE_VALIDATOR = new PositiveLongValidator(PARQUET_DICT_PAGE_SIZE, Integer.MAX_VALUE);
  public static final String PARQUET_WRITER_COMPRESSION_TYPE = "store.parquet.compression";
  public static final OptionValidator PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR = new EnumeratedStringValidator(
      PARQUET_WRITER_COMPRESSION_TYPE, "snappy", "gzip", "none");
  public static final String PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING = "store.parquet.enable_dictionary_encoding";
  public static final OptionValidator PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR = new BooleanValidator(
      PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING);

  public static final String PARQUET_VECTOR_FILL_THRESHOLD = "store.parquet.vector_fill_threshold";
  public static final OptionValidator PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR = new PositiveLongValidator(PARQUET_VECTOR_FILL_THRESHOLD, 99l);
  public static final String PARQUET_VECTOR_FILL_CHECK_THRESHOLD = "store.parquet.vector_fill_check_threshold";
  public static final OptionValidator PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR = new PositiveLongValidator(PARQUET_VECTOR_FILL_CHECK_THRESHOLD, 100l);
  public static final String PARQUET_NEW_RECORD_READER = "store.parquet.use_new_reader";
  public static final OptionValidator PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR = new BooleanValidator(PARQUET_NEW_RECORD_READER);
  public static final String PARQUET_READER_INT96_AS_TIMESTAMP = "store.parquet.reader.int96_as_timestamp";
  public static final OptionValidator PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR = new BooleanValidator(PARQUET_READER_INT96_AS_TIMESTAMP);

  public static final String PARQUET_PAGEREADER_ASYNC = "store.parquet.reader.pagereader.async";
  public static final OptionValidator PARQUET_PAGEREADER_ASYNC_VALIDATOR = new BooleanValidator(PARQUET_PAGEREADER_ASYNC);

  // Number of pages the Async Parquet page reader will read before blocking
  public static final String PARQUET_PAGEREADER_QUEUE_SIZE = "store.parquet.reader.pagereader.queuesize";
  public static final OptionValidator PARQUET_PAGEREADER_QUEUE_SIZE_VALIDATOR = new  PositiveLongValidator(PARQUET_PAGEREADER_QUEUE_SIZE, Integer.MAX_VALUE);

  public static final String PARQUET_PAGEREADER_ENFORCETOTALSIZE = "store.parquet.reader.pagereader.enforceTotalSize";
  public static final OptionValidator PARQUET_PAGEREADER_ENFORCETOTALSIZE_VALIDATOR = new BooleanValidator(PARQUET_PAGEREADER_ENFORCETOTALSIZE);

  public static final String PARQUET_COLUMNREADER_ASYNC = "store.parquet.reader.columnreader.async";
  public static final OptionValidator PARQUET_COLUMNREADER_ASYNC_VALIDATOR = new BooleanValidator(PARQUET_COLUMNREADER_ASYNC);

  // Use a buffering reader for parquet page reader
  public static final String PARQUET_PAGEREADER_USE_BUFFERED_READ = "store.parquet.reader.pagereader.bufferedread";
  public static final OptionValidator PARQUET_PAGEREADER_USE_BUFFERED_READ_VALIDATOR = new  BooleanValidator(PARQUET_PAGEREADER_USE_BUFFERED_READ);

  // Size in MiB of the buffer the Parquet page reader will use to read from disk. Default is 1 MiB
  public static final String PARQUET_PAGEREADER_BUFFER_SIZE = "store.parquet.reader.pagereader.buffersize";
  public static final OptionValidator PARQUET_PAGEREADER_BUFFER_SIZE_VALIDATOR = new  LongValidator(PARQUET_PAGEREADER_BUFFER_SIZE);

  // try to use fadvise if available
  public static final String PARQUET_PAGEREADER_USE_FADVISE = "store.parquet.reader.pagereader.usefadvise";
  public static final OptionValidator PARQUET_PAGEREADER_USE_FADVISE_VALIDATOR = new  BooleanValidator(PARQUET_PAGEREADER_USE_FADVISE);

  public static final OptionValidator COMPILE_SCALAR_REPLACEMENT = new BooleanValidator("exec.compile.scalar_replacement");

  public static final String JSON_ALL_TEXT_MODE = "store.json.all_text_mode";
  public static final BooleanValidator JSON_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(JSON_ALL_TEXT_MODE);
  public static final BooleanValidator JSON_EXTENDED_TYPES = new BooleanValidator("store.json.extended_types");
  public static final BooleanValidator JSON_WRITER_UGLIFY = new BooleanValidator("store.json.writer.uglify");
  public static final BooleanValidator JSON_WRITER_SKIPNULLFIELDS = new BooleanValidator("store.json.writer.skip_null_fields");
  public static final String JSON_READER_SKIP_INVALID_RECORDS_FLAG = "store.json.reader.skip_invalid_records";
  public static final BooleanValidator JSON_SKIP_MALFORMED_RECORDS_VALIDATOR = new BooleanValidator(JSON_READER_SKIP_INVALID_RECORDS_FLAG);
  public static final String JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG = "store.json.reader.print_skipped_invalid_record_number";
  public static final BooleanValidator JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR = new BooleanValidator(JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG);
  public static final DoubleValidator TEXT_ESTIMATED_ROW_SIZE = new RangeDoubleValidator("store.text.estimated_row_size_bytes", 1, Long.MAX_VALUE);

  /**
   * The column label (for directory levels) in results when querying files in a directory
   * E.g.  labels: dir0   dir1
   *    structure: foo
   *                |-    bar  -  a.parquet
   *                |-    baz  -  b.parquet
   */
  public static final String FILESYSTEM_PARTITION_COLUMN_LABEL = "drill.exec.storage.file.partition.column.label";
  public static final OptionValidator FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR = new StringValidator(FILESYSTEM_PARTITION_COLUMN_LABEL);

  /**
   * Implicit file columns
   */
  public static final String IMPLICIT_FILENAME_COLUMN_LABEL = "drill.exec.storage.implicit.filename.column.label";
  public static final OptionValidator IMPLICIT_FILENAME_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_FILENAME_COLUMN_LABEL);
  public static final String IMPLICIT_SUFFIX_COLUMN_LABEL = "drill.exec.storage.implicit.suffix.column.label";
  public static final OptionValidator IMPLICIT_SUFFIX_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_SUFFIX_COLUMN_LABEL);
  public static final String IMPLICIT_FQN_COLUMN_LABEL = "drill.exec.storage.implicit.fqn.column.label";
  public static final OptionValidator IMPLICIT_FQN_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_FQN_COLUMN_LABEL);
  public static final String IMPLICIT_FILEPATH_COLUMN_LABEL = "drill.exec.storage.implicit.filepath.column.label";
  public static final OptionValidator IMPLICIT_FILEPATH_COLUMN_LABEL_VALIDATOR = new StringValidator(IMPLICIT_FILEPATH_COLUMN_LABEL);

  public static final String JSON_READ_NUMBERS_AS_DOUBLE = "store.json.read_numbers_as_double";
  public static final BooleanValidator JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(JSON_READ_NUMBERS_AS_DOUBLE);

  public static final String MONGO_ALL_TEXT_MODE = "store.mongo.all_text_mode";
  public static final OptionValidator MONGO_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(MONGO_ALL_TEXT_MODE);
  public static final String MONGO_READER_READ_NUMBERS_AS_DOUBLE = "store.mongo.read_numbers_as_double";
  public static final OptionValidator MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(MONGO_READER_READ_NUMBERS_AS_DOUBLE);
  public static final String MONGO_BSON_RECORD_READER = "store.mongo.bson.record.reader";
  public static final OptionValidator MONGO_BSON_RECORD_READER_VALIDATOR = new BooleanValidator(MONGO_BSON_RECORD_READER);

  public static final BooleanValidator ENABLE_UNION_TYPE = new BooleanValidator("exec.enable_union_type");

  // Kafka plugin related options.
  public static final String KAFKA_ALL_TEXT_MODE = "store.kafka.all_text_mode";
  public static final OptionValidator KAFKA_READER_ALL_TEXT_MODE_VALIDATOR = new BooleanValidator(KAFKA_ALL_TEXT_MODE);
  public static final String KAFKA_READER_READ_NUMBERS_AS_DOUBLE = "store.kafka.read_numbers_as_double";
  public static final OptionValidator KAFKA_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR = new BooleanValidator(
      KAFKA_READER_READ_NUMBERS_AS_DOUBLE);
  public static final String KAFKA_RECORD_READER = "store.kafka.record.reader";
  public static final OptionValidator KAFKA_RECORD_READER_VALIDATOR = new StringValidator(KAFKA_RECORD_READER);
  public static final String KAFKA_POLL_TIMEOUT = "store.kafka.poll.timeout";
  public static final PositiveLongValidator KAFKA_POLL_TIMEOUT_VALIDATOR = new PositiveLongValidator(KAFKA_POLL_TIMEOUT,
      Long.MAX_VALUE);

  // TODO: We need to add a feature that enables storage plugins to add their own options. Currently we have to declare
  // in core which is not right. Move this option and above two mongo plugin related options once we have the feature.
  public static final String HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS = "store.hive.optimize_scan_with_native_readers";
  public static final OptionValidator HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR =
      new BooleanValidator(HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS);

  public static final String SLICE_TARGET = "planner.slice_target";
  public static final long SLICE_TARGET_DEFAULT = 100000l;
  public static final PositiveLongValidator SLICE_TARGET_OPTION = new PositiveLongValidator(SLICE_TARGET, Long.MAX_VALUE);

  public static final String CAST_TO_NULLABLE_NUMERIC = "drill.exec.functions.cast_empty_string_to_null";
  public static final BooleanValidator CAST_TO_NULLABLE_NUMERIC_OPTION = new BooleanValidator(CAST_TO_NULLABLE_NUMERIC);

  /**
   * HashTable runtime settings
   */
  public static final String MIN_HASH_TABLE_SIZE_KEY = "exec.min_hash_table_size";
  public static final PositiveLongValidator MIN_HASH_TABLE_SIZE = new PositiveLongValidator(MIN_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY);
  public static final String MAX_HASH_TABLE_SIZE_KEY = "exec.max_hash_table_size";
  public static final PositiveLongValidator MAX_HASH_TABLE_SIZE = new PositiveLongValidator(MAX_HASH_TABLE_SIZE_KEY, HashTable.MAXIMUM_CAPACITY);

  /**
   * Limits the maximum level of parallelization to this factor time the number of Drillbits
   */
  public static final String CPU_LOAD_AVERAGE_KEY = "planner.cpu_load_average";
  public static final DoubleValidator CPU_LOAD_AVERAGE = new DoubleValidator(CPU_LOAD_AVERAGE_KEY);
  public static final String MAX_WIDTH_PER_NODE_KEY = "planner.width.max_per_node";
  public static final MaxWidthValidator MAX_WIDTH_PER_NODE = new MaxWidthValidator(MAX_WIDTH_PER_NODE_KEY);

  /**
   * The maximum level or parallelization any stage of the query can do. Note that while this
   * might be the number of active Drillbits, realistically, this could be well beyond that
   * number of we want to do things like speed results return.
   */
  public static final String MAX_WIDTH_GLOBAL_KEY = "planner.width.max_per_query";
  public static final OptionValidator MAX_WIDTH_GLOBAL = new PositiveLongValidator(MAX_WIDTH_GLOBAL_KEY, Integer.MAX_VALUE);

  /**
   * Factor by which a node with endpoint affinity will be favored while creating assignment
   */
  public static final String AFFINITY_FACTOR_KEY = "planner.affinity_factor";
  public static final OptionValidator AFFINITY_FACTOR = new DoubleValidator(AFFINITY_FACTOR_KEY);

  public static final String EARLY_LIMIT0_OPT_KEY = "planner.enable_limit0_optimization";
  public static final BooleanValidator EARLY_LIMIT0_OPT = new BooleanValidator(EARLY_LIMIT0_OPT_KEY);

  public static final String ENABLE_MEMORY_ESTIMATION_KEY = "planner.memory.enable_memory_estimation";
  public static final OptionValidator ENABLE_MEMORY_ESTIMATION = new BooleanValidator(ENABLE_MEMORY_ESTIMATION_KEY);

  /**
   * Maximum query memory per node (in MB). Re-plan with cheaper operators if
   * memory estimation exceeds this limit.
   * <p/>
   * DEFAULT: 2048 MB
   */
  public static final String MAX_QUERY_MEMORY_PER_NODE_KEY = "planner.memory.max_query_memory_per_node";
  public static final LongValidator MAX_QUERY_MEMORY_PER_NODE = new RangeLongValidator(MAX_QUERY_MEMORY_PER_NODE_KEY, 1024 * 1024, Long.MAX_VALUE);

  /**
   * Alternative way to compute per-query-per-node memory as a percent
   * of the total available system memory.
   * <p>
   * Suggestion for computation.
   * <ul>
   * <li>Assume an allowance for non-managed operators. Default assumption:
   * 50%</li>
   * <li>Assume a desired number of concurrent queries. Default assumption:
   * 10.</li>
   * <li>The value of this parameter is<br>
   * (1 - non-managed allowance) / concurrency</li>
   * </ul>
   * Doing the math produces the default 5% number. The actual number
   * given is no less than the <tt>max_query_memory_per_node</tt>
   * amount.
   * <p>
   * This number is used only when throttling is disabled. Setting the
   * number to 0 effectively disables this technique as it will always
   * produce values lower than <tt>max_query_memory_per_node</tt>.
   * <p>
   * DEFAULT: 5%
   */

  public static String PERCENT_MEMORY_PER_QUERY_KEY = "planner.memory.percent_per_query";
  public static DoubleValidator PERCENT_MEMORY_PER_QUERY = new RangeDoubleValidator(
      PERCENT_MEMORY_PER_QUERY_KEY, 0, 1.0);

  /**
   * Minimum memory allocated to each buffered operator instance.
   * <p/>
   * DEFAULT: 40 MB
   */
  public static final String MIN_MEMORY_PER_BUFFERED_OP_KEY = "planner.memory.min_memory_per_buffered_op";
  public static final LongValidator MIN_MEMORY_PER_BUFFERED_OP = new RangeLongValidator(MIN_MEMORY_PER_BUFFERED_OP_KEY, 1024 * 1024, Long.MAX_VALUE);

  /**
   * Extra query memory per node for non-blocking operators.
   * NOTE: This option is currently used only for memory estimation.
   * <p/>
   * DEFAULT: 64 MB
   * MAXIMUM: 2048 MB
   */
  public static final String NON_BLOCKING_OPERATORS_MEMORY_KEY = "planner.memory.non_blocking_operators_memory";
  public static final OptionValidator NON_BLOCKING_OPERATORS_MEMORY = new PowerOfTwoLongValidator(
    NON_BLOCKING_OPERATORS_MEMORY_KEY, 1 << 11);

  public static final String HASH_JOIN_TABLE_FACTOR_KEY = "planner.memory.hash_join_table_factor";
  public static final OptionValidator HASH_JOIN_TABLE_FACTOR = new DoubleValidator(HASH_JOIN_TABLE_FACTOR_KEY);

  public static final String HASH_AGG_TABLE_FACTOR_KEY = "planner.memory.hash_agg_table_factor";
  public static final OptionValidator HASH_AGG_TABLE_FACTOR = new DoubleValidator(HASH_AGG_TABLE_FACTOR_KEY);

  public static final String AVERAGE_FIELD_WIDTH_KEY = "planner.memory.average_field_width";
  public static final OptionValidator AVERAGE_FIELD_WIDTH = new PositiveLongValidator(AVERAGE_FIELD_WIDTH_KEY, Long.MAX_VALUE);

  // Resource management boot-time options.

  public static final String MAX_MEMORY_PER_NODE = "drill.exec.rm.memory_per_node";
  public static final String MAX_CPUS_PER_NODE = "drill.exec.rm.cpus_per_node";

  // Resource management system run-time options.

  // Enables queues. When running embedded, enables an in-process queue. When
  // running distributed, enables the Zookeeper-based distributed queue.

  public static final BooleanValidator ENABLE_QUEUE = new BooleanValidator("exec.queue.enable");
  public static final LongValidator LARGE_QUEUE_SIZE = new PositiveLongValidator("exec.queue.large", 10_000);
  public static final LongValidator SMALL_QUEUE_SIZE = new PositiveLongValidator("exec.queue.small", 100_000);
  public static final LongValidator QUEUE_THRESHOLD_SIZE = new PositiveLongValidator("exec.queue.threshold", Long.MAX_VALUE);
  public static final LongValidator QUEUE_TIMEOUT = new PositiveLongValidator("exec.queue.timeout_millis", Long.MAX_VALUE);

  // Ratio of memory for small queries vs. large queries.
  // Each small query gets 1 unit, each large query gets QUEUE_MEMORY_RATIO units.
  // A lower limit of 1 enforces the intuition that a large query should never get
  // *less* memory than a small one.

  public static final DoubleValidator QUEUE_MEMORY_RATIO = new RangeDoubleValidator("exec.queue.memory_ratio", 1.0, 1000);

  public static final DoubleValidator QUEUE_MEMORY_RESERVE = new RangeDoubleValidator("exec.queue.memory_reserve_ratio", 0, 1.0);

  public static final String ENABLE_VERBOSE_ERRORS_KEY = "exec.errors.verbose";
  public static final OptionValidator ENABLE_VERBOSE_ERRORS = new BooleanValidator(ENABLE_VERBOSE_ERRORS_KEY);

  public static final String ENABLE_NEW_TEXT_READER_KEY = "exec.storage.enable_new_text_reader";
  public static final OptionValidator ENABLE_NEW_TEXT_READER = new BooleanValidator(ENABLE_NEW_TEXT_READER_KEY);

  public static final String BOOTSTRAP_STORAGE_PLUGINS_FILE = "bootstrap-storage-plugins.json";

  public static final String DRILL_SYS_FILE_SUFFIX = ".sys.drill";

  public static final String ENABLE_WINDOW_FUNCTIONS = "window.enable";
  public static final OptionValidator ENABLE_WINDOW_FUNCTIONS_VALIDATOR = new BooleanValidator(ENABLE_WINDOW_FUNCTIONS);

  public static final String DRILLBIT_CONTROL_INJECTIONS = "drill.exec.testing.controls";
  public static final OptionValidator DRILLBIT_CONTROLS_VALIDATOR = new ExecutionControls.ControlsOptionValidator(DRILLBIT_CONTROL_INJECTIONS, 1);

  public static final String NEW_VIEW_DEFAULT_PERMS_KEY = "new_view_default_permissions";
  public static final OptionValidator NEW_VIEW_DEFAULT_PERMS_VALIDATOR = new StringValidator(NEW_VIEW_DEFAULT_PERMS_KEY);

  public static final String CTAS_PARTITIONING_HASH_DISTRIBUTE = "store.partition.hash_distribute";
  public static final BooleanValidator CTAS_PARTITIONING_HASH_DISTRIBUTE_VALIDATOR = new BooleanValidator(CTAS_PARTITIONING_HASH_DISTRIBUTE);

  public static final String ENABLE_BULK_LOAD_TABLE_LIST_KEY = "exec.enable_bulk_load_table_list";
  public static final BooleanValidator ENABLE_BULK_LOAD_TABLE_LIST = new BooleanValidator(ENABLE_BULK_LOAD_TABLE_LIST_KEY);

  /**
   * When getting Hive Table information with exec.enable_bulk_load_table_list set to true,
   * use the exec.bulk_load_table_list.bulk_size to determine how many tables to fetch from HiveMetaStore
   * at a time. (The number of tables can get to be quite large.)
   */
  public static final String BULK_LOAD_TABLE_LIST_BULK_SIZE_KEY = "exec.bulk_load_table_list.bulk_size";
  public static final PositiveLongValidator BULK_LOAD_TABLE_LIST_BULK_SIZE = new PositiveLongValidator(BULK_LOAD_TABLE_LIST_BULK_SIZE_KEY, Integer.MAX_VALUE);

  /**
   * Option whose value is a comma separated list of admin usernames. Admin users are users who have special privileges
   * such as changing system options.
   */
  public static final String ADMIN_USERS_KEY = "security.admin.users";
  public static final AdminUsersValidator ADMIN_USERS_VALIDATOR = new AdminUsersValidator(ADMIN_USERS_KEY);

  /**
   * Option whose value is a comma separated list of admin usergroups.
   */
  public static final String ADMIN_USER_GROUPS_KEY = "security.admin.user_groups";
  public static final AdminUserGroupsValidator ADMIN_USER_GROUPS_VALIDATOR =
          new AdminUserGroupsValidator(ADMIN_USER_GROUPS_KEY);
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
  public static final String IMPERSONATION_POLICIES_KEY = "exec.impersonation.inbound_policies";
  public static final StringValidator IMPERSONATION_POLICY_VALIDATOR =
      new InboundImpersonationManager.InboundImpersonationPolicyValidator(IMPERSONATION_POLICIES_KEY);


  /**
   * Web settings
   */
  public static final String WEB_LOGS_MAX_LINES = "web.logs.max_lines";
  public static final OptionValidator WEB_LOGS_MAX_LINES_VALIDATOR = new PositiveLongValidator(WEB_LOGS_MAX_LINES, Integer.MAX_VALUE);

  public static final String CODE_GEN_EXP_IN_METHOD_SIZE = "exec.java.compiler.exp_in_method_size";
  public static final LongValidator CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR = new LongValidator(CODE_GEN_EXP_IN_METHOD_SIZE);

  /**
   * Timeout for create prepare statement request. If the request exceeds this timeout, then request is timed out.
   * Default value is 10mins.
   */
  public static final String CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS = "prepare.statement.create_timeout_ms";
  public static final OptionValidator CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS_VALIDATOR =
      new PositiveLongValidator(CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS, Integer.MAX_VALUE);

  public static final String DYNAMIC_UDF_SUPPORT_ENABLED = "exec.udf.enable_dynamic_support";
  public static final BooleanValidator DYNAMIC_UDF_SUPPORT_ENABLED_VALIDATOR = new BooleanValidator(DYNAMIC_UDF_SUPPORT_ENABLED);

  /**
   * Option to save query profiles. If false, no query profile will be saved
   * for any query.
   */
  public static final String ENABLE_QUERY_PROFILE_OPTION = "exec.query_profile.save";
  public static final BooleanValidator ENABLE_QUERY_PROFILE_VALIDATOR = new BooleanValidator(ENABLE_QUERY_PROFILE_OPTION);

  /**
   * Profiles are normally written after the last client message to reduce latency.
   * When running tests, however, we want the profile written <i>before</i> the
   * return so that the client can immediately read the profile for test
   * verification.
   */
  public static final String QUERY_PROFILE_DEBUG_OPTION = "exec.query_profile.debug_mode";
  public static final BooleanValidator QUERY_PROFILE_DEBUG_VALIDATOR = new BooleanValidator(QUERY_PROFILE_DEBUG_OPTION);

  public static final String USE_DYNAMIC_UDFS_KEY = "exec.udf.use_dynamic";
  public static final BooleanValidator USE_DYNAMIC_UDFS = new BooleanValidator(USE_DYNAMIC_UDFS_KEY);

  public static final String QUERY_TRANSIENT_STATE_UPDATE_KEY = "exec.query.progress.update";
  public static final BooleanValidator QUERY_TRANSIENT_STATE_UPDATE = new BooleanValidator(QUERY_TRANSIENT_STATE_UPDATE_KEY);

  public static final String PERSISTENT_TABLE_UMASK = "exec.persistent_table.umask";
  public static final StringValidator PERSISTENT_TABLE_UMASK_VALIDATOR = new StringValidator(PERSISTENT_TABLE_UMASK);

  /**
   * Enables batch iterator (operator) validation. Validation is normally enabled
   * only when assertions are enabled. This option enables iterator validation even
   * if assertions are not enabled. That is, it allows iterator validation even on
   * a "production" Drill instance.
   */
  public static final String ENABLE_ITERATOR_VALIDATION_OPTION = "debug.validate_iterators";
  public static final BooleanValidator ENABLE_ITERATOR_VALIDATOR = new BooleanValidator(ENABLE_ITERATOR_VALIDATION_OPTION);

  /**
   * Boot-time config option to enable validation. Primarily used for tests.
   * If true, overrrides the above. (That is validation is done if assertions are on,
   * if the above session option is set to true, or if this config option is set to true.
   */
  public static final String ENABLE_ITERATOR_VALIDATION = "drill.exec.debug.validate_iterators";

  /**
   * When iterator validation is enabled, additionally validates the vectors in
   * each batch passed to each iterator.
   */
  public static final String ENABLE_VECTOR_VALIDATION_OPTION = "debug.validate_vectors";
  public static final BooleanValidator ENABLE_VECTOR_VALIDATOR = new BooleanValidator(ENABLE_VECTOR_VALIDATION_OPTION);

  /**
   * Boot-time config option to enable vector validation. Primarily used for
   * tests. Add the following to the command line to enable:<br>
   * <tt>-ea -Ddrill.exec.debug.validate_vectors=true</tt>
   */
  public static final String ENABLE_VECTOR_VALIDATION = "drill.exec.debug.validate_vectors";

  public static final String OPTION_DEFAULTS_ROOT = "drill.exec.options.";

  public static String bootDefaultFor(String name) {
    return OPTION_DEFAULTS_ROOT + name;
}
  /**
   * Boot-time config option provided to modify duration of the grace period.
   * Grace period is the amount of time where the drillbit accepts work after
   * the shutdown request is triggered. The primary use of grace period is to
   * avoid the race conditions caused by zookeeper delay in updating the state
   * information of the drillbit that is shutting down. So, it is advisable
   * to have a grace period that is atleast twice the amount of zookeeper
   * refresh time.
   */
  public static final String GRACE_PERIOD = "drill.exec.grace_period_ms";

  public static final String DRILL_PORT_HUNT = "drill.exec.port_hunt";

}
