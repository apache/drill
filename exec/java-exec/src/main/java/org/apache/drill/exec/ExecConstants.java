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

import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;

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
  public static final String STORAGE_ENGINE_SCAN_PACKAGES = "drill.exec.storage.packages";
  public static final String SERVICE_NAME = "drill.exec.cluster-id";
  public static final String INITIAL_BIT_PORT = "drill.exec.rpc.bit.server.port";
  public static final String INITIAL_USER_PORT = "drill.exec.rpc.user.server.port";
  public static final String METRICS_CONTEXT_NAME = "drill.exec.metrics.context";
  public static final String FUNCTION_PACKAGES = "drill.exec.functions";
  public static final String USE_IP_ADDRESS = "drill.exec.rpc.use.ip";
  public static final String METRICS_JMX_OUTPUT_ENABLED = "drill.exec.metrics.jmx.enabled";
  public static final String METRICS_LOG_OUTPUT_ENABLED = "drill.exec.metrics.log.enabled";
  public static final String METRICS_LOG_OUTPUT_INTERVAL = "drill.exec.metrics.log.interval";
  public static final String GLOBAL_MAX_WIDTH = "drill.exec.work.global.max.width";
  public static final String MAX_WIDTH_PER_ENDPOINT = "drill.exec.work.max.width.per.endpoint";
  public static final String EXECUTOR_THREADS = "drill.exec.work.executor.threads";
  public static final String AFFINITY_FACTOR = "drill.exec.work.affinity.factor";
  public static final String CLIENT_RPC_THREADS = "drill.exec.rpc.user.client.threads";
  public static final String BIT_SERVER_RPC_THREADS = "drill.exec.rpc.bit.server.threads";
  public static final String USER_SERVER_RPC_THREADS = "drill.exec.rpc.user.server.threads";
  public static final String TRACE_DUMP_DIRECTORY = "drill.exec.trace.directory";
  public static final String TRACE_DUMP_FILESYSTEM = "drill.exec.trace.filesystem";
  public static final String TEMP_DIRECTORIES = "drill.exec.tmp.directories";
  public static final String TEMP_FILESYSTEM = "drill.exec.tmp.filesystem";
  public static final String INCOMING_BUFFER_IMPL = "drill.exec.buffer.impl";
  public static final String INCOMING_BUFFER_SIZE = "drill.exec.buffer.size"; // incoming buffer size (number of batches)
  public static final String SPOOLING_BUFFER_DELETE = "drill.exec.buffer.spooling.delete";
  public static final String SPOOLING_BUFFER_MEMORY = "drill.exec.buffer.spooling.size";
  public static final String BATCH_PURGE_THRESHOLD = "drill.exec.sort.purge.threshold";
  public static final String EXTERNAL_SORT_TARGET_BATCH_SIZE = "drill.exec.sort.external.batch.size";
  public static final String EXTERNAL_SORT_TARGET_SPILL_BATCH_SIZE = "drill.exec.sort.external.spill.batch.size";
  public static final String EXTERNAL_SORT_SPILL_GROUP_SIZE = "drill.exec.sort.external.spill.group.size";
  public static final String EXTERNAL_SORT_SPILL_THRESHOLD = "drill.exec.sort.external.spill.threshold";
  public static final String EXTERNAL_SORT_SPILL_DIRS = "drill.exec.sort.external.spill.directories";
  public static final String EXTERNAL_SORT_SPILL_FILESYSTEM = "drill.exec.sort.external.spill.fs";
  public static final String TEXT_LINE_READER_BATCH_SIZE = "drill.exec.storage.file.text.batch.size";
  public static final String TEXT_LINE_READER_BUFFER_SIZE = "drill.exec.storage.file.text.buffer.size";
  public static final String FILESYSTEM_PARTITION_COLUMN_LABEL = "drill.exec.storage.file.partition.column.label";
  public static final String HAZELCAST_SUBNETS = "drill.exec.cache.hazel.subnets";
  public static final String TOP_LEVEL_MAX_ALLOC = "drill.exec.memory.top.max";
  public static final String OUTPUT_FORMAT_OPTION = "store.format";
  public static final OptionValidator OUTPUT_FORMAT_VALIDATOR = new StringValidator(OUTPUT_FORMAT_OPTION, "parquet");
  public static final String PARQUET_BLOCK_SIZE = "parquet.block.size";
  public static final OptionValidator PARQUET_BLOCK_SIZE_VALIDATOR = new LongValidator(PARQUET_BLOCK_SIZE, 512*1024*1024);
  public static final String HTTP_ENABLE = "drill.exec.http.enabled";
  public static final String HTTP_PORT = "drill.exec.http.port";
  public static final String ERROR_ON_MEMORY_LEAK = "drill.exec.debug.error_on_leak";
}
