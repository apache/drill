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
  public static final String CLIENT_RPC_THREADS = "drill.exec.rpc.user.client.threads";
  public static final String BIT_SERVER_RPC_THREADS = "drill.exec.rpc.bit.server.threads";
  public static final String USER_SERVER_RPC_THREADS = "drill.exec.rpc.user.server.threads";
}
