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
package org.apache.drill.exec.store.mongo;

public interface DrillMongoConstants {

  public static final String SYS_STORE_PROVIDER_MONGO_URL = "drill.exec.sys.store.provider.mongo.url";

  public static final String ID = "_id";

  public static final String SHARDS = "shards";

  public static final String NS = "ns";

  public static final String SHARD = "shard";

  public static final String HOST = "host";

  public static final String CHUNKS = "chunks";

  public static final String SIZE = "size";

  public static final String COUNT = "count";

  public static final String CONFIG = "config";

  public static final String MIN = "min";

  public static final String MAX = "max";
}
