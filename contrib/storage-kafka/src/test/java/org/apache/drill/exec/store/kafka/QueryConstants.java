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
package org.apache.drill.exec.store.kafka;

public interface QueryConstants {

  // Kafka Server Prop Constants
  public static final String BROKER_DELIM = ",";
  public final String LOCAL_HOST = "127.0.0.1";

  // ZK
  public final static String ZK_TMP = "zk_tmp";
  public final static int TICK_TIME = 500;
  public final static int MAX_CLIENT_CONNECTIONS = 100;

  public static final String JSON_TOPIC = "drill-json-topic";
  public static final String AVRO_TOPIC = "drill-avro-topic";
  public static final String INVALID_TOPIC = "invalid-topic";

  // Queries
  public static final String MSG_COUNT_QUERY = "select count(*) from kafka.`%s`";
  public static final String MSG_SELECT_QUERY = "select * from kafka.`%s`";
  public static final String MIN_OFFSET_QUERY = "select MIN(kafkaMsgOffset) as minOffset from kafka.`%s`";
  public static final String MAX_OFFSET_QUERY = "select MAX(kafkaMsgOffset) as maxOffset from kafka.`%s`";
}
