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

public interface MongoTestConstants {

  public static final String LOCALHOST = "localhost";
  // TODO: DRILL-3934: add some randomization to this as it fails when running concurrent builds
  public static final int CONFIG_SERVER_PORT = 27019;
  public static final int MONGOD_1_PORT = 27020;
  public static final int MONGOD_2_PORT = 27021;
  public static final int MONGOD_3_PORT = 27022;

  public static final int MONGOD_4_PORT = 27023;
  public static final int MONGOD_5_PORT = 27024;
  public static final int MONGOD_6_PORT = 27025;

  public static final int MONGOS_PORT = 27017;

  public static final String CONFIG_DB = "config";
  public static final String ADMIN_DB = "admin";

  public static final String TEST_DB = "testDB";
  public static final String EMPLOYEE_DB = "employee";

  public static final String DONUTS_COLLECTION = "donuts";
  public static final String EMPINFO_COLLECTION = "empinfo";
  public static final String SCHEMA_CHANGE_COLLECTION = "schema_change";

  public static final String DONUTS_DATA = "donuts.json";
  public static final String EMP_DATA = "emp.json";
  public static final String SCHEMA_CHANGE_DATA = "schema_change_int_to_string.json";

  public static final String REPLICA_SET_1_NAME = "shard_1_replicas";
  public static final String REPLICA_SET_2_NAME = "shard_2_replicas";

  // test queries
  public static final String TEST_QUERY_1 = "SELECT * FROM mongo.employee.`empinfo` limit 5";
  public static final String TEST_QUERY_LIMIT = "SELECT first_name, last_name FROM mongo.employee.`empinfo` limit 2;";

  // test query template1
  public static final String TEST_QUERY_PROJECT_PUSH_DOWN_TEMPLATE_1 = "SELECT `employee_id` FROM mongo.%s.`%s`";
  public static final String TEST_QUERY_PROJECT_PUSH_DOWN__TEMPLATE_2 = "select `employee_id`, `rating` from mongo.%s.`%s`";
  public static final String TEST_QUERY_PROJECT_PUSH_DOWN__TEMPLATE_3 = "select * from mongo.%s.`%s`";
  public static final String TEST_FILTER_PUSH_DOWN_IS_NULL_QUERY_TEMPLATE_1 = "SELECT `employee_id` FROM mongo.%s.`%s` where position_id is null";
  public static final String TEST_FILTER_PUSH_DOWN_IS_NOT_NULL_QUERY_TEMPLATE_1 = "SELECT `employee_id` FROM mongo.%s.`%s` where position_id is not null";
  public static final String TEST_FILTER_PUSH_DOWN_EQUAL_QUERY_TEMPLATE_1 = "SELECT `full_name` FROM mongo.%s.`%s` where rating = 52.17";
  public static final String TEST_FILTER_PUSH_DOWN_NOT_EQUAL_QUERY_TEMPLATE_1 = "SELECT `employee_id` FROM mongo.%s.`%s` where rating != 52.17";
  public static final String TEST_FILTER_PUSH_DOWN_LESS_THAN_QUERY_TEMPLATE_1 = "SELECT `full_name` FROM mongo.%s.`%s` where rating < 52.17";
  public static final String TEST_FILTER_PUSH_DOWN_GREATER_THAN_QUERY_TEMPLATE_1 = "SELECT `full_name` FROM mongo.%s.`%s` where rating > 52.17";
  public static final String TEST_EMPTY_TABLE_QUERY_TEMPLATE = "select count(*) from mongo.%s.`%s`";

  public static final String TEST_BOOLEAN_FILTER_QUERY_TEMPLATE1 = "select `employee_id` from mongo.%s.`%s` where isFTE = true";
  public static final String TEST_BOOLEAN_FILTER_QUERY_TEMPLATE2 = "select `employee_id` from mongo.%s.`%s` where isFTE = false";
  public static final String TEST_BOOLEAN_FILTER_QUERY_TEMPLATE3 = "select `employee_id` from mongo.%s.`%s` where position_id = 16 and isFTE = true";
  public static final String TEST_BOOLEAN_FILTER_QUERY_TEMPLATE4 = "select `employee_id` from mongo.%s.`%s` where (position_id = 16 and isFTE = true) or last_name = 'Yonce'";

}
