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

package org.apache.drill.exec.store.elasticsearch;

public class ElasticSearchTestConstants {

    public static final String TEST_BOOLEAN_FILTER_QUERY_TEMPLATE1 = "select `name` from elasticsearch.%s.`%s`";
    public static final String EMPLOYEE_IDX = "employee";
    public static final String DEVELOPER_MAPPING = "developer";
    public static final String MANAGER_MAPPING = "manager";
    public static final String TEST_BOOLEAN_FILTER_QUERY_TEMPLATE2 = "select `employeeId` from elasticsearch.%s.`%s` where reportsTo = false";
    public static final String TEST_SELECT_ALL_QUERY_TEMPLATE = "select * from elasticsearch.%s.`%s`";
    public static final String TEST_SELECT_IDNAMES_QUERY_TEMPLATE = "select `_id`,`_source.names` from elasticsearch.%s.`%s`";
}
