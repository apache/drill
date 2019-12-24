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

import org.junit.Ignore;
import org.junit.Test;

@Ignore("It requires an elasticsearch server running on localhost, port 9200 with init-script.sh script run on it")
public class TestElasticQueries extends ElasticTestBase {

    //@Test
    public void testPluginIsLoaded() {
        //DO NOTHING
    }
    @Test
    public void testGetAllDevelopers() throws Exception {
        String queryString = String.format(ElasticSearchTestConstants.TEST_SELECT_ALL_QUERY_TEMPLATE,
                ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
        runElasticSearchSQLVerifyCount(queryString, 19);
    }

    @Test
    public void testGetAllDevelopersIDsAndNames() throws Exception {
        String queryString = String.format(ElasticSearchTestConstants.TEST_SELECT_IDNAMES_QUERY_TEMPLATE,
                ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
        runElasticSearchSQLVerifyCount(queryString, 19);
    }

    //@Test
    public void testBooleanFilter() throws Exception {
        String queryString = String.format(ElasticSearchTestConstants.TEST_BOOLEAN_FILTER_QUERY_TEMPLATE1,
                ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
        runElasticSearchSQLVerifyCount(queryString, 19);
        queryString = String.format(ElasticSearchTestConstants.TEST_BOOLEAN_FILTER_QUERY_TEMPLATE2,
                ElasticSearchTestConstants.EMPLOYEE_IDX, ElasticSearchTestConstants.DEVELOPER_MAPPING);
        runElasticSearchSQLVerifyCount(queryString, 8);

    }
}
