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
package org.apache.drill.cassandra;

import org.junit.Test;

public class CassandraFilterPushdownTest extends BaseCassandraTest implements CassandraTestConstants {

    @Test
    public void testSelectAll() throws Exception{
        runCassandraSQLVerifyCount(SELECT_ALL, 14);
    }

    @Test
    public void testFilter() throws Exception{
        runCassandraSQLVerifyCount(SELECT_QUERY_FILTER, 6);
    }

    @Test
    public void testFilter1() throws Exception{
        runCassandraSQLVerifyCount(SELECT_QUERY_FILTER_1, 4);
    }

    @Test
    public void testFilter2() throws Exception{
        runCassandraSQLVerifyCount(SELECT_QUERY_FILTER_2, 4);
    }

    @Test
    public void testFilterWithOrCondition() throws Exception{
        runCassandraSQLVerifyCount(SELECT_QUERY_FILTER_With_OR, 8);
    }

    @Test
    public void testFilterWithAndCondition() throws Exception{
        runCassandraSQLVerifyCount(SELECT_QUERY_FILTER_WITH_AND, 1);
    }

}
