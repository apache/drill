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
package org.apache.drill.exec.store.cassandra;

public interface CassandraTestConstants {

    static final String COL_NAME_1 = "id";
    static final String COL_NAME_2 = "pog_id";

    static final String KEYSPACE_NAME = "drilltest";
    static final String TABLE_NAME = "trending_now";

    static final String PHYSICAL_PLAN_SCAN = "/cassandra-plans/cassandra_scan_screen_physical.json";
    static final String PHYSICAL_PLAN_SCAN_WITH_COLS  = "/cassandra-plans/cassandra_scan_screen_with_columns_physical.json";

    static final String SELECT_ALL = "SELECT * FROM cassandra."+KEYSPACE_NAME +".`"+ TABLE_NAME +"` t";

    static final String SELECT_QUERY_PROJECT =
            "SELECT "
                    + COL_NAME_1 +"," + COL_NAME_2
                    + " FROM  cassandra."+ KEYSPACE_NAME +".`"+ TABLE_NAME +"` t";

    static final String SELECT_QUERY_FILTER =
            "SELECT  * FROM  cassandra."+ KEYSPACE_NAME +".`"+ TABLE_NAME +"` t WHERE "
                    +COL_NAME_1+" = 'id0004'";

    static final String SELECT_QUERY_FILTER_1 =
            "SELECT  * FROM  cassandra."+ KEYSPACE_NAME +".`"+ TABLE_NAME +"` t WHERE "
                    +COL_NAME_2+" = 10002";

    static final String SELECT_QUERY_FILTER_2 =
            "SELECT * FROM  cassandra."+ KEYSPACE_NAME +".`"+ TABLE_NAME +"` t WHERE "
                    +COL_NAME_1+" = 'id0004' and " +COL_NAME_2+" = '10002'";

    static final String SELECT_QUERY_FILTER_With_OR =
            "SELECT * FROM  cassandra."+ KEYSPACE_NAME +".`"+ TABLE_NAME +"` t WHERE " +
                    "(" +COL_NAME_1+" = 'id0004' or "  +COL_NAME_1+" = 'id0002') and " +
                    "(" +COL_NAME_2+" = '10001' or "+COL_NAME_2+" = '10002') " +
                    "order by " + COL_NAME_2 +" asc, "+ COL_NAME_1 +" desc limit 8";

    static final String SELECT_QUERY_FILTER_WITH_AND =
            "SELECT * FROM  cassandra."+ KEYSPACE_NAME +".`"+ TABLE_NAME +"` t WHERE "
                    +COL_NAME_1+" = 'id0004' and pog_rank = 2";

}
