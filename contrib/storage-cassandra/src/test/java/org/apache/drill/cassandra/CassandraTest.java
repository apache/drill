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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraTest {

    public static void main(String[] args) {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoint("127.0.0.1")
          .withoutJMXReporting()
                .withPort(9042);

        Cluster cluster = builder.build();

        Session session = cluster.connect();
        ResultSet rs = session.execute("SELECT * FROM drilltest.trending_now");


        while(!rs.isExhausted()){
            Row r = rs.one();
            System.out.println(r.getString(0));
            System.out.println(r.getInt(1));
            System.out.println(r.getLong(2));
        }
    }
}
