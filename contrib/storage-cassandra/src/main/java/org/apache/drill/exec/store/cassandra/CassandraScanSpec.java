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

import com.datastax.driver.core.querybuilder.Clause;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CassandraScanSpec {
    private String keyspace;
    private String table;

    @JsonIgnore
    private List<Clause> filters;

    @JsonCreator
    public CassandraScanSpec(@JsonProperty("keyspace") String keyspace,
                         @JsonProperty("table") String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public CassandraScanSpec(String keyspace, String table, List<Clause> filters) {
        this.keyspace = keyspace;
        this.table = table;
        this.filters = filters;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public List<Clause> getFilters() {
        return filters;
    }


    @Override
    public String toString() {
        return "CassandraScanSpec [keyspace=" + keyspace + ", table="
                + table + ", filters=" + filters + "]";
    }

}
