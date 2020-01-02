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

import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.Table;

import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.cassandra.CassandraSchemaFactory.CassandraSchema;

import com.google.common.collect.Sets;

public class CassandraDatabaseSchema extends AbstractSchema {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CassandraDatabaseSchema.class);
    private final CassandraSchema cassandraSchema;
    private final Set<String> tables;

    public CassandraDatabaseSchema(List<String> tableList, CassandraSchema cassandraSchema,
                               String name) {
        super(cassandraSchema.getSchemaPath(), name);
        this.cassandraSchema = cassandraSchema;
        this.tables = Sets.newHashSet(tableList);
    }

    @Override
    public Table getTable(String tableName) {
        return cassandraSchema.getDrillTable(this.name, tableName);
    }

    @Override
    public Set<String> getTableNames() {
        return tables;
    }

    @Override
    public String getTypeName() {
        return CassandraStoragePluginConfig.NAME;
    }

}
