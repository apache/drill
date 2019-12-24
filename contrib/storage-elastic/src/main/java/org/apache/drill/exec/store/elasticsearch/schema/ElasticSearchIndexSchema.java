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

package org.apache.drill.exec.store.elasticsearch.schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchPluginConfig;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ElasticSearchIndexSchema extends AbstractSchema {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchIndexSchema.class);

    private final ElasticSearchSchema elasticSearchSchema;
    private final Set<String> typeMappings;

    private final Map<String, DrillTable> drillTables = Maps.newHashMap();

    public ElasticSearchIndexSchema(Collection<String> typeMappings, ElasticSearchSchema elasticSearchSchema, String name) {
        super(elasticSearchSchema.getSchemaPath(), name);
        this.elasticSearchSchema = elasticSearchSchema;
        this.typeMappings = Sets.newHashSet(typeMappings);
    }

    @Override
    public Table getTable(String tableName) {
        if (!typeMappings.contains(tableName)) { // table does not exist
            return null;
        }

        if (! drillTables.containsKey(tableName)) {
        	// 去拉取这个表数据 this.name == 'es' .
            drillTables.put(tableName, elasticSearchSchema.getDrillTable(this.name, tableName));
        }

        return drillTables.get(tableName);

    }

    @Override
    public Set<String> getTableNames() {
        return typeMappings;
    }

    @Override
    public String getTypeName() {
        return ElasticSearchPluginConfig.NAME;
    }

}
