/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB.schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePluginConfig;
import org.apache.drill.exec.store.openTSDB.schema.OpenTSDBSchemaFactory.OpenTSDBTables;

import java.util.Map;
import java.util.Set;

@Slf4j
public class OpenTSDBDatabaseSchema extends AbstractSchema {

    private final OpenTSDBTables schema;
    private final Set<String> tableNames;

    private final Map<String, DrillTable> drillTables = Maps.newHashMap();

    public OpenTSDBDatabaseSchema(Set<String> tableList, OpenTSDBTables schema,
                                  String name) {
        super(schema.getSchemaPath(), name);
        this.schema = schema;
        this.tableNames = Sets.newHashSet(tableList);
    }

    @Override
    public Table getTable(String tableName) {
        if (!tableNames.contains(tableName)) { // table does not exist
            return null;
        }

        if (!drillTables.containsKey(tableName)) {
            drillTables.put(tableName, schema.getDrillTable(tableName));
        }

        return drillTables.get(tableName);

    }

    @Override
    public Set<String> getTableNames() {
        return tableNames;
    }

    @Override
    public String getTypeName() {
        return OpenTSDBStoragePluginConfig.NAME;
    }

}
