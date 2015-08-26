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
package org.apache.drill.exec.store.memory;

import com.google.common.base.Preconditions;

import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.commons.lang3.ClassUtils;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.memory.MemoryTable.MemoryTableDefinition;
import org.apache.drill.exec.store.memory.MemoryTable.MemoryColumnDefinition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.sql.Timestamp;

/**
 *
 */
public class MemorySchemaProvider {

    private static final Logger logger = LoggerFactory.getLogger(MemorySchemaProvider.class);

    public boolean exists(String schema) {
        return false;
    }

    public List<String> schemas() {
        return new ArrayList<>();
    }

    public Set<String> tables(String schema) {
        return null;
    }

    public MemoryTableDefinition table(String schema, String table) {
        return null;
    }

    public void createSchema(String schema) {
        ;
    }

    public MemoryTableDefinition createTable(String schema, String table, Object prototype) {
        return createTable(schema, table, prototype.getClass());
    }

    public MemoryTableDefinition createTable(String schema, String table, Class c) {

        Preconditions.checkNotNull(schema, "Schema name may not be null");
        Preconditions.checkNotNull(table, "Table name may not be null");
        Preconditions.checkNotNull(c, "Class may not be null");

        List<MemoryColumnDefinition> columns = new ArrayList<>();

        for (Field field : c.getDeclaredFields()) {

            Class type = field.getType();

            if (type.isPrimitive()) {

                Class<?> wrapper = ClassUtils.primitiveToWrapper(type);
                SqlTypeName sqlType = SqlTypeName.get(wrapper.getSimpleName().toUpperCase(Locale.ENGLISH));

                if (sqlType != null) {
                    columns.add(new MemoryColumnDefinition(
                            field.getName(), SchemaPath.getSimplePath(field.getName()), sqlType));
                }
                else {
                    logger.warn("Unsupported data type: [{}]", type);
                }
            }
            else {
                if (type == String.class) {
                    columns.add(new MemoryColumnDefinition(
                            field.getName(), SchemaPath.getSimplePath(field.getName()), SqlTypeName.VARCHAR));
                }
                else if (type == Timestamp.class) {
                    columns.add(new MemoryColumnDefinition(
                            field.getName(), SchemaPath.getSimplePath(field.getName()), SqlTypeName.TIMESTAMP));
                }
                else if (type == Date.class) {
                    columns.add(new MemoryColumnDefinition(
                            field.getName(), SchemaPath.getSimplePath(field.getName()), SqlTypeName.DATE));
                }
                else {
                    logger.warn("Unsupported data type: [{}]", type);
                }
            }
        }

        MemoryTableDefinition definition = new MemoryTableDefinition(schema, table, columns);
        logger.info("Created new memory table: [{}]", definition);

        return definition;
    }
    
}
