/**
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
package org.apache.drill.exec.store;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.CreateTableEntry;

import com.google.common.collect.ImmutableList;

public class SubSchemaWrapper extends AbstractSchema {

  private final AbstractSchema innerSchema;

  public SubSchemaWrapper(AbstractSchema innerSchema) {
    super(ImmutableList.<String>of(), innerSchema.getFullSchemaName());
    this.innerSchema = innerSchema;
  }

  @Override
  public boolean showInInformationSchema() {
    return false;
  }

  @Override
  public Iterable<String> getSubPartitions(String table,
                                           List<String> partitionColumns,
                                           List<String> partitionValues
  ) throws PartitionNotFoundException {
    Schema defaultSchema = getDefaultSchema();
    if (defaultSchema instanceof AbstractSchema) {
      return ((AbstractSchema) defaultSchema).getSubPartitions(table, partitionColumns, partitionValues);
    } else {
      return Collections.EMPTY_LIST;
    }

  }

  @Override
  public Schema getDefaultSchema() {
    return innerSchema.getDefaultSchema();
  }

  @Override
  public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns) {
    return innerSchema.createNewTable(tableName, partitionColumns);
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return innerSchema.getFunctions(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return innerSchema.getFunctionNames();
  }

  @Override
  public Schema getSubSchema(String name) {
    return innerSchema.getSubSchema(name);
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return innerSchema.getSubSchemaNames();
  }

  @Override
  public boolean isMutable() {
    return innerSchema.isMutable();
  }

  @Override
  public Table getTable(final String name) {
    return safeGetTable(new SafeTableGetter() {
      @Override
      public Table safeGetTable() {
        return innerSchema.getTable(name);
      }
    });
  }

  @Override
  public Set<String> getTableNames() {
    return safeGetTableNames(new SafeTableNamesGetter() {
      @Override
      public Set<String> safeGetTableNames() {
        return innerSchema.getTableNames();
      }
    });
  }

  @Override
  public String getTypeName() {
    return innerSchema.getTypeName();
  }

}
