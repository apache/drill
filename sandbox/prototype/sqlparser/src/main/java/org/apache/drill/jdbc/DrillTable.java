/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.jdbc;

import java.lang.reflect.Type;
import java.util.Collections;

import net.hydromatic.linq4j.BaseQueryable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.MethodCallExpression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.rse.ClasspathRSE;
import org.apache.drill.exec.ref.rse.ClasspathRSE.ClasspathInputConfig;
import org.apache.drill.optiq.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

/** Optiq Table used by Drill. */
public class DrillTable extends BaseQueryable<Object>
    implements TranslatableTable<Object>
{
  private final Schema schema;
  private final String name;
  private final RelDataType rowType;
  public final StorageEngineConfig storageEngineConfig;
  public final Object selection;

  /** Creates a DrillTable. */
  public DrillTable(Schema schema,
      Type elementType,
      Expression expression,
      RelDataType rowType,
      String name,
      StorageEngineConfig storageEngineConfig,
      Object selection) {
    super(schema.getQueryProvider(), elementType, expression);
    this.schema = schema;
    this.name = name;
    this.rowType = rowType;
    this.storageEngineConfig = storageEngineConfig;
    this.selection = selection;
  }

  private static DrillTable createTable(
      RelDataTypeFactory typeFactory,
      MutableSchema schema,
      String name,
      StorageEngineConfig storageEngineConfig,
      Object selection) {
    final MethodCallExpression call = Expressions.call(schema.getExpression(),
        BuiltinMethod.DATA_CONTEXT_GET_TABLE.method,
        Expressions.constant(name),
        Expressions.constant(Object.class));
    final RelDataType rowType =
        typeFactory.createStructType(
            Collections.singletonList(
                typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.ANY))),
            Collections.singletonList("_MAP"));
      return new DrillTable(schema, Object.class, call, rowType, name,
          storageEngineConfig, selection);
  }

  @Override
  public DataContext getDataContext() {
    return schema;
  }

  @Override
  public RelDataType getRowType() {
    return rowType;
  }

  @Override
  public Enumerator<Object> enumerator() {
    return Linq4j.emptyEnumerator();
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
    return new DrillScan(context.getCluster(),
        context.getCluster().traitSetOf(DrillRel.CONVENTION),
        table);
  }

  /** Factory for custom tables in Optiq schema. */
  @SuppressWarnings("UnusedDeclaration")
  public static class Factory implements TableFactory<DrillTable> {
    public DrillTable create(
        JavaTypeFactory typeFactory,
        Schema schema,
        String name,
        Object operand,
        RelDataType rowType) {
      final ClasspathRSE.ClasspathRSEConfig rseConfig =
          new ClasspathRSE.ClasspathRSEConfig("donuts-json");
      final ClasspathInputConfig inputConfig = new ClasspathInputConfig();
      inputConfig.path = "/donuts.json";
      inputConfig.type = DataWriter.ConverterType.JSON;
      return createTable(typeFactory, (MutableSchema) schema, name, rseConfig,
          inputConfig);
    }
  }
}

// End DrillTable.java
