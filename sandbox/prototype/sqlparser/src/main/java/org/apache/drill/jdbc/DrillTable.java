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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.hydromatic.linq4j.BaseQueryable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.MethodCallExpression;
import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TableFactory;
import net.hydromatic.optiq.TranslatableTable;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.rse.ClasspathRSE;
import org.apache.drill.exec.ref.rse.ClasspathRSE.ClasspathInputConfig;
import org.apache.drill.optiq.DrillRel;
import org.apache.drill.optiq.DrillScan;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

/** Optiq Table used by Drill. */
public class DrillTable extends BaseQueryable<Object> implements TranslatableTable<Object>{
  
  private final Schema schema;
  private final String name;
  private final String storageEngineName;  
  private final RelDataType rowType;
  public final StorageEngineConfig storageEngineConfig;
  private Object selection;
  public DrillClient client;
  

  public boolean isRefEngine(){
    return client == null;
  }
  
  /** Creates a DrillTable. */
  public DrillTable(DrillClient client, Schema schema,
      Type elementType,
      Expression expression,
      RelDataType rowType,
      String name,
      String storageEngineName,
      Object selection,
      StorageEngineConfig storageEngineConfig
      ) {
    super(schema.getQueryProvider(), elementType, expression);
    this.client = client;
    this.schema = schema;
    this.name = name;
    this.rowType = rowType;
    this.selection = selection;
    this.storageEngineConfig = storageEngineConfig;
    this.storageEngineName = client == null ? storageEngineName : schema.getName();
  }
  
  public String getName() {
    return name;
  }

  public StorageEngineConfig getStorageEngineConfig(){
    return storageEngineConfig;
  }
  
  public Object getSelection() {
    return selection;
  }

  public static DrillTable createTable(
      DrillClient client,
      RelDataTypeFactory typeFactory,
      Schema schema,
      String name,
      String storageEngineName,
      StorageEngineConfig storageEngineConfig,
      Object selection
      ) {
    final MethodCallExpression call = Expressions.call(schema.getExpression(), //
        BuiltinMethod.DATA_CONTEXT_GET_TABLE.method, //
        Expressions.constant(name), //
        Expressions.constant(Object.class));
    
    final RelDataType rowType =
        typeFactory.createStructType(
            Collections.singletonList(
                typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.ANY))),
            Collections.singletonList("_MAP"));
      return new DrillTable(client, schema, Object.class, call, rowType, name, storageEngineName, selection, storageEngineConfig);
  }
  
  

  @Override
  public DataContext getDataContext() {
    return schema;
  }

  
  public String getStorageEngineName() {
    return storageEngineName;
  }

  public boolean useReferenceInterpreter() {
    return this.client == null;
  }

  @Override
  public RelDataType getRowType() {
    return rowType;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
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

    private static final List<String> DONUTS_TABLES = Arrays.asList(
        "DONUTS");

    private static final List<String> HR_TABLES = Arrays.asList(
        "EMPLOYEES", "DEPARTMENTS");

    private static final List<String> FOODMART_TABLES = Arrays.asList(
        "ACCOUNT", "CATEGORY", "CURRENCY", "CUSTOMER", "DAYS", "DEPARTMENT",
        "EMPLOYEE_CLOSURE", "EMPLOYEE", "EXPENSE_FACT", "INVENTORY_FACT_1997",
        "INVENTORY_FACT_1998", "POSITION", "PRODUCT_CLASS", "PRODUCT",
        "PROMOTION", "REGION", "RESERVE_EMPLOYEE", "SALARY", "SALES_FACT_1997",
        "SALES_FACT_1998", "SALES_FACT_DEC_1998", "STORE", "STORE_RAGGED",
        "TIME_BY_DAY", "WAREHOUSE", "WAREHOUSE_CLASS");

//    public DrillTable create(
//        JavaTypeFactory typeFactory,
//        Schema schema,
//        String name,
//        Map<String, Object> operand, 
//        RelDataType rowType) {
//      final ClasspathRSE.ClasspathRSEConfig rseConfig = new ClasspathRSE.ClasspathRSEConfig();
//      final ClasspathInputConfig inputConfig = new ClasspathInputConfig();
//      assert DONUTS_TABLES.contains(name)
//          || HR_TABLES.contains(name)
//          || FOODMART_TABLES.contains(name)
//          : name;
//      inputConfig.path = "/" + name.toLowerCase() + ".json";
//      inputConfig.type = DataWriter.ConverterType.JSON;
//      boolean useReferenceInterpreter;
//      if (operand.get("useReferenceInterpreter") != null){
//        useReferenceInterpreter = operand.get("useReferenceInterpreter").equals("true") ? true : false;
//      }
//      else{
//        useReferenceInterpreter = false;
//      }      
//      return createTable(typeFactory, (MutableSchema) schema, name, rseConfig,
//          inputConfig, "donuts-json", useReferenceInterpreter);
//    }
//
    @Override
    public DrillTable create(Schema schema, String name, Map<String, Object> operand, RelDataType rowType) {
      
      final ClasspathRSE.ClasspathRSEConfig rseConfig = new ClasspathRSE.ClasspathRSEConfig();
      final ClasspathInputConfig inputConfig = new ClasspathInputConfig();
      assert DONUTS_TABLES.contains(name)
          || HR_TABLES.contains(name)
          || FOODMART_TABLES.contains(name)
          : name;
      inputConfig.path = "/" + name.toLowerCase() + ".json";
      inputConfig.type = DataWriter.ConverterType.JSON;
      return createTable(null, schema.getTypeFactory(), (MutableSchema) schema, name, "donuts-json", rseConfig, inputConfig);
    }
  }

  
  
}