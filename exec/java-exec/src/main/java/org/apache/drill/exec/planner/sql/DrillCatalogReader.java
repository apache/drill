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
package org.apache.drill.exec.planner.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchemaImpl;
import org.apache.calcite.jdbc.SimpleCalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.store.SchemaConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link org.apache.calcite.prepare.Prepare.CatalogReader}
 * and also {@link org.apache.calcite.sql.SqlOperatorTable} based on tables and
 * functions defined schemas.
 *
 */
public class DrillCatalogReader extends CalciteCatalogReader {
  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillCatalogReader.class);

  final private QueryContext queryContext;
  private boolean allowTemporaryTables;
  private String temporarySchema;

  public DrillCatalogReader(
      QueryContext qcontext,
      CalciteSchema rootSchema,
      boolean caseSensitive,
      List<String> defaultSchema,
      JavaTypeFactory typeFactory,
      String temporarySchema) {
    super(rootSchema, caseSensitive, defaultSchema, typeFactory);
    assert rootSchema != defaultSchema;
    queryContext = qcontext;
    this.temporarySchema = temporarySchema;
    this.allowTemporaryTables = true;
  }

  /** Disallow temporary tables presence in sql statement (ex: in view definitions) */
  public void disallowTemporaryTables() {
    this.allowTemporaryTables = false;
  }

  /**
   * If schema is not indicated (only one element in the list) or schema is default temporary workspace,
   * we need to check among session temporary tables first in default temporary workspace.
   * If temporary table is found and temporary tables usage is allowed, its table instance will be returned,
   * otherwise search will be conducted in original workspace.
   *
   * @param names list of schema and table names, table name is always the last element
   * @return table instance, null otherwise
   * @throws UserException if temporary tables usage is disallowed
   */
  @Override
  public RelOptTableImpl getTable(final List<String> names) {
    RelOptTableImpl temporaryTable = null;
    if (mightBeTemporaryTable(names, queryContext.getSession().getDefaultSchemaPath(), queryContext.getConfig())) {
      String temporaryTableName = queryContext.getSession().resolveTemporaryTableName(names.get(names.size() - 1));
      if (temporaryTableName != null) {
        List<String> temporaryNames = Lists.newArrayList(temporarySchema, temporaryTableName);
        temporaryTable = super.getTable(temporaryNames);
      }
    }
    if (temporaryTable != null) {
      if (allowTemporaryTables) {
        return temporaryTable;
      }
      throw UserException
          .validationError()
          .message("Temporary tables usage is disallowed. Used temporary table name: %s.", names)
          .build(logger);
    }
    return super.getTable(names);
  }

  /**
   * We should check if passed table is temporary or not if:
   * <li>schema is not indicated (only one element in the names list)<li/>
   * <li>current schema or indicated schema is default temporary workspace<li/>
   *
   * Examples (where dfs.tmp is default temporary workspace):
   * <li>select * from t<li/>
   * <li>select * from dfs.tmp.t<li/>
   * <li>use dfs; select * from tmp.t<li/>
   *
   * @param names             list of schema and table names, table name is always the last element
   * @param defaultSchemaPath current schema path set using USE command
   * @param drillConfig       drill config
   * @return true if check for temporary table should be done, false otherwise
   */
  private boolean mightBeTemporaryTable(List<String> names, String defaultSchemaPath, DrillConfig drillConfig) {
    if (names.size() == 1) {
      return true;
    }

    String schemaPath = SchemaUtilites.getSchemaPath(names.subList(0, names.size() - 1));
    return SchemaUtilites.isTemporaryWorkspace(schemaPath, drillConfig) ||
        SchemaUtilites.isTemporaryWorkspace(
            SchemaUtilites.SCHEMA_PATH_JOINER.join(defaultSchemaPath, schemaPath), drillConfig);
  }
  public DrillCatalogReader withSchemaPath(List<String> schemaPath) {
    return new DrillCatalogReader(queryContext, super.getSchema(ImmutableList.<String>of()),
        this.isCaseSensitive(), schemaPath, (JavaTypeFactory)getTypeFactory(), temporarySchema);
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public CalciteSchema getSchema(Iterable<String> schemaNames) {

    //get 'rootSchema'
    CalciteSchema existingRootSchema = super.getSchema(ImmutableList.<String>of());
    CalciteSchema schema = existingRootSchema;
    int layer = 0;
    for (String schemaName : schemaNames) {
      schema = schema.getSubSchema(schemaName, isCaseSensitive());
      if (schema == null) {
        if (layer == 0) {
          final Set<String> strSet = Sets.newHashSet();
          strSet.add(schemaName);
          if(schemaName.contains(".")) {
            String[] schemaArray = schemaName.split("\\.");
            String prefix = schemaArray[0];
            for(int i=1; i<schemaArray.length; ++i) {
              strSet.add(prefix);
              prefix = Joiner.on(".").join(prefix, schemaArray[i]);
            }
          }

          //queryContext.addNewRelevantSchema(strSet, existingRootSchema.plus());
          SchemaPlus rootSchema = existingRootSchema.plus();
          queryContext.getSchemaTreeProvider().addPartialRootSchema(queryContext.getQueryUserName(),
              queryContext, strSet, rootSchema);
          SchemaPlus plus = rootSchema.getSubSchema(schemaName);
          if (plus != null) {
            schema = SimpleCalciteSchema.from(plus);
          }
        }
      }
      if(schema == null) {
        return null;
      }
      layer++;
    }
    return schema;
  }


}


