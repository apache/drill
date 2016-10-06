/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import org.apache.calcite.jdbc.SimpleCalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.SchemaConfig.SchemaConfigInfoProvider;
import org.apache.drill.exec.util.ImpersonationUtil;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Class which creates new schema trees. It keeps track of newly created schema trees and closes them safely as
 * part of {@link #close()}.
 */
public class SchemaTreeProvider implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaTreeProvider.class);

  private final DrillbitContext dContext;
  private final List<SchemaPlus> schemaTreesToClose;
  private final boolean isImpersonationEnabled;

  public SchemaTreeProvider(final DrillbitContext dContext) {
    this.dContext = dContext;
    schemaTreesToClose = Lists.newArrayList();
    isImpersonationEnabled = dContext.getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED);
  }

  /**
   * Return root schema with schema owner as the given user.
   *
   * @param userName Name of the user who is accessing the storage sources.
   * @param provider {@link SchemaConfigInfoProvider} instance
   * @return Root of the schema tree.
   */
  public SchemaPlus createRootSchema(final String userName, final SchemaConfigInfoProvider provider) {
    final String schemaUser = isImpersonationEnabled ? userName : ImpersonationUtil.getProcessUserName();
    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(schemaUser, provider).build();
    return createRootSchema(schemaConfig);
  }

  /**
   * Create and return a SchemaTree with given <i>schemaConfig</i>.
   * @param schemaConfig
   * @return
   */
  public SchemaPlus createRootSchema(SchemaConfig schemaConfig) {
    try {
      final SchemaPlus rootSchema = SimpleCalciteSchema.createRootSchema(false);
      dContext.getSchemaFactory().registerSchemas(schemaConfig, rootSchema);
      schemaTreesToClose.add(rootSchema);
      return rootSchema;
    } catch(IOException e) {
      // We can't proceed further without a schema, throw a runtime exception.
      throw UserException
          .resourceError(e)
          .message("Failed to create schema tree.")
          .build(logger);
    }
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> toClose = Lists.newArrayList();
    for(SchemaPlus tree : schemaTreesToClose) {
      addSchemasToCloseList(tree, toClose);
    }

    AutoCloseables.close(toClose);
  }

  private static void addSchemasToCloseList(final SchemaPlus tree, final List<AutoCloseable> toClose) {
    for(String subSchemaName : tree.getSubSchemaNames()) {
      addSchemasToCloseList(tree.getSubSchema(subSchemaName), toClose);
    }

    try {
      AbstractSchema drillSchemaImpl =  tree.unwrap(AbstractSchema.class);
      toClose.add(drillSchemaImpl);
    } catch (ClassCastException e) {
      // Ignore as the SchemaPlus is not an implementation of Drill schema.
    }
  }
}
