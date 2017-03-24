/*
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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Sets;
import org.apache.calcite.jdbc.SimpleCalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ViewExpansionContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.SchemaConfig.SchemaConfigInfoProvider;
import org.apache.drill.exec.util.ImpersonationUtil;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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
    final DrillConfig config = dContext.getConfig();
    isImpersonationEnabled = config == null? false : config.getBoolean(ExecConstants.IMPERSONATION_ENABLED);
  }

  /**
   * Return root schema for process user.
   *
   * @param options list of options
   * @return root of the schema tree
   */
  public SchemaPlus createRootSchema(final OptionManager options) {
    SchemaConfigInfoProvider schemaConfigInfoProvider = new SchemaConfigInfoProvider() {

      @Override
      public ViewExpansionContext getViewExpansionContext() {
        throw new UnsupportedOperationException("View expansion context is not supported");
      }

      @Override
      public OptionValue getOption(String optionKey) {
        return options.getOption(optionKey);
      }

      @Override public SchemaPlus getRootSchema(String userName) {
        return createRootSchema(userName, this);
      }

      @Override public String getQueryUserName() {
        return ImpersonationUtil.getProcessUserName();
      }
    };

    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(
        ImpersonationUtil.getProcessUserName(), schemaConfigInfoProvider)
        .build();

    return createRootSchema(schemaConfig);
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


  public SchemaPlus createPartialRootSchema(final String userName, final SchemaConfigInfoProvider provider,
                                            final String storage) {
    final String schemaUser = isImpersonationEnabled ? userName : ImpersonationUtil.getProcessUserName();
    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(schemaUser, provider).build();
    final SchemaPlus rootSchema = SimpleCalciteSchema.createRootSchema(false);
    Set<String> storageSet = Sets.newHashSet();
    storageSet.add(storage);
    addNewStoragesToRootSchema(schemaConfig, rootSchema, storageSet);
    schemaTreesToClose.add(rootSchema);
    return rootSchema;
  }

  public SchemaPlus addPartialRootSchema(final String userName, final SchemaConfigInfoProvider provider,
                                            Set<String> storages, SchemaPlus rootSchema) {
    final String schemaUser = isImpersonationEnabled ? userName : ImpersonationUtil.getProcessUserName();
    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(schemaUser, provider).build();
    addNewStoragesToRootSchema(schemaConfig, rootSchema, storages);
    schemaTreesToClose.add(rootSchema);
    return rootSchema;
  }

  private void expandSecondLevelSchema(SchemaPlus parent) {
    List<SchemaPlus> secondLevelSchemas = Lists.newArrayList();
    for (String firstLevelSchemaName : parent.getSubSchemaNames()) {
      SchemaPlus firstLevelSchema = parent.getSubSchema(firstLevelSchemaName);
      for (String secondLevelSchemaName : firstLevelSchema.getSubSchemaNames()) {
        secondLevelSchemas.add(firstLevelSchema.getSubSchema(secondLevelSchemaName));
      }
    }

    for (SchemaPlus schema : secondLevelSchemas) {
      AbstractSchema drillSchema;
      try {
        drillSchema = schema.unwrap(AbstractSchema.class);
      } catch (ClassCastException e) {
        throw new RuntimeException(String.format("Schema '%s' is not expected under root schema", schema.getName()));
      }
      SubSchemaWrapper wrapper = new SubSchemaWrapper(drillSchema);
      parent.add(wrapper.getName(), wrapper);
    }
  }

  public void addNewStoragesToRootSchema(SchemaConfig schemaConfig, SchemaPlus rootSchema, Set<String> storages) {
    try {
      for(String name: storages) {
        try {
          StoragePlugin plugin = dContext.getStorage().getPlugin(name);
          if(plugin == null) {
            logger.error("Got null for storage plugin of name: " + name);
            continue;
          }
          plugin.registerSchemas(schemaConfig, rootSchema);
          expandSecondLevelSchema(rootSchema);
        }
        catch (ExecutionSetupException ex) {
          logger.error("failed to get plugin and register schemas for " + name, ex);
        }
      }
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
