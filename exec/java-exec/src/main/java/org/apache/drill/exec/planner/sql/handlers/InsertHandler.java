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
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs plan to be executed for inserting data into the table.
 */
public class InsertHandler extends DefaultSqlHandler {
  private static final Logger logger = LoggerFactory.getLogger(InsertHandler.class);

  public InsertHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  protected ConvertedRelNode validateAndConvert(SqlNode sqlNode)
    throws ForemanSetupException, RelConversionException, ValidationException {
    ConvertedRelNode convertedRelNode = super.validateAndConvert(sqlNode);

    String storageName = SchemaUtilites.getSchemaPathAsList(
      convertedRelNode.getConvertedNode().getTable().getQualifiedName().iterator().next()).iterator().next();
    try {
      if (!context.getStorage().getPlugin(storageName).supportsInsert()) {
        throw UserException.validationError()
          .message("Storage plugin [%s] is immutable or doesn't support inserts", storageName)
          .build(logger);
      }
    } catch (StoragePluginRegistry.PluginException e) {
      throw new DrillRuntimeException(e);
    }

    return convertedRelNode;
  }

}
