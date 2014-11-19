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
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.util.List;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.eigenbase.sql.SqlNode;

import com.google.common.base.Joiner;

public abstract class AbstractSqlHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSqlHandler.class);

  public abstract PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException;

  public static <T> T unwrap(Object o, Class<T> clazz) throws ForemanSetupException {
    if (clazz.isAssignableFrom(o.getClass())) {
      return (T) o;
    } else {
      throw new ForemanSetupException(String.format("Failure trying to treat %s as type %s.", o.getClass().getSimpleName(), clazz.getSimpleName()));
    }
  }

  /**
   * From a given SchemaPlus return a Drill schema object of type AbstractSchema if exists.
   * Otherwise throw errors.
   */
  public static AbstractSchema getDrillSchema(SchemaPlus schemaPlus) throws Exception {
    AbstractSchema drillSchema;
    try {
      drillSchema = schemaPlus.unwrap(AbstractSchema.class);
      drillSchema = drillSchema.getDefaultSchema();
    } catch (ClassCastException e) {
      throw new Exception("Current schema is not a Drill schema. " +
              "Can't create new relations (tables or views) in non-Drill schemas.", e);
    }

    return drillSchema;
  }

  /**
   * Search for a schema with given schemaPath. First search in schema tree rooted at defaultSchema,
   * if not found search in rootSchema. If no schema found throw errors.
   */
  public static SchemaPlus findSchema(SchemaPlus rootSchema, SchemaPlus defaultSchema, List<String> schemaPath)
      throws Exception {
    if (schemaPath.size() == 0) {
      return defaultSchema;
    }

    SchemaPlus schema;

    if ((schema = searchSchemaTree(defaultSchema, schemaPath)) != null) {
      return schema;
    }

    if ((schema = searchSchemaTree(rootSchema, schemaPath)) != null) {
      return schema;
    }

    throw new Exception(String.format("Invalid schema path '%s'.", Joiner.on(".").join(schemaPath)));
  }

  public static boolean isRootSchema(SchemaPlus schema) {
    return schema.getParentSchema() == null;
  }

  private static SchemaPlus searchSchemaTree(SchemaPlus schema, List<String> schemaPath) {
    for (String schemaName : schemaPath) {
      schema = schema.getSubSchema(schemaName);
      if (schema == null) {
        return null;
      }
    }
    return schema;
  }

}
