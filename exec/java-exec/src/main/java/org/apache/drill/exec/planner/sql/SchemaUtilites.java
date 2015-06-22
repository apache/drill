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
package org.apache.drill.exec.planner.sql;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.AbstractSchema;

import java.util.Collections;
import java.util.List;

public class SchemaUtilites {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaUtilites.class);
  public static final Joiner SCHEMA_PATH_JOINER = Joiner.on(".").skipNulls();

  /**
   * Search and return schema with given schemaPath. First search in schema tree starting from defaultSchema,
   * if not found search starting from rootSchema. Root schema tree is derived from the defaultSchema reference.
   *
   * @param defaultSchema Reference to the default schema in complete schema tree.
   * @param schemaPath Schema path to search.
   * @return SchemaPlus object.
   */
  public static SchemaPlus findSchema(final SchemaPlus defaultSchema, final List<String> schemaPath) {
    if (schemaPath.size() == 0) {
      return defaultSchema;
    }

    SchemaPlus schema;
    if ((schema = searchSchemaTree(defaultSchema, schemaPath)) != null) {
      return schema;
    }

    SchemaPlus rootSchema = defaultSchema;
    while(rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    if (rootSchema != defaultSchema &&
        (schema = searchSchemaTree(rootSchema, schemaPath)) != null) {
      return schema;
    }

    return null;
  }

  /**
   * Same utility as {@link #findSchema(SchemaPlus, List)} except the search schema path given here is complete path
   * instead of list. Use "." separator to divided the schema into nested schema names.
   * @param defaultSchema
   * @param schemaPath
   * @return
   * @throws ValidationException
   */
  public static SchemaPlus findSchema(final SchemaPlus defaultSchema, final String schemaPath) {
    final List<String> schemaPathAsList = Lists.newArrayList(schemaPath.split("\\."));
    return findSchema(defaultSchema, schemaPathAsList);
  }

  /** Utility method to search for schema path starting from the given <i>schema</i> reference */
  private static SchemaPlus searchSchemaTree(SchemaPlus schema, final List<String> schemaPath) {
    for (String schemaName : schemaPath) {
      schema = schema.getSubSchema(schemaName);
      if (schema == null) {
        return null;
      }
    }
    return schema;
  }

  /**
   * Returns true if the given <i>schema</i> is root schema. False otherwise.
   * @param schema
   * @return
   */
  public static boolean isRootSchema(SchemaPlus schema) {
    return schema.getParentSchema() == null;
  }

  /**
   * Unwrap given <i>SchemaPlus</i> instance as Drill schema instance (<i>AbstractSchema</i>). Once unwrapped, return
   * default schema from <i>AbstractSchema</i>. If the given schema is not an instance of <i>AbstractSchema</i> a
   * {@link UserException} is thrown.
   */
  public static AbstractSchema unwrapAsDrillSchemaInstance(SchemaPlus schemaPlus)  {
    try {
      return schemaPlus.unwrap(AbstractSchema.class).getDefaultSchema();
    } catch (ClassCastException e) {
      throw UserException.validationError(e)
          .message("Schema [%s] is not a Drill schema.", getSchemaPath(schemaPlus))
          .build(logger);
    }
  }

  /** Utility method to get the schema path for given schema instance. */
  public static String getSchemaPath(SchemaPlus schema) {
    return SCHEMA_PATH_JOINER.join(getSchemaPathAsList(schema));
  }

  /** Utility method to get the schema path as list for given schema instance. */
  public static List<String> getSchemaPathAsList(SchemaPlus schema) {
    if (isRootSchema(schema)) {
      return Collections.EMPTY_LIST;
    }

    List<String> path = Lists.newArrayListWithCapacity(5);
    while(schema != null) {
      final String name = schema.getName();
      if (!Strings.isNullOrEmpty(name)) {
        path.add(schema.getName());
      }
      schema = schema.getParentSchema();
    }

    return Lists.reverse(path);
  }

  /** Utility method to throw {@link UserException} with context information */
  public static void throwSchemaNotFoundException(final SchemaPlus defaultSchema, final String givenSchemaPath) {
    throw UserException.validationError()
        .message("Schema [%s] is not valid with respect to either root schema or current default schema.",
            givenSchemaPath)
        .addContext("Current default schema: ",
            isRootSchema(defaultSchema) ? "No default schema selected" : getSchemaPath(defaultSchema))
        .build(logger);
  }

  /**
   * Given reference to default schema in schema tree, search for schema with given <i>schemaPath</i>. Once a schema is
   * found resolve it into a mutable <i>AbstractDrillSchema</i> instance. A {@link UserException} is throws when:
   *   1. No schema for given <i>schemaPath</i> is found,
   *   2. Schema found for given <i>schemaPath</i> is a root schema
   *   3. Resolved schema is not a mutable schema.
   * @param defaultSchema
   * @param schemaPath
   * @return
   */
  public static AbstractSchema resolveToMutableDrillSchema(final SchemaPlus defaultSchema, List<String> schemaPath) {
    final SchemaPlus schema = findSchema(defaultSchema, schemaPath);

    if (schema == null) {
      throwSchemaNotFoundException(defaultSchema, SCHEMA_PATH_JOINER.join(schemaPath));
    }

    if (isRootSchema(schema)) {
      throw UserException.parseError()
          .message("Root schema is immutable. Creating or dropping tables/views is not allowed in root schema." +
              "Select a schema using 'USE schema' command.")
          .build(logger);
    }

    final AbstractSchema drillSchema = unwrapAsDrillSchemaInstance(schema);
    if (!drillSchema.isMutable()) {
      throw UserException.parseError()
          .message("Unable to create or drop tables/views. Schema [%s] is immutable.", getSchemaPath(schema))
          .build(logger);
    }

    return drillSchema;
  }
}
