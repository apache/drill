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

import com.google.common.base.Joiner;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.store.AbstractSchema;
import org.eigenbase.sql.SqlNode;

public abstract class AbstractSqlHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSqlHandler.class);

  public abstract PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException;

  public static <T> T unwrap(Object o, Class<T> clazz) throws RelConversionException{
    if(clazz.isAssignableFrom(o.getClass())){
      return (T) o;
    }else{
      throw new RelConversionException(String.format("Failure trying to treat %s as type %s.", o.getClass().getSimpleName(), clazz.getSimpleName()));
    }
  }

  /**
   * From a given SchemaPlus return a mutable Drill schema object AbstractSchema if exists.
   * Otherwise throw errors.
   */
  public static AbstractSchema getMutableDrillSchema(SchemaPlus schemaPlus) throws Exception{
    AbstractSchema drillSchema;
    try {
      drillSchema = schemaPlus.unwrap(AbstractSchema.class);
      drillSchema = drillSchema.getDefaultSchema();
    } catch(ClassCastException e) {
      throw new Exception("Current schema is not a Drill schema. " +
              "Can't create new relations (tables or views) in non-Drill schemas.", e);
    }

    if (!drillSchema.isMutable())
      throw new Exception(String.format("Current schema '%s' is not a mutable schema. " +
          "Can't create new relations.", Joiner.on(".").join(drillSchema.getSchemaPath())));

    return drillSchema;
  }
}
