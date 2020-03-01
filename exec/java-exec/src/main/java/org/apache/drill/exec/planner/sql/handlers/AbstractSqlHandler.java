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

import java.io.IOException;

import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.calcite.sql.SqlNode;

public abstract class AbstractSqlHandler {
  static final Logger logger = LoggerFactory.getLogger(AbstractSqlHandler.class);

  public abstract PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException;

  @SuppressWarnings("unchecked")
  public static <T> T unwrap(Object o, Class<T> clazz) throws ForemanSetupException {
    if (clazz.isAssignableFrom(o.getClass())) {
      return (T) o;
    } else {
      throw new ForemanSetupException(String.format("Failure trying to treat %s as type %s.", o.getClass().getSimpleName(), clazz.getSimpleName()));
    }
  }
}
