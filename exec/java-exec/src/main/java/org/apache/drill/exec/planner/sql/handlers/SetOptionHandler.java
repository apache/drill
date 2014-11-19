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

import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlSetOption;

public class SetOptionHandler extends AbstractSqlHandler{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  QueryContext context;


  public SetOptionHandler(QueryContext context) {
    super();
    this.context = context;
  }


  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    SqlSetOption option = unwrap(sqlNode, SqlSetOption.class);
    String scope = option.getScope();
    String name = option.getName();
    SqlNode value = option.getValue();
    OptionValue.OptionType type;
    if (value instanceof SqlLiteral) {
      switch (scope.toLowerCase()) {
        case "session":
          type = OptionValue.OptionType.SESSION;
          break;
        case "system":
          type = OptionValue.OptionType.SYSTEM;
          break;
//        case "query":
//          type = OptionValue.OptionType.QUERY;
//          break;
        default:
          throw new ValidationException("Invalid OPTION scope. Scope must be SESSION or SYSTEM.");
      }
      context.getOptions().setOption(name, (SqlLiteral) value, type);
    }else{
      throw new ValidationException("Sql options can only be literals.");
    }

    return DirectPlan.createDirectPlan(context, true, String.format("%s updated.", name));

  }



}
