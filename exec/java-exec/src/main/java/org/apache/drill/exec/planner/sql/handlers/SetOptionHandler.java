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
import java.math.BigDecimal;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.calcite.util.NlsString;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;

public class SetOptionHandler extends AbstractSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  private final QueryContext context;

  public SetOptionHandler(QueryContext context) {
    this.context = context;
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final SqlSetOption option = unwrap(sqlNode, SqlSetOption.class);
    final String scope = option.getScope();
    final String name = option.getName();
    final SqlNode value = option.getValue();
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

      if (type == OptionType.SYSTEM) {
        // If the user authentication is enabled, make sure the user who is trying to change the system option has
        // administrative privileges.
        if (context.isUserAuthenticationEnabled() &&
            !ImpersonationUtil.hasAdminPrivileges(
                context.getQueryUserName(),
                context.getOptions().getOption(ExecConstants.ADMIN_USERS_KEY).string_val,
                context.getOptions().getOption(ExecConstants.ADMIN_USER_GROUPS_KEY).string_val)) {
          throw UserException.permissionError()
              .message("Not authorized to change SYSTEM options.")
              .build(logger);
        }
      }

      final OptionValue optionValue = createOptionValue(name, type, (SqlLiteral) value);
      context.getOptions().setOption(optionValue);
    }else{
      throw new ValidationException("Sql options can only be literals.");
    }

    return DirectPlan.createDirectPlan(context, true, String.format("%s updated.", name));
  }

  private static OptionValue createOptionValue(final String name, final OptionValue.OptionType type,
                                               final SqlLiteral literal) {
    final Object object = literal.getValue();
    final SqlTypeName typeName = literal.getTypeName();
    switch (typeName) {
    case DECIMAL: {
      final BigDecimal bigDecimal = (BigDecimal) object;
      if (bigDecimal.scale() == 0) {
        return OptionValue.createLong(type, name, bigDecimal.longValue());
      } else {
        return OptionValue.createDouble(type, name, bigDecimal.doubleValue());
      }
    }

    case DOUBLE:
    case FLOAT:
      return OptionValue.createDouble(type, name, ((BigDecimal) object).doubleValue());

    case SMALLINT:
    case TINYINT:
    case BIGINT:
    case INTEGER:
      return OptionValue.createLong(type, name, ((BigDecimal) object).longValue());

    case VARBINARY:
    case VARCHAR:
    case CHAR:
      return OptionValue.createString(type, name, ((NlsString) object).getValue());

    case BOOLEAN:
      return OptionValue.createBoolean(type, name, (Boolean) object);

    default:
      throw new ExpressionParsingException(String.format(
        "Drill doesn't support set option expressions with literals of type %s.", typeName));
    }
  }
}
