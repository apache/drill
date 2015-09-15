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

import java.math.BigDecimal;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;

import org.apache.calcite.util.NlsString;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;

/**
 * Converts a {@link SqlNode} representing "ALTER .. SET option = value" and "ALTER ... RESET ..." statements to a
 * {@link PhysicalPlan}. See {@link SqlSetOption}. These statements have side effects i.e. the options within the
 * system context or the session context are modified. The resulting {@link DirectPlan} returns to the client a string
 * that is the name of the option that was updated.
 */
public class SetOptionHandler extends AbstractSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  private final QueryContext context;

  public SetOptionHandler(QueryContext context) {
    this.context = context;
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, ForemanSetupException {
    final SqlSetOption option = unwrap(sqlNode, SqlSetOption.class);
    final SqlNode value = option.getValue();
    if (value != null && !(value instanceof SqlLiteral)) {
      throw UserException.validationError()
          .message("Drill does not support assigning non-literal values in SET statements.")
          .build(logger);
    }

    final String scope = option.getScope();
    final OptionValue.OptionType type;
    if (scope == null) { // No scope mentioned assumed SESSION
      type = OptionType.SESSION;
    } else {
      switch (scope.toLowerCase()) {
      case "session":
        type = OptionType.SESSION;
        break;
      case "system":
        type = OptionType.SYSTEM;
        break;
      default:
        throw UserException.validationError()
            .message("Invalid OPTION scope %s. Scope must be SESSION or SYSTEM.", scope)
            .build(logger);
      }
    }

    final OptionManager options = context.getOptions();
    if (type == OptionType.SYSTEM) {
      // If the user authentication is enabled, make sure the user who is trying to change the system option has
      // administrative privileges.
      if (context.isUserAuthenticationEnabled() &&
          !ImpersonationUtil.hasAdminPrivileges(
            context.getQueryUserName(),
            options.getOption(ExecConstants.ADMIN_USERS_VALIDATOR),
            options.getOption(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR))) {
        throw UserException.permissionError()
            .message("Not authorized to change SYSTEM options.")
            .build(logger);
      }
    }

    // Currently, we convert multi-part identifier to a string.
    final String name = option.getName().toString();
    if (value != null) { // SET option
      final OptionValue optionValue = createOptionValue(name, type, (SqlLiteral) value);
      options.setOption(optionValue);
    } else { // RESET option
      if ("ALL".equalsIgnoreCase(name)) {
        options.deleteAllOptions(type);
      } else {
        options.deleteOption(name, type);
      }
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
      throw UserException.validationError()
        .message("Drill doesn't support assigning literals of type %s in SET statements.", typeName)
        .build(logger);
    }
  }
}
