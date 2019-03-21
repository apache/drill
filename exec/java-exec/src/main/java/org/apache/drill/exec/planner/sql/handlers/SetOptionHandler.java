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
import org.apache.drill.exec.server.options.OptionValue.OptionScope;
import org.apache.drill.exec.server.options.QueryOptionManager;
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

    final QueryOptionManager options = context.getOptions();
    final String scope = option.getScope();
    final OptionValue.OptionScope optionScope;
    if (scope == null) { // No scope mentioned assumed SESSION
      optionScope = OptionScope.SESSION;
    } else {
      switch (scope.toLowerCase()) {
      case "session":
        optionScope = OptionScope.SESSION;
        // Skip writing profiles for "ALTER SESSION SET" queries
        if (options.getBoolean(ExecConstants.SKIP_ALTER_SESSION_QUERY_PROFILE)) {
          logger.debug("Will not write profile for ALTER SESSION SET ... ");
          context.skipWritingProfile(true);
        }
        break;
      case "system":
        optionScope = OptionScope.SYSTEM;
        break;
      default:
        throw UserException.validationError()
            .message("Invalid OPTION scope %s. Scope must be SESSION or SYSTEM.", scope)
            .build(logger);
      }
    }

    if (optionScope == OptionScope.SYSTEM) {
      // If the user authentication is enabled, make sure the user who is trying to change the system option has
      // administrative privileges.
      if (context.isUserAuthenticationEnabled() &&
          !ImpersonationUtil.hasAdminPrivileges(
            context.getQueryUserName(),
            ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(options),
            ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(options))) {
        throw UserException.permissionError()
            .message("Not authorized to change SYSTEM options.")
            .build(logger);
      }
    }

    final String optionName = option.getName().toString();

    // Currently, we convert multi-part identifier to a string.
    final OptionManager chosenOptions = options.getOptionManager(optionScope);

    if (value != null) { // SET option
      final Object literalObj = sqlLiteralToObject((SqlLiteral) value);
      chosenOptions.setLocalOption(optionName, literalObj);
    } else { // RESET option
      if ("ALL".equalsIgnoreCase(optionName)) {
        chosenOptions.deleteAllLocalOptions();
      } else {
        chosenOptions.deleteLocalOption(optionName);
      }
    }

    return DirectPlan.createDirectPlan(context, true, String.format("%s updated.", optionName));
  }

  private static Object sqlLiteralToObject(final SqlLiteral literal) {
    final Object object = literal.getValue();
    final SqlTypeName typeName = literal.getTypeName();
    switch (typeName) {
    case DECIMAL: {
      final BigDecimal bigDecimal = (BigDecimal) object;
      if (bigDecimal.scale() == 0) {
        return bigDecimal.longValue();
      } else {
        return bigDecimal.doubleValue();
      }
    }

    case DOUBLE:
    case FLOAT:
      return ((BigDecimal) object).doubleValue();

    case SMALLINT:
    case TINYINT:
    case BIGINT:
    case INTEGER:
      return ((BigDecimal) object).longValue();

    case VARBINARY:
    case VARCHAR:
    case CHAR:
      return ((NlsString) object).getValue().toString();

    case BOOLEAN:
      return object;

    default:
      throw UserException.validationError()
        .message("Drill doesn't support assigning literals of type %s in SET statements.", typeName)
        .build(logger);
    }
  }
}
