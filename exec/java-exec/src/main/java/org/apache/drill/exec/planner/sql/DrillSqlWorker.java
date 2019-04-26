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
package org.apache.drill.exec.planner.sql;

import java.io.IOException;

import org.apache.calcite.sql.SqlDescribeSchema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.ops.QueryContext.SqlStatementType;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.AnalyzeTableHandler;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DescribeSchemaHandler;
import org.apache.drill.exec.planner.sql.handlers.DescribeTableHandler;
import org.apache.drill.exec.planner.sql.handlers.ExplainHandler;
import org.apache.drill.exec.planner.sql.handlers.RefreshMetadataHandler;
import org.apache.drill.exec.planner.sql.handlers.ResetOptionHandler;
import org.apache.drill.exec.planner.sql.handlers.SchemaHandler;
import org.apache.drill.exec.planner.sql.handlers.SetOptionHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.planner.sql.parser.DrillSqlCall;
import org.apache.drill.exec.planner.sql.parser.DrillSqlDescribeTable;
import org.apache.drill.exec.planner.sql.parser.DrillSqlResetOption;
import org.apache.drill.exec.planner.sql.parser.DrillSqlSetOption;
import org.apache.drill.exec.planner.sql.parser.SqlCreateTable;
import org.apache.drill.exec.planner.sql.parser.SqlSchema;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.hadoop.security.AccessControlException;

public class DrillSqlWorker {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlWorker.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(DrillSqlWorker.class);

  private DrillSqlWorker() {
  }

  /**
   * Converts sql query string into query physical plan.
   *
   * @param context query context
   * @param sql sql query
   * @return query physical plan
   */
  public static PhysicalPlan getPlan(QueryContext context, String sql) throws ForemanSetupException {
    return getPlan(context, sql, null);
  }

  /**
   * Converts sql query string into query physical plan.
   * Catches various exceptions and converts them into user exception when possible.
   *
   * @param context query context
   * @param sql sql query
   * @param textPlan text plan
   * @return query physical plan
   */
  public static PhysicalPlan getPlan(QueryContext context, String sql, Pointer<String> textPlan) throws ForemanSetupException {
    try {
      return convertPlan(context, sql, textPlan);
    } catch (ValidationException e) {
      String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
      throw UserException.validationError(e)
        .message(errorMessage)
        .build(logger);
    } catch (AccessControlException e) {
      throw UserException.permissionError(e)
        .build(logger);
    } catch (SqlUnsupportedException e) {
      throw UserException.unsupportedError(e)
        .build(logger);
    } catch (IOException | RelConversionException e) {
      throw new QueryInputException("Failure handling SQL.", e);
    }
  }

  /**
   * Converts sql query string into query physical plan.
   * In case of any errors (that might occur due to missing function implementation),
   * checks if local function registry should be synchronized with remote function registry.
   * If sync took place, reloads drill operator table
   * (since functions were added to / removed from local function registry)
   * and attempts to converts sql query string into query physical plan one more time.
   *
   * @param context query context
   * @param sql sql query
   * @param textPlan text plan
   * @return query physical plan
   */
  private static PhysicalPlan convertPlan(QueryContext context, String sql, Pointer<String> textPlan)
      throws ForemanSetupException, RelConversionException, IOException, ValidationException {
    Pointer<String> textPlanCopy = textPlan == null ? null : new Pointer<>(textPlan.value);
    try {
      return getQueryPlan(context, sql, textPlan);
    } catch (Exception e) {
      logger.trace("There was an error during conversion into physical plan. " +
          "Will sync remote and local function registries if needed and retry " +
          "in case if issue was due to missing function implementation.", e);
      if (context.getFunctionRegistry().syncWithRemoteRegistry(
          context.getDrillOperatorTable().getFunctionRegistryVersion())) {
        context.reloadDrillOperatorTable();
        logger.trace("Local function registry was synchronized with remote. Trying to find function one more time.");
        return getQueryPlan(context, sql, textPlanCopy);
      }
      throw e;
    }
  }

  /**
   * Converts sql query string into query physical plan.
   *
   * @param context query context
   * @param sql sql query
   * @param textPlan text plan
   * @return query physical plan
   */
  private static PhysicalPlan getQueryPlan(QueryContext context, String sql, Pointer<String> textPlan)
      throws ForemanSetupException, RelConversionException, IOException, ValidationException {

    final SqlConverter parser = new SqlConverter(context);
    injector.injectChecked(context.getExecutionControls(), "sql-parsing", ForemanSetupException.class);
    final SqlNode sqlNode = checkAndApplyAutoLimit(parser, context, sql);
    final AbstractSqlHandler handler;
    final SqlHandlerConfig config = new SqlHandlerConfig(context, parser);

    switch (sqlNode.getKind()) {
      case EXPLAIN:
        handler = new ExplainHandler(config, textPlan);
        context.setSQLStatementType(SqlStatementType.EXPLAIN);
        break;
      case SET_OPTION:
        if (sqlNode instanceof DrillSqlSetOption) {
          handler = new SetOptionHandler(context);
          context.setSQLStatementType(SqlStatementType.SETOPTION);
          break;
        }
        if (sqlNode instanceof DrillSqlResetOption) {
          handler = new ResetOptionHandler(context);
          context.setSQLStatementType(SqlStatementType.SETOPTION);
          break;
        }
      case DESCRIBE_TABLE:
        if (sqlNode instanceof DrillSqlDescribeTable) {
          handler = new DescribeTableHandler(config);
          context.setSQLStatementType(SqlStatementType.DESCRIBE_TABLE);
          break;
        }
      case DESCRIBE_SCHEMA:
        if (sqlNode instanceof SqlDescribeSchema) {
          handler = new DescribeSchemaHandler(config);
          context.setSQLStatementType(SqlStatementType.DESCRIBE_SCHEMA);
          break;
        }
        if (sqlNode instanceof SqlSchema.Describe) {
          handler = new SchemaHandler.Describe(config);
          context.setSQLStatementType(SqlStatementType.DESCRIBE_SCHEMA);
          break;
        }
      case CREATE_TABLE:
        handler = ((DrillSqlCall) sqlNode).getSqlHandler(config, textPlan);
        break;
      case DROP_TABLE:
      case CREATE_VIEW:
      case DROP_VIEW:
      case OTHER_DDL:
      case OTHER:
        if (sqlNode instanceof SqlCreateTable) {
          handler = ((DrillSqlCall) sqlNode).getSqlHandler(config, textPlan);
          context.setSQLStatementType(SqlStatementType.CTAS);
          break;
        }

        if (sqlNode instanceof DrillSqlCall) {
          handler = ((DrillSqlCall) sqlNode).getSqlHandler(config);
          if (handler instanceof AnalyzeTableHandler) {
            context.setSQLStatementType(SqlStatementType.ANALYZE);
          } else if (handler instanceof RefreshMetadataHandler) {
            context.setSQLStatementType(SqlStatementType.REFRESH);
          }
          break;
        }
        // fallthrough
      default:
        handler = new DefaultSqlHandler(config, textPlan);
        context.setSQLStatementType(SqlStatementType.OTHER);
    }

    // Determines whether result set should be returned for the query based on return result set option and sql node kind.
    // Overrides the option on a query level if it differs from the current value.
    boolean currentReturnResultValue = context.getOptions().getBoolean(ExecConstants.RETURN_RESULT_SET_FOR_DDL);
    boolean newReturnResultSetValue = currentReturnResultValue || !SqlKind.DDL.contains(sqlNode.getKind());
    if (newReturnResultSetValue != currentReturnResultValue) {
      context.getOptions().setLocalOption(ExecConstants.RETURN_RESULT_SET_FOR_DDL, true);
    }

    return handler.getPlan(sqlNode);
  }

  private static boolean isAutoLimitShouldBeApplied(SqlNode sqlNode, int queryMaxRows) {
    return (queryMaxRows > 0) && sqlNode.getKind().belongsTo(SqlKind.QUERY)
        && (sqlNode.getKind() != SqlKind.ORDER_BY || isAutoLimitLessThanOrderByFetch((SqlOrderBy) sqlNode, queryMaxRows));
  }

  private static SqlNode checkAndApplyAutoLimit(SqlConverter parser, QueryContext context, String sql) {
    SqlNode sqlNode = parser.parse(sql);
    int queryMaxRows = context.getOptions().getOption(ExecConstants.QUERY_MAX_ROWS).num_val.intValue();
    if (isAutoLimitShouldBeApplied(sqlNode, queryMaxRows)) {
      sqlNode = wrapWithAutoLimit(sqlNode, queryMaxRows);
    } else {
      //Force setting to zero IFF autoLimit was intended to be set originally but is inapplicable
      if (queryMaxRows > 0) {
        context.getOptions().setLocalOption(ExecConstants.QUERY_MAX_ROWS, 0);
      }
    }
    return sqlNode;
  }

  private static boolean isAutoLimitLessThanOrderByFetch(SqlOrderBy orderBy, int queryMaxRows) {
    return orderBy.fetch == null || Integer.parseInt(orderBy.fetch.toString()) > queryMaxRows;
  }

  private static SqlNode wrapWithAutoLimit(SqlNode sqlNode, int queryMaxRows) {
    SqlNumericLiteral autoLimitLiteral = SqlLiteral.createExactNumeric(String.valueOf(queryMaxRows), SqlParserPos.ZERO);
    if (sqlNode.getKind() == SqlKind.ORDER_BY) {
      SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
      return new SqlOrderBy(orderBy.getParserPosition(), orderBy.query, orderBy.orderList, orderBy.offset, autoLimitLiteral);
    }
    return new SqlOrderBy(SqlParserPos.ZERO, sqlNode, SqlNodeList.EMPTY, null, autoLimitLiteral);
  }
}
