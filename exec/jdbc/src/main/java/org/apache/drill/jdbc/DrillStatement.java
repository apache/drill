/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.jdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;


/**
 * Drill-specific {@link Statement}.
 * @see #unwrap
 */
public interface DrillStatement extends Statement {

  /**
   * <strong>Drill</strong>:
   * Returns zero, indicating that no timeout is set.
   *
   * @throws  AlreadyClosedSqlException
   *            if connection is closed
   */
  @Override
  int getQueryTimeout() throws AlreadyClosedSqlException, SQLException;

  /**
   * <strong>Drill</strong>:
   * Supported (for non-zero timeout value).
   * <p>
   *   Normally, just throws {@link SQLFeatureNotSupportedException} unless
   *   request is trivially for no timeout (zero {@code milliseconds} value).
   * </p>
   * @throws  AlreadyClosedSqlException
   *            if connection is closed
   * @throws  JdbcApiSqlException
   *            if an invalid parameter value is detected (and not above case)
   * @throws  SQLFeatureNotSupportedException
   *            if timeout is non-zero (and not above case)
   */
  @Override
  void setQueryTimeout( int milliseconds )
      throws AlreadyClosedSqlException,
             JdbcApiSqlException,
             SQLFeatureNotSupportedException,
             SQLException;

  /**
   * {@inheritDoc}
   * <p>
   *   <strong>Drill</strong>: Does not throw SQLException.
   * </p>
   */
  @Override
  boolean isClosed();

  /**
   * <p>
   *   <strong>Drill</strong>: Identify if the statement has already timed out
   * </p>
   */
  boolean isTimedOut();

  /**
   * <p>
   *   <strong>Drill</strong>: Allows cancel due to timeout.
   *       The duration in which the cancellation occurred is retrieved from {@link Statement#getQueryTimeout() }.
   *       The statement, however, is not closed to allow access to most getter methods
   * </p>
   *
   * @throws SqlTimeoutException  if Timeout occurred (recommended exception)
   * @throws SQLException         Non-timeout related exceptions
   */
  void cancelDueToTimeout() throws SqlTimeoutException, SQLException;
}
