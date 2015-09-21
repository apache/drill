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
package org.apache.drill.jdbc;

import java.sql.Statement;  // (for Javadoc reference(s))


/**
 * SQLException for object-already-closed conditions, e.g., calling a method
 * on a closed {@link Statement}.
 */
public class AlreadyClosedSqlException extends JdbcApiSqlException {

  private static final long serialVersionUID = 2015_03_25L;

  /**
   * See {@link JdbcApiSqlException#JdbcApiSqlException(String, String, int)}.
   */
  public AlreadyClosedSqlException( String reason,
                                    String SQLState,
                                    int vendorCode ) {
    super( reason, SQLState, vendorCode );
  }

  /**
   * See {@link JdbcApiSqlException#JdbcApiSqlException(String, String)}.
   */
  public AlreadyClosedSqlException( String reason, String SQLState ) {
    super( reason, SQLState );
  }

  /**
   * See {@link JdbcApiSqlException#JdbcApiSqlException(String)}.
   */
  public AlreadyClosedSqlException( String reason ) {
    super( reason );
  }

  /**
   * See {@link JdbcApiSqlException#JdbcApiSqlException()}.
   */
  public AlreadyClosedSqlException() {
    super();
  }

  /**
   * See {@link JdbcApiSqlException#JdbcApiSqlException(Throwable cause)}.
   */
  public AlreadyClosedSqlException( Throwable cause ) {
    super( cause );
  }

  /**
   * See {@link JdbcApiSqlException#JdbcApiSqlException(String, Throwable)}.
   */
  public AlreadyClosedSqlException( String reason, Throwable cause ) {
    super( reason, cause );
  }

  /**
   * See
   * {@link JdbcApiSqlException#JdbcApiSqlException(String, String, Throwable)}.
   */
  public AlreadyClosedSqlException( String reason, String sqlState,
                                         Throwable cause ) {
    super( reason, sqlState, cause );
  }

  /**
   * See
   * {@link JdbcApiSqlException#JdbcApiSqlException(String, String, int, Throwable)}.
   */
  public AlreadyClosedSqlException( String reason,
                                    String sqlState,
                                    int vendorCode,
                                    Throwable cause ) {
    super( reason, sqlState, vendorCode, cause );
  }

}
