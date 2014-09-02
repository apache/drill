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
package org.apache.drill.exec.planner.sql.parser.impl;

import org.apache.drill.exec.planner.sql.parser.CompoundIdentifierConverter;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlAbstractParserImpl;
import org.eigenbase.sql.parser.SqlParserImplFactory;
import org.eigenbase.sql.util.SqlVisitor;

import java.io.Reader;

public class DrillParserWithCompoundIdConverter extends DrillParserImpl {

  /**
   * {@link org.eigenbase.sql.parser.SqlParserImplFactory} implementation for creating parser.
   */
  public static final SqlParserImplFactory FACTORY = new SqlParserImplFactory() {
    public SqlAbstractParserImpl getParser(Reader stream) {
      return new DrillParserWithCompoundIdConverter(stream);
    }
  };

  public DrillParserWithCompoundIdConverter(Reader stream) {
    super(stream);
  }

  protected SqlVisitor<SqlNode> createConverter() {
    return new CompoundIdentifierConverter();
  }

  @Override
  public SqlNode parseSqlExpressionEof() throws Exception {
    SqlNode originalSqlNode = super.parseSqlExpressionEof();
    return originalSqlNode.accept(createConverter());
  }

  @Override
  public SqlNode parseSqlStmtEof() throws Exception {
    SqlNode originalSqlNode = super.parseSqlStmtEof();
    return originalSqlNode.accept(createConverter());
  }
}
