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
package org.apache.drill.exec.planner.sql.parser;

import net.hydromatic.optiq.tools.Planner;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.parser.SqlParserPos;

/**
 * SqlCall interface with addition of method to get the handler.
 */
public abstract class DrillSqlCall extends SqlCall {

  public DrillSqlCall(SqlParserPos pos) {
    super(pos);
  }

  public AbstractSqlHandler getSqlHandler(Planner planner, QueryContext context) {
    return new DefaultSqlHandler(planner, context);
  }
}
