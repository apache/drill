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
package org.apache.drill.exec.planner.sql;

import java.io.Reader;
import java.util.List;

import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.parser.SqlParserImplFactory;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.impl.SqlParserImpl;

public class DrillParserFactory implements SqlParserImplFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillParserFactory.class);

  private final SqlOperatorTable table;

  public DrillParserFactory(SqlOperatorTable table) {
    super();
    this.table = table;
  }

  public DrillParser getParser(Reader stream) {
    return new DrillParser(stream);
  }

  public class DrillParser extends SqlParserImpl {

    public DrillParser(Reader stream) {
      super(stream);
    }

    protected SqlCall createCall(SqlIdentifier funName, SqlParserPos pos, SqlFunctionCategory funcType,
        SqlLiteral functionQualifier, SqlNode[] operands) {
      SqlOperator fun = null;

      // First, try a half-hearted resolution as a builtin function.
      // If we find one, use it; this will guarantee that we
      // preserve the correct syntax (i.e. don't quote builtin function
      // / name when regenerating SQL).
      if (funName.isSimple()) {
        List<SqlOperator> list = table.lookupOperatorOverloads(funName, null, SqlSyntax.FUNCTION);
        if (list.size() == 1) {
          fun = list.get(0);
        }
      }

      // Otherwise, just create a placeholder function. Later, during
      // validation, it will be resolved into a real function reference.
      if (fun == null) {
        fun = new SqlFunction(funName, null, null, null, null, funcType);
      }

      return fun.createCall(functionQualifier, pos, operands);
    }

  }

}
