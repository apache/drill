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

import static org.junit.Assert.assertEquals;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

public class TestDrillSQLWorker {

  private void validateFormattedIs(String sql, SqlParserPos pos, String expected) {
    String formatted = SqlConverter.formatSQLParsingError(sql, pos);
    assertEquals(expected, formatted);
  }

  @Test
  public void testErrorFormating() {
    String sql = "Select * from Foo\nwhere tadadidada;\n";
    validateFormattedIs(sql, new SqlParserPos(1, 2),
        "Select * from Foo\n"
      + " ^\n"
      + "where tadadidada;\n");
    validateFormattedIs(sql, new SqlParserPos(2, 2),
        "Select * from Foo\n"
      + "where tadadidada;\n"
      + " ^\n" );
    validateFormattedIs(sql, new SqlParserPos(1, 10),
        "Select * from Foo\n"
      + "         ^\n"
      + "where tadadidada;\n");
    validateFormattedIs(sql, new SqlParserPos(-11, -10), sql);
    validateFormattedIs(sql, new SqlParserPos(0, 10), sql);
    validateFormattedIs(sql, new SqlParserPos(100, 10), sql);
  }
}
