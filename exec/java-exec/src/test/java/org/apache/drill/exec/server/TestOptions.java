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
package org.apache.drill.exec.server;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.UserExceptionMatcher;
import org.junit.Test;

import static org.apache.drill.exec.ExecConstants.ENABLE_VERBOSE_ERRORS_KEY;
import static org.apache.drill.exec.ExecConstants.SLICE_TARGET;
import static org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType.VALIDATION;

public class TestOptions extends BaseTestQuery{
//  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOptions.class);

  @Test
  public void testDrillbits() throws Exception{
    test("select * from sys.drillbits;");
  }

  @Test
  public void testOptions() throws Exception{
    test(
        "select * from sys.options;" +
        "ALTER SYSTEM set `planner.disable_exchanges` = true;" +
        "select * from sys.options;" +
        "ALTER SESSION set `planner.disable_exchanges` = true;" +
        "select * from sys.options;"
    );
  }

  @Test
  public void checkValidationException() throws Exception {
    thrownException.expect(new UserExceptionMatcher(VALIDATION));
    test("ALTER session SET %s = '%s';", SLICE_TARGET, "fail");
  }

  @Test // DRILL-3122
  public void checkChangedColumn() throws Exception {
    test("ALTER session SET `%s` = %d;", SLICE_TARGET,
      ExecConstants.SLICE_TARGET_DEFAULT);
    testBuilder()
        .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SESSION'", SLICE_TARGET)
        .unOrdered()
        .baselineColumns("status")
        .baselineValues("DEFAULT")
        .build()
        .run();
  }

  @Test
  public void setAndResetSessionOption() throws Exception {
    // check unchanged
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SESSION'", SLICE_TARGET)
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();

    // change option
    test("SET `%s` = %d;", SLICE_TARGET, 10);
    // check changed
    test("SELECT status, type, name FROM sys.options WHERE type = 'SESSION';");
    testBuilder()
      .sqlQuery("SELECT num_val FROM sys.options WHERE name = '%s' AND type = 'SESSION'", SLICE_TARGET)
      .unOrdered()
      .baselineColumns("num_val")
      .baselineValues((long) 10)
      .build()
      .run();

    // reset option
    test("RESET `%s`;", SLICE_TARGET);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SESSION'", SLICE_TARGET)
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
  }

  @Test
  public void setAndResetSystemOption() throws Exception {
    // check unchanged
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SYSTEM'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("status")
      .baselineValues("DEFAULT")
      .build()
      .run();

    // change option
    test("ALTER system SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE name = '%s' AND type = 'SYSTEM'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();

    // reset option
    test("ALTER system RESET `%s`;", ENABLE_VERBOSE_ERRORS_KEY);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SYSTEM'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("status")
      .baselineValues("DEFAULT")
      .build()
      .run();
  }

  @Test
  public void resetAllSessionOptions() throws Exception {
    // change options
    test("SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();

    // reset all options
    test("RESET ALL;");
    // check no session options changed
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE status <> 'DEFAULT' AND type = 'SESSION'")
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
  }

  @Test
  public void changeSessionAndSystemButRevertSession() throws Exception {
    // change options
    test("ALTER SESSION SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    test("ALTER SYSTEM SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();

    // reset session option
    test("RESET `%s`;", ENABLE_VERBOSE_ERRORS_KEY);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SESSION'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
    // check unchanged
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // reset system option
    test("ALTER SYSTEM RESET `%s`;", ENABLE_VERBOSE_ERRORS_KEY);
  }

  @Test
  public void changeSessionAndNotSystem() throws Exception {
    // change options
    test("ALTER SESSION SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    test("ALTER SYSTEM SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();

    // reset all session options
    test("ALTER SESSION RESET ALL;");
    // check no session options changed
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE status <> 'DEFAULT' AND type = 'SESSION'")
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
  }

  @Test
  public void changeSystemAndNotSession() throws Exception {
    // change options
    test("ALTER SESSION SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    test("ALTER SYSTEM SET `%s` = %b;", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();

    // reset option
    test("ALTER system RESET `%s`;", ENABLE_VERBOSE_ERRORS_KEY);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND type = 'SYSTEM'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("status")
      .baselineValues("DEFAULT")
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options WHERE type = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
  }

  @Test
  public void unsupportedLiteralValidation() throws Exception {
    thrownException.expect(new UserExceptionMatcher(VALIDATION,
      "Drill doesn't support assigning literals of type"));
    test("ALTER session SET `%s` = DATE '1995-01-01';", ENABLE_VERBOSE_ERRORS_KEY);
  }
}
