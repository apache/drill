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
package org.apache.drill.exec.server;

import static org.apache.drill.exec.ExecConstants.ENABLE_VERBOSE_ERRORS_KEY;
import static org.apache.drill.exec.ExecConstants.SLICE_TARGET;

import org.apache.drill.categories.OptionsTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(OptionsTest.class)
public class TestOptions extends ClusterTest {
//  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOptions.class);

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testDrillbits() throws Exception {
    run("select * from sys.drillbits");
  }

  @Test
  public void testOptions() throws Exception{
    // We don't make use of client.alterSystem and alterSession because ALTER
    // statement execution is part of what we're testing in this class.
    client.exec(
        "select * from sys.options;" +
        "ALTER SYSTEM set `planner.disable_exchanges` = true;" +
        "select * from sys.options;" +
        "ALTER SESSION set `planner.disable_exchanges` = true;" +
        "select * from sys.options;"
    );
  }

  @Test
  public void checkValidationException() throws Exception {
    client.queryBuilder()
      .sql("ALTER session SET %s = '%s'", SLICE_TARGET, "fail")
      .userExceptionMatcher()
      .expectedType(ErrorType.VALIDATION)
      .match();
  }

  @Test // DRILL-3122
  public void checkChangedColumn() throws Exception {
    run("ALTER session SET `%s` = %d", SLICE_TARGET,
      ExecConstants.SLICE_TARGET_DEFAULT);
    testBuilder()
        .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND optionScope = 'SESSION'", SLICE_TARGET)
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
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND optionScope = 'SESSION'", SLICE_TARGET)
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();

    // change option
    run("SET `%s` = %d", SLICE_TARGET, 10);
    // check changed
    run("SELECT status, accessibleScopes, name FROM sys.options WHERE optionScope = 'SESSION'");
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE name = '%s' AND optionScope = 'SESSION'", SLICE_TARGET)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(10L))
      .build()
      .run();

    // reset option
    run("RESET `%s`", SLICE_TARGET);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND optionScope = 'SESSION'", SLICE_TARGET)
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
  }

  @Test
  public void setAndResetSystemOption() throws Exception {
    // check unchanged
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND optionScope = 'BOOT'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("status")
      .baselineValues("DEFAULT")
      .build()
      .run();

    // change option
    run("ALTER system SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE name = '%s' AND optionScope = 'SYSTEM'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();

    // reset option
    run("ALTER system RESET `%s`", ENABLE_VERBOSE_ERRORS_KEY);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND optionScope = 'BOOT'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("status")
      .baselineValues("DEFAULT")
      .build()
      .run();
  }

  @Test
  public void testResetAllSessionOptions() throws Exception {
    // change options
    run("SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();

    // reset all options
    run("RESET ALL");
    // check no session options changed
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE status <> 'DEFAULT' AND optionScope = 'SESSION'")
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
  }

  @Test
  public void changeSessionAndSystemButRevertSession() throws Exception {
    // change options
    run("ALTER SESSION SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    run("ALTER SYSTEM SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options_old WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options_old WHERE optionScope = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed new table
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();


    // reset session option
    run("RESET `%s`", ENABLE_VERBOSE_ERRORS_KEY);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE name = '%s' AND optionScope = 'SESSION'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
    // check unchanged
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE optionScope = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();
    // reset system option
    run("ALTER SYSTEM RESET `%s`", ENABLE_VERBOSE_ERRORS_KEY);
  }

  @Test
  public void changeSessionAndNotSystem() throws Exception {
    // change options
    run("ALTER SESSION SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    run("ALTER SYSTEM SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options_old WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options_old WHERE optionScope = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed for new table
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();

    // reset all session options
    run("ALTER SESSION RESET ALL");
    // check no session options changed
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options WHERE status <> 'DEFAULT' AND optionScope = 'SESSION'")
      .unOrdered()
      .expectsEmptyResultSet()
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE optionScope = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();
  }

  @Test
  public void changeSystemAndNotSession() throws Exception {
    // change options
    run("ALTER SESSION SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    run("ALTER SYSTEM SET `%s` = %b", ENABLE_VERBOSE_ERRORS_KEY, true);
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options_old WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT bool_val FROM sys.options_old WHERE optionScope = 'SYSTEM' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("bool_val")
      .baselineValues(true)
      .build()
      .run();
    // check changed in new table
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();

    // reset option
    run("ALTER system RESET `%s`", ENABLE_VERBOSE_ERRORS_KEY);
    // check reverted
    testBuilder()
      .sqlQuery("SELECT status FROM sys.options_old WHERE optionScope = 'BOOT' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("status")
      .baselineValues("DEFAULT")
      .build()
      .run();
    // check changed
    testBuilder()
      .sqlQuery("SELECT val FROM sys.options WHERE optionScope = 'SESSION' AND name = '%s'", ENABLE_VERBOSE_ERRORS_KEY)
      .unOrdered()
      .baselineColumns("val")
      .baselineValues(String.valueOf(true))
      .build()
      .run();
  }

  @Test
  public void unsupportedLiteralValidation() throws Exception {
    String query = "ALTER session SET `%s` = %s";

    client.queryBuilder()
      .sql(query, ENABLE_VERBOSE_ERRORS_KEY, "DATE '1995-01-01'")
      .userExceptionMatcher()
      .expectedType(ErrorType.VALIDATION)
      .include("Drill doesn't support assigning literals of type")
      .match();
  }
}
