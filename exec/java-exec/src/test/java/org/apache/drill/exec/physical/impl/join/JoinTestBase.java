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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.PlanTestBase;
import org.junit.experimental.categories.Category;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


@Category(OperatorTest.class)
public class JoinTestBase extends PlanTestBase {

  private static final String testEmptyJoin = "select count(*) as cnt from cp.`employee.json` emp %s join dfs.`dept.json` " +
          "as dept on dept.manager = emp.`last_name`";

  /**
   * This method runs a join query with one of the table generated as an
   * empty json file.
   * @param testDir in which the empty json file is generated.
   * @param joinType to be executed.
   * @param joinPattern to look for the pattern in the successful run.
   * @param result number of the output rows.
   */
  public void testJoinWithEmptyFile(File testDir, String joinType,
                         String joinPattern, long result) throws Exception {
    buildFile("dept.json", new String[0], testDir);
    String query = String.format(testEmptyJoin, joinType);
    testPlanMatchingPatterns(query, new String[]{joinPattern}, new String[]{});
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("cnt")
            .baselineValues(result)
            .build().run();
  }

  private void buildFile(String fileName, String[] data, File testDir) throws IOException {
    try(PrintWriter out = new PrintWriter(new FileWriter(new File(testDir, fileName)))) {
      for (String line : data) {
        out.println(line);
      }
    }
  }
}
