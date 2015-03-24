/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.dfs;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.TestTools;
import org.junit.Test;

public class TestGlob extends BaseTestQuery {

    String MULTILEVEL = TestTools.getWorkingPath() + "/../java-exec/src/test/resources/multilevel";

    @Test
    public void testGlobSet() throws Exception {
        testBuilder()
            .sqlQuery(String.format("select count(*) from dfs_test.`%s/parquet/{1994,1995}`", MULTILEVEL))
            .unOrdered()
            .baselineColumns("EXPR$0")
            .baselineValues(80L)
            .build().run();
    }

    @Test
    public void testGlobWildcard() throws Exception {
        testBuilder()
            .sqlQuery(String.format("select count(*) from dfs_test.`%s/parquet/1994/*`", MULTILEVEL))
            .unOrdered()
            .baselineColumns("EXPR$0")
            .baselineValues(40L)
            .build().run();
    }

    @Test
    public void testGlobSingleCharacter() throws Exception {
        testBuilder()
            .sqlQuery(String.format("select count(*) from dfs_test.`%s/parquet/199?/*`", MULTILEVEL))
            .unOrdered()
            .baselineColumns("EXPR$0")
            .baselineValues(120L)
            .build().run();
    }

    @Test
    public void testGlobSingleCharacterRange() throws Exception {
        testBuilder()
            .sqlQuery(String.format("select count(*) from dfs_test.`%s/parquet/199[4-5]/*`", MULTILEVEL))
            .unOrdered()
            .baselineColumns("EXPR$0")
            .baselineValues(80L)
            .build().run();
    }
}
