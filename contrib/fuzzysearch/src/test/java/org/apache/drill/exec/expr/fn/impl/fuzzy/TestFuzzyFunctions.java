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
package org.apache.drill.exec.expr.fn.impl.fuzzy;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestFuzzyFunctions extends BaseTestQuery {

  @Test
  public void testLevenshteinSameTexts() throws Exception {

    testBuilder()
    .sqlQuery("select levenshtein(columns[0], columns[1]) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'same'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues((float)1.0)
    .build()
    .run();
  }

  @Test
  public void testCosineSimilaritySameTexts() throws Exception {

    testBuilder()
    .sqlQuery("select cosine_similarity(columns[0], columns[1]) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'same'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues((float)1.0)
    .build()
    .run();
  }

  @Test
  public void testJaroSameTexts() throws Exception {

    testBuilder()
    .sqlQuery("select jaro(columns[0], columns[1]) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'same'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues((float)1.0)
    .build()
    .run();
  }

  @Test
  public void testLevenshteinSimilarTexts() throws Exception {

    testBuilder()
    .sqlQuery("select round(levenshtein(columns[0], columns[1]), 4) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'similar-lev-test'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(0.7813)
    .build()
    .run();
  }

  @Test
  public void testCosineSimilaritySimilarTexts() throws Exception {

    testBuilder()
    .sqlQuery("select round(cosine_similarity(columns[0], columns[1]), 4) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'similar-cosine-test'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(0.4472)
    .build()
    .run();
  }

  @Test
  public void testJaroSimilarTexts() throws Exception {

    testBuilder()
    .sqlQuery("select round(jaro(columns[0], columns[1]), 4) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'similar-cosine-test'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(0.7383)
    .build()
    .run();
  }


  @Test
  public void testLevenshteinDifferentTexts() throws Exception {

    testBuilder()
    .sqlQuery("select round(levenshtein(columns[0], columns[1]), 4) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'different'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(0.0)
    .build()
    .run();
  }

  @Test
  public void testCosineDifferentSimilarTexts() throws Exception {

    testBuilder()
    .sqlQuery("select round(cosine_similarity(columns[0], columns[1]), 4) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'different'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(0.0)
    .build()
    .run();
  }

  @Test
  public void testJaroDifferentTexts() throws Exception {

    testBuilder()
    .sqlQuery("select round(jaro(columns[0], columns[1]), 4) "
        + "from cp.`/sample-data/similarities.csv` where columns[2] = 'different'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(0.0)
    .build()
    .run();
  }

}