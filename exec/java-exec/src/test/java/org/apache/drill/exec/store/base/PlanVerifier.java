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
package org.apache.drill.exec.store.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;

import org.apache.drill.test.ClientFixture;

/**
 * Verifier for execution plans. A handy tool to ensure that the
 * planner produces the expected plan given some set of conditions.
 * <p>
 * The test works by comparing the actual {@code EXPLAIN} output
 * to a "golden" file with the expected ("golden") plan.
 * <p>
 * To create a test, just write it and let it fail due to a missing file.
 * The output will go to the console. Inspect it. If it looks good,
 * copy the plan to the golden file and run again.
 * <p>
 * If comparison fails, you can optionally ask the verifier to write the
 * output to {@code /tmp/drill/test} so you can compare the golden and actual
 * outputs using your favorite diff tool to understand changes. If the changes
 * are expected, use that same IDE to copy changes from the actual
 * to the golden file.
 * <p>
 * The JSON properties of the serialized classes are all controlled
 * to have a fixed order to ensure that files compare across test
 * runs. If you see spurious failures do to changed JSON order, consider
 * adding a {@code @JsonPropertyOrder} tag to enforce a consistent order.
 * <p>
 * A fancier version of this class would use a regex or other mechanism
 * to say "ignore differences in this bit of the plan." For example, when
 * using systems with metadata, the exact values might bounce around
 * some.
 */
public class PlanVerifier {

  private final String basePath;

  private boolean saveResults;

  public PlanVerifier(String goldenFileDir) {
    this.basePath = goldenFileDir;
  }

  /**
   * Persist test results. Turn this on if you have failing tests.
   * Then, you can diff the actual and golden results in your favorite
   * tool or IDE.
   */
  public void saveResults(boolean flag) {
    saveResults = flag;
  }

  protected void verifyPlan(ClientFixture client, String sql, String expectedFile) throws Exception {
    String plan = client.queryBuilder().sql(sql).explainJson();
    verify(plan, expectedFile);
  }

  protected void verify(String actual, String relativePath) {
    URL url = getClass().getResource(basePath + relativePath);
    if (url == null) {

      // We' re about to fail. Before we do, dump the plan to output
      // so you can inspect the output, verify it is what you want,
      // and copy it into a golden file. Then, the next run will
      // pass. So, output goes to the console only if the test will
      // fail because the golden output file is missing.
      System.out.println(actual);
    }
    assertNotNull("Golden file is missing: " + relativePath, url);
    File resource = new File(url.getPath());
    try {
      verify(actual, resource);
    } catch (AssertionError e) {

      // If a test fails, it is handy to persist the results.
      // Done only when requested.
      if (saveResults) {
        System.out.println(actual);
        File dest = new File("/tmp/drill/test", basePath);
        File destFile = new File(dest, relativePath);
        dest.mkdirs();
        try (PrintWriter out = new PrintWriter(destFile)) {
          out.println(actual);
        } catch (FileNotFoundException e1) {
          fail("Cannnot save actual results to " + destFile.getAbsolutePath());
        }
      }
      throw e;
    }
  }

  protected void verify(String actual, File resource) {
    try (Reader expected = new FileReader(resource)) {
      verify(new StringReader(actual), expected);
    } catch (FileNotFoundException e) {
      fail("Missing resource file: " + resource.getAbsolutePath());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  private void verify(Reader actualReader, Reader expectedReader) throws IOException {
    try (BufferedReader actual = new BufferedReader(actualReader);
         BufferedReader expected = new BufferedReader(expectedReader);) {
      for (int lineNo = 1; ;lineNo++ ) {
        String aLine = actual.readLine();
        String eLine = expected.readLine();
        if (aLine == null && eLine == null) {
          break;
        }
        if (eLine == null) {
          fail("Too many actual lines");
        }
        if (aLine == null) {
          fail("Missing actual lines");
        }
        assertEquals("Line: " + lineNo, eLine, aLine);
      }
    }
  }
}
