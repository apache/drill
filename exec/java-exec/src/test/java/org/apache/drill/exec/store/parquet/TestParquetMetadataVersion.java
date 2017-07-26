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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.Version;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestParquetMetadataVersion {

  private static String TEST_EXCEPTION_MESSAGE = "Failure is expected while parsing the wrong metadata version";
  private static String TEST_ASSERT_EQUALS_MESSAGE = "Parquet metadata version is parsed incorrectly";
  private static String TEST_ASSERT_TRUE_MESSAGE = "Not expected exception is obtained while parsing parquet metadata version";

  @Test
  public void testCorrectOnlyMajorVersion() throws Exception {
    String correctOnlyMajorVersion = MetadataVersions.V1;
    Version parsedMetadataVersion = MetadataVersions.VersionParser.parse(correctOnlyMajorVersion);
    Version expectedMetadataVersion = new Version(correctOnlyMajorVersion, 1, 0, 0, 0, "");
    assertEquals(TEST_ASSERT_EQUALS_MESSAGE, expectedMetadataVersion, parsedMetadataVersion);
  }

  @Test
  public void testCorrectMajorMinorVersion() throws Exception {
    String correctMajorMinorVersion = MetadataVersions.V3_1;
    Version parsedMetadataVersion = MetadataVersions.VersionParser.parse(correctMajorMinorVersion);
    Version expectedMetadataVersion = new Version(correctMajorMinorVersion, 3, 1, 0, 0, "");
    assertEquals(TEST_ASSERT_EQUALS_MESSAGE, expectedMetadataVersion, parsedMetadataVersion);
  }

  @Test
  public void testVersionWithoutFirstLetter() throws Exception {
    String versionWithoutFirstLetter = "3.1";
    try {
      MetadataVersions.VersionParser.parse(versionWithoutFirstLetter);
    } catch (DrillRuntimeException e) {
      assertTrue(TEST_ASSERT_TRUE_MESSAGE, e.getMessage().contains("Could not parse metadata"));
      return;
    }
    throw new Exception(TEST_EXCEPTION_MESSAGE);
  }

  @Test
  public void testVersionWithFirstLetterInUpperCase() throws Exception {
    String versionWithFirstLetterInUpperCase = "V2";
    try {
      MetadataVersions.VersionParser.parse(versionWithFirstLetterInUpperCase);
    } catch (DrillRuntimeException e) {
      assertTrue(TEST_ASSERT_TRUE_MESSAGE, e.getMessage().contains("Could not parse metadata"));
      return;
    }
    throw new Exception(TEST_EXCEPTION_MESSAGE);
  }

  @Test
  public void testVersionWithWrongDelimiter() throws Exception {
    String versionWithWrongDelimiter = "v3_1";
    try {
      MetadataVersions.VersionParser.parse(versionWithWrongDelimiter);
    } catch (DrillRuntimeException e) {
      assertTrue(TEST_ASSERT_TRUE_MESSAGE, e.getMessage().contains("Could not parse metadata"));
      return;
    }
    throw new Exception(TEST_EXCEPTION_MESSAGE);
  }

  @Test
  public void testVersionWithLetterInsteadOfNumber() throws Exception {
    String versionWithLetterInsteadOfNumber = "v3.O"; // "O" is a letter, not a zero
    try {
      MetadataVersions.VersionParser.parse(versionWithLetterInsteadOfNumber);
    } catch (DrillRuntimeException e) {
      assertTrue(TEST_ASSERT_TRUE_MESSAGE, e.getMessage().contains("Could not parse metadata"));
      return;
    }
    throw new Exception(TEST_EXCEPTION_MESSAGE);
  }
}
