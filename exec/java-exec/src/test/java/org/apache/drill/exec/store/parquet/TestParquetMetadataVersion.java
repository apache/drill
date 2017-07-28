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

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestParquetMetadataVersion {

  @Test
  public void testCorrectOnlyMajorVersion() throws Exception {
    MetadataVersion parsedMetadataVersion = new MetadataVersion(MetadataVersion.Constants.V1);
    MetadataVersion expectedMetadataVersion = new MetadataVersion(1, 0);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedMetadataVersion, parsedMetadataVersion);
  }

  @Test
  public void testCorrectMajorMinorVersions() throws Exception {
    MetadataVersion parsedMetadataVersion = new MetadataVersion(MetadataVersion.Constants.V3_1);
    MetadataVersion expectedMetadataVersion = new MetadataVersion(3, 1);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedMetadataVersion, parsedMetadataVersion);
  }

  @Test
  public void testTwoDigitNumberMajorVersion() throws Exception {
    String twoDigitNumberMajorVersion = "v10.2";
    MetadataVersion parsedMetadataVersion = new MetadataVersion(twoDigitNumberMajorVersion);
    MetadataVersion expectedMetadataVersion = new MetadataVersion(10, 2);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedMetadataVersion, parsedMetadataVersion);
  }

  @Test
  public void testCorrectTwoDigitNumberMinorVersion() throws Exception {
    String twoDigitNumberMinorVersion = "v3.10";
    MetadataVersion parsedMetadataVersion = new MetadataVersion(twoDigitNumberMinorVersion);
    MetadataVersion expectedMetadataVersion = new MetadataVersion(3, 10);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedMetadataVersion, parsedMetadataVersion);
  }

  @Test(expected = DrillRuntimeException.class)
  public void testVersionWithoutFirstLetter() throws Exception {
    String versionWithoutFirstLetter = "3.1";
    try {
      new MetadataVersion(versionWithoutFirstLetter);
    } catch (DrillRuntimeException e) {
      assertTrue("Not expected exception is obtained while parsing parquet metadata version",
          e.getMessage().contains(String.format("Could not parse metadata version '%s'", versionWithoutFirstLetter)));
      throw e;
    }
  }

  @Test(expected = DrillRuntimeException.class)
  public void testVersionWithFirstLetterInUpperCase() throws Exception {
    String versionWithFirstLetterInUpperCase = "V2";
    try {
      new MetadataVersion(versionWithFirstLetterInUpperCase);
    } catch (DrillRuntimeException e) {
      assertTrue("Not expected exception is obtained while parsing parquet metadata version",
          e.getMessage().contains(String.format("Could not parse metadata version '%s'", versionWithFirstLetterInUpperCase)));
      throw e;
    }
  }

  @Test(expected = DrillRuntimeException.class)
  public void testVersionWithWrongDelimiter() throws Exception {
    String versionWithWrongDelimiter = "v3_1";
    try {
      new MetadataVersion(versionWithWrongDelimiter);
    } catch (DrillRuntimeException e) {
      assertTrue("Not expected exception is obtained while parsing parquet metadata version",
          e.getMessage().contains(String.format("Could not parse metadata version '%s'", versionWithWrongDelimiter)));
      throw e;
    }
  }

  @Test(expected = DrillRuntimeException.class)
  public void testZeroMajorVersion() throws Exception {
    String zeroMajorVersion = "v0.2";
    try {
      new MetadataVersion(zeroMajorVersion);
    } catch (DrillRuntimeException e) {
      assertTrue("Not expected exception is obtained while parsing parquet metadata version",
          e.getMessage().contains(String.format("Could not parse metadata version '%s'", zeroMajorVersion)));
      throw e;
    }
  }

  @Test(expected = DrillRuntimeException.class)
  public void testZeroMinorVersion() throws Exception {
    String zeroMinorVersion = "v3.0";
    try {
      new MetadataVersion(zeroMinorVersion);
    } catch (DrillRuntimeException e) {
      assertTrue("Not expected exception is obtained while parsing parquet metadata version",
          e.getMessage().contains(String.format("Could not parse metadata version '%s'", zeroMinorVersion)));
      throw e;
    }
  }

  @Test(expected = DrillRuntimeException.class)
  public void testVersionWithLetterInsteadOfNumber() throws Exception {
    String versionWithLetterInsteadOfNumber = "v3.O"; // "O" is a letter, not a zero
    try {
      new MetadataVersion(versionWithLetterInsteadOfNumber);
    } catch (DrillRuntimeException e) {
      assertTrue("Not expected exception is obtained while parsing parquet metadata version",
          e.getMessage().contains(String.format("Could not parse metadata version '%s'", versionWithLetterInsteadOfNumber)));
      throw e;
    }
  }
}
