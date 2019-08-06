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

import org.apache.drill.categories.ParquetTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.parquet.metadata.MetadataVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({ParquetTest.class, UnlikelyTest.class})
public class TestParquetMetadataVersion {

  @Test
  public void testFirstLetter() throws Exception {
    MetadataVersion versionWithFirstLetter = new MetadataVersion("v4");
    MetadataVersion expectedVersion = new MetadataVersion(4, 0);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedVersion, versionWithFirstLetter);
    MetadataVersion versionWithoutFirstLetter = new MetadataVersion("4");
    assertEquals("Parquet metadata version is parsed incorrectly", expectedVersion, versionWithoutFirstLetter);
  }

  @Test(expected = DrillRuntimeException.class)
  public void testWrongFirstLetter() throws Exception {
    String versionWithFirstLetterInUpperCase = "V2";
    try {
      new MetadataVersion(versionWithFirstLetterInUpperCase);
    } catch (DrillRuntimeException e) {
      assertTrue("Not expected exception is obtained while parsing parquet metadata version",
          e.getMessage().contains(String.format("Could not parse metadata version '%s'", versionWithFirstLetterInUpperCase)));
      throw e;
    }
  }

  @Test
  public void testTwoDigitsMajorVersion() throws Exception {
    MetadataVersion twoDigitsMetadataVersion = new MetadataVersion("10.2");
    MetadataVersion expectedVersion = new MetadataVersion(10, 2);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedVersion, twoDigitsMetadataVersion);
  }

  @Test
  public void testMinorVersion() throws Exception {
    MetadataVersion withMinorVersion = new MetadataVersion("3.1");
    MetadataVersion expectedVersionWithMinorVersion = new MetadataVersion(3, 1);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedVersionWithMinorVersion, withMinorVersion);
  }

  @Test
  public void testTwoDigitsMinorVersion() throws Exception {
    MetadataVersion twoDigitsMinorVersion = new MetadataVersion("3.13");
    MetadataVersion expectedVersionWithTwoDigitsMinorVersion = new MetadataVersion(3, 13);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedVersionWithTwoDigitsMinorVersion, twoDigitsMinorVersion);
  }

  @Test
  public void testWithoutMinorVersion() throws Exception {
    MetadataVersion withoutMinorVersion = new MetadataVersion("v3");
    MetadataVersion expectedVersionWithoutMinorVersion = new MetadataVersion(3, 0);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedVersionWithoutMinorVersion, withoutMinorVersion);
  }

  @Test
  public void testZeroMinorVersion() throws Exception {
    MetadataVersion zeroMinorVersion = new MetadataVersion("4.0");
    MetadataVersion expectedVersionZeroMinorVersion = new MetadataVersion(4, 0);
    assertEquals("Parquet metadata version is parsed incorrectly", expectedVersionZeroMinorVersion, zeroMinorVersion);
  }


  @Test(expected = DrillRuntimeException.class)
  public void testWrongDelimiter() throws Exception {
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
