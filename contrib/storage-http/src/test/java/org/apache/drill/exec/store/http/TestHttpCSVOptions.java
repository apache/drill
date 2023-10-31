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

package org.apache.drill.exec.store.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.util.DrillFileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestHttpCSVOptions {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static String CSV_OPTIONS_JSON;

  @BeforeAll
  public static void setup() throws Exception {
    CSV_OPTIONS_JSON = Files.asCharSource(
        DrillFileUtils.getResourceAsFile("/data/csvOptions.json"), Charsets.UTF_8
    ).read().trim();
  }


  @Test
  void testBuilderDefaults() {
    HttpCSVOptions options = HttpCSVOptions.builder().build();

    assertEquals(",", options.getDelimiter());
    assertEquals('"', options.getQuote());
    assertEquals('"', options.getQuoteEscape());
    assertEquals("\n", options.getLineSeparator());
    assertNull(options.getHeaderExtractionEnabled());
    assertEquals(0, options.getNumberOfRowsToSkip());
    assertEquals(-1, options.getNumberOfRecordsToRead());
    assertTrue(options.isLineSeparatorDetectionEnabled());
    assertEquals(512, options.getMaxColumns());
    assertEquals(4096, options.getMaxCharsPerColumn());
    assertTrue(options.isSkipEmptyLines());
    assertTrue(options.isIgnoreLeadingWhitespaces());
    assertTrue(options.isIgnoreTrailingWhitespaces());
    assertNull(options.getNullValue());
  }

  @Test
  void testBuilderOverride() {
    HttpCSVOptions options = HttpCSVOptions.builder()
        .delimiter(";")
        .quote('\'')
        .quoteEscape('\\')
        .lineSeparator("\r\n")
        .headerExtractionEnabled(false)
        .numberOfRowsToSkip(5)
        .numberOfRecordsToRead(10)
        .lineSeparatorDetectionEnabled(false)
        .maxColumns(1024)
        .maxCharsPerColumn(8192)
        .skipEmptyLines(false)
        .ignoreLeadingWhitespaces(false)
        .ignoreTrailingWhitespaces(false)
        .nullValue("NULL")
        .build();

    assertEquals(";", options.getDelimiter());
    assertEquals('\'', options.getQuote());
    assertEquals('\\', options.getQuoteEscape());
    assertEquals("\r\n", options.getLineSeparator());
    assertFalse(options.getHeaderExtractionEnabled());
    assertEquals(5, options.getNumberOfRowsToSkip());
    assertEquals(10, options.getNumberOfRecordsToRead());
    assertFalse(options.isLineSeparatorDetectionEnabled());
    assertEquals(1024, options.getMaxColumns());
    assertEquals(8192, options.getMaxCharsPerColumn());
    assertFalse(options.isSkipEmptyLines());
    assertFalse(options.isIgnoreLeadingWhitespaces());
    assertFalse(options.isIgnoreTrailingWhitespaces());
    assertEquals("NULL", options.getNullValue());
  }

  @Test
  void testJSONSerialization() throws Exception {
    HttpCSVOptions options = HttpCSVOptions.builder()
        .delimiter(";")
        .quote('\'')
        .quoteEscape('\\')
        .lineSeparator("\r\n")
        .headerExtractionEnabled(false)
        .numberOfRowsToSkip(5)
        .numberOfRecordsToRead(10)
        .lineSeparatorDetectionEnabled(false)
        .maxColumns(1024)
        .maxCharsPerColumn(8192)
        .skipEmptyLines(false)
        .ignoreLeadingWhitespaces(false)
        .ignoreTrailingWhitespaces(false)
        .nullValue("NULL")
        .build();

    String json = objectMapper.writeValueAsString(options);

    assertNotNull(json);
    assertEquals(CSV_OPTIONS_JSON, json);
  }

  @Test
  public void testJSONDeserialization() throws JsonProcessingException {
    HttpCSVOptions options = objectMapper.readValue(CSV_OPTIONS_JSON, HttpCSVOptions.class);

    assertEquals(";", options.getDelimiter());
    assertEquals('\'', options.getQuote());
    assertEquals('\\', options.getQuoteEscape());
    assertEquals("\r\n", options.getLineSeparator());
    assertNull(options.getHeaderExtractionEnabled());
    assertEquals(5, options.getNumberOfRowsToSkip());
    assertEquals(10, options.getNumberOfRecordsToRead());
    assertTrue(options.isLineSeparatorDetectionEnabled());
    assertEquals(1024, options.getMaxColumns());
    assertEquals(8192, options.getMaxCharsPerColumn());
    assertTrue(options.isSkipEmptyLines());
    assertTrue(options.isIgnoreLeadingWhitespaces());
    assertTrue(options.isIgnoreTrailingWhitespaces());
    assertEquals("NULL", options.getNullValue());

  }

}
