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
package org.apache.drill.exec.server.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dashboard must distinguish "not configured" from "no usage", and must not
 * report zero merely because the active log has rolled into an archive.
 */
public class AiAnalyticsReadinessTest {

  @Test
  public void testEventLogPresentWhenActiveFileExists(@TempDir Path dir) throws IOException {
    new File(dir.toFile(), "ai-events.log").createNewFile();
    assertTrue(AiAnalyticsResources.hasEventLog(dir.toString()));
  }

  @Test
  public void testEventLogPresentWhenOnlyArchivesRemain(@TempDir Path dir) throws IOException {
    new File(dir.toFile(), "ai-events-2026-07-01.0.log").createNewFile();
    assertTrue(AiAnalyticsResources.hasEventLog(dir.toString()));
  }

  @Test
  public void testNoEventLogWhenDirectoryEmpty(@TempDir Path dir) {
    assertFalse(AiAnalyticsResources.hasEventLog(dir.toString()));
  }

  @Test
  public void testNoEventLogWhenDirectoryMissing() {
    assertFalse(AiAnalyticsResources.hasEventLog(null));
  }
}
