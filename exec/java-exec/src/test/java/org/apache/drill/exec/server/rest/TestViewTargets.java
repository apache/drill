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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.mock.MockStorageEngineConfig;
import org.junit.Test;

/**
 * A view can only be created in a writable file-based workspace. Both halves of that
 * rule matter: plugins such as Splunk, Kudu and JDBC report IS_MUTABLE = YES yet reject
 * CREATE VIEW, because only WorkspaceSchema overrides createView.
 */
public class TestViewTargets {

  private static FileSystemConfig fileConfig() {
    return new FileSystemConfig("file:///", null, null, null, null);
  }

  @Test
  public void writableFileWorkspaceIsAViewTarget() {
    assertTrue(MetadataResources.isViewTarget(fileConfig(), "YES"));
  }

  @Test
  public void nonWritableFileWorkspaceIsNotAViewTarget() {
    assertFalse(MetadataResources.isViewTarget(fileConfig(), "NO"));
  }

  /** The case the endpoint exists for: mutable, but CREATE VIEW would throw. */
  @Test
  public void mutableNonFilePluginIsNotAViewTarget() {
    assertFalse(MetadataResources.isViewTarget(MockStorageEngineConfig.INSTANCE, "YES"));
  }

  /** A schema whose plugin is absent from the registry must not be offered. */
  @Test
  public void unknownPluginIsNotAViewTarget() {
    assertFalse(MetadataResources.isViewTarget(null, "YES"));
  }
}
