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
package org.apache.drill.exec.planner.sql;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import org.apache.drill.exec.store.PlanCacheTable;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.easy.text.TextFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.test.BaseTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestTextPlanCacheMetadata extends BaseTest {
  private static final String ALICE = "plan-cache-alice";
  private static final String BOB = "plan-cache-bob";
  private static final URI URI_ROOT = URI.create("plan-cache-test:///");

  /** Models a remote filesystem whose client identity is fixed at creation. */
  public static class UserScopedFileSystem extends RawLocalFileSystem {
    private String userName;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
      userName = UserGroupInformation.getCurrentUser().getShortUserName();
    }

    @Override
    public URI getUri() {
      return URI_ROOT;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      if (!path.getParent().getName().equals(userName)) {
        throw new AccessControlException("File metadata is private to " + path.getParent().getName());
      }
      return new FileStatus(10, false, 1, 1024, 1, path);
    }
  }

  @Test
  public void testVersionReadsUseScanIdentityAndKeepUsersSeparate() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, URI_ROOT.toString());
    conf.setClass("fs.plan-cache-test.impl", UserScopedFileSystem.class, FileSystem.class);
    TextFormatConfig formatConfig = new TextFormatConfig(Collections.singletonList("csv"),
        null, null, null, null, null, null, null);
    TextFormatPlugin format = new TextFormatPlugin("csv", null, conf, null, formatConfig);
    FileSystemPlugin storage = mock(FileSystemPlugin.class, CALLS_REAL_METHODS);
    doReturn(format).when(storage).getFormatPlugin(formatConfig);
    FormatSelection alice = selection(ALICE, formatConfig);
    FormatSelection bob = selection(BOB, formatConfig);

    PlanCacheTable first = storage.planCacheTable(ALICE, alice);
    assertEquals("10:1", first.getVersion());
    // A different user must not inherit Alice's client or permissions.
    assertThrows(AccessControlException.class, () -> storage.planCacheTable(BOB, alice));
    assertEquals("10:1", storage.planCacheTable(BOB, bob).getVersion());
    assertEquals(first.getIdentifier(), storage.planCacheTable(ALICE, alice).getIdentifier());
  }

  private FormatSelection selection(String userName, TextFormatConfig formatConfig) {
    Path path = new Path(URI_ROOT.toString() + userName + "/data.csv");
    FileStatus status = new FileStatus(10, false, 1, 1024, 1, path);
    return new FormatSelection(formatConfig,
        FileSelection.create(Collections.singletonList(status), null, path));
  }
}
