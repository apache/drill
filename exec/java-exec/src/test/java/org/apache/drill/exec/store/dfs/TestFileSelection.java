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
package org.apache.drill.exec.store.dfs;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.drill.test.BaseTestQuery;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;

public class TestFileSelection extends BaseTestQuery {
  private static final List<FileStatus> EMPTY_STATUSES = ImmutableList.of();
  private static final List<String> EMPTY_FILES = ImmutableList.of();
  private static final String EMPTY_ROOT = "";

  @Test
  public void testCreateReturnsNullWhenArgumentsAreIllegal() {
    for (final Object statuses : new Object[] { null, EMPTY_STATUSES}) {
      for (final Object files : new Object[]{null, EMPTY_FILES}) {
        for (final Object root : new Object[]{null, EMPTY_ROOT}) {
          final FileSelection selection = FileSelection.create((List<FileStatus>) statuses, (List<String>) files,
              (String)root);
          assertNull(selection);
        }
      }
    }
  }

  @Test(expected = Exception.class)
  public void testEmptyFolderThrowsTableNotFound() throws Exception {
    final String emptyDirPath = dirTestWatcher.makeRootSubDir(Paths.get("empty")).getAbsolutePath();
    final String query = String.format("select * from dfs.`%s`", emptyDirPath);
    try {
      testNoResult(query);
    } catch (Exception ex) {
      final String pattern = String.format("%s' not found", emptyDirPath).toLowerCase();
      final boolean isTableNotFound = ex.getMessage().toLowerCase().contains(pattern);
      assertTrue(isTableNotFound);
      throw ex;
    }
  }
}
