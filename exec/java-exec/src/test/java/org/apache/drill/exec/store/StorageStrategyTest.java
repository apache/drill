/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StorageStrategyTest {

  private static final Configuration configuration = new Configuration();
  private static final FsPermission full_permission = new FsPermission("777");
  private static final StorageStrategy persistent_strategy = new StorageStrategy("775", "644", false);
  private static final StorageStrategy temporary_strategy = new StorageStrategy("700", "600", true);
  private FileSystem fs;

  @Before
  public void setup() throws Exception {
    initFileSystem();
  }

  @Test
  public void testPermissionAndDeleteOnExitFalseForFileWithParent() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 2, true);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = persistent_strategy.createFileAndApply(fs, file);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, file, true, 2, persistent_strategy);
    checkDeleteOnExit(firstCreatedParentPath, true);
  }

  @Test
  public void testPermissionAndDeleteOnExitTrueForFileWithParent() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 2, true);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = temporary_strategy.createFileAndApply(fs, file);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, file, true, 2, temporary_strategy);
    checkDeleteOnExit(firstCreatedParentPath, false);
  }

  @Test
  public void testPermissionAndDeleteOnExitFalseForFileOnly() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);

    Path createdFile = persistent_strategy.createFileAndApply(fs, file);

    assertEquals("Path should match", file, createdFile);
    checkPathAndPermission(initialPath, file, true, 0, persistent_strategy);
    checkDeleteOnExit(file, true);
  }

  @Test
  public void testPermissionAndDeleteOnExitTrueForFileOnly() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);

    Path createdFile = temporary_strategy.createFileAndApply(fs, file);

    assertEquals("Path should match", file, createdFile);
    checkPathAndPermission(initialPath, file, true, 0, temporary_strategy);
    checkDeleteOnExit(file, false);
  }

  @Test(expected = IOException.class)
  public void testFailureOnExistentFile() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);
    fs.createNewFile(file);
    assertTrue("File should exist", fs.exists(file));
    try {
      persistent_strategy.createFileAndApply(fs, file);
    } catch (IOException e) {
      assertEquals("Error message should match", String.format("File [%s] already exists on file system [%s].",
          file.toUri().getPath(), fs.getUri()), e.getMessage());
      throw e;
    }
  }

  @Test
  public void testCreatePathAndDeleteOnExitFalse() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path resultPath = addNLevelsAndFile(initialPath, 2, false);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = persistent_strategy.createPathAndApply(fs, resultPath);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, resultPath, false, 2, persistent_strategy);
    checkDeleteOnExit(firstCreatedParentPath, true);
  }

  @Test
  public void testCreatePathAndDeleteOnExitTrue() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path resultPath = addNLevelsAndFile(initialPath, 2, false);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = temporary_strategy.createPathAndApply(fs, resultPath);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, resultPath, false, 2, temporary_strategy);
    checkDeleteOnExit(firstCreatedParentPath, false);
  }

  @Test
  public void testCreateNoPath() throws Exception {
    Path path = prepareStorageDirectory();

    Path createdParentPath = temporary_strategy.createPathAndApply(fs, path);

    assertNull("Path should be null", createdParentPath);
    assertEquals("Permission should match", full_permission, fs.getFileStatus(path).getPermission());
  }

  @Test
  public void testStrategyForExistingFile() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);
    fs.createNewFile(file);
    fs.setPermission(file, full_permission);

    assertTrue("File should exist", fs.exists(file));
    assertEquals("Permission should match", full_permission, fs.getFileStatus(file).getPermission());

    temporary_strategy.applyToFile(fs, file);

    assertEquals("Permission should match", new FsPermission(temporary_strategy.getFilePermission()),
        fs.getFileStatus(file).getPermission());
    checkDeleteOnExit(file, false);
  }

  private Path prepareStorageDirectory() throws IOException {
    File storageDirectory = Files.createTempDir();
    storageDirectory.deleteOnExit();
    Path path = new Path(storageDirectory.toURI().getPath());
    fs.setPermission(path, full_permission);
    return path;
  }

  private void initFileSystem() throws IOException {
    if (fs != null) {
      try {
        fs.close();
      } catch (Exception e) {
        // do nothing
      }
    }
    fs = FileSystem.get(configuration);
  }

  private Path addNLevelsAndFile(Path initialPath, int levels, boolean addFile) {
    Path resultPath = initialPath;
    for (int i = 1; i <= levels; i++) {
      resultPath = new Path(resultPath, "level" + i);
    }
    if (addFile) {
      resultPath = new Path(resultPath, "test_file.txt");
    }
    return resultPath;
  }

  private void checkPathAndPermission(Path initialPath,
                                      Path resultPath,
                                      boolean isFile,
                                      int levels,
                                      StorageStrategy storageStrategy) throws IOException {

    assertEquals("Path type should match", isFile, fs.isFile(resultPath));
    assertEquals("Permission should match", full_permission, fs.getFileStatus(initialPath).getPermission());

    if (isFile) {
      assertEquals("Permission should match", new FsPermission(storageStrategy.getFilePermission()),
          fs.getFileStatus(resultPath).getPermission());
    }
    Path startingPath = initialPath;
    FsPermission folderPermission = new FsPermission(storageStrategy.getFolderPermission());
    for (int i = 1; i <= levels; i++) {
      startingPath = new Path(startingPath, "level" + i);
      assertEquals("Permission should match", folderPermission, fs.getFileStatus(startingPath).getPermission());
    }
  }

  private void checkDeleteOnExit(Path path, boolean isPresent) throws IOException {
    assertTrue("Path should be present", fs.exists(path));
    // close and open file system to check for path presence
    initFileSystem();
    assertEquals("Path existence flag should match", isPresent, fs.exists(path));
  }
}
