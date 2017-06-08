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

import com.google.common.collect.Lists;
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

  private static final Configuration CONFIGURATION = new Configuration();
  private static final FsPermission FULL_PERMISSION = FsPermission.getDirDefault();
  private static final StorageStrategy PERSISTENT_STRATEGY = new StorageStrategy("002", false);
  private static final StorageStrategy TEMPORARY_STRATEGY = new StorageStrategy("077", true);
  private FileSystem FS;

  @Before
  public void setup() throws Exception {
    initFileSystem();
  }

  @Test
  public void testPermissionAndDeleteOnExitFalseForFileWithParent() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 2, true);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = PERSISTENT_STRATEGY.createFileAndApply(FS, file);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, file, true, 2, PERSISTENT_STRATEGY);
    checkDeleteOnExit(firstCreatedParentPath, true);
  }

  @Test
  public void testPermissionAndDeleteOnExitTrueForFileWithParent() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 2, true);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = TEMPORARY_STRATEGY.createFileAndApply(FS, file);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, file, true, 2, TEMPORARY_STRATEGY);
    checkDeleteOnExit(firstCreatedParentPath, false);
  }

  @Test
  public void testPermissionAndDeleteOnExitFalseForFileOnly() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);

    Path createdFile = PERSISTENT_STRATEGY.createFileAndApply(FS, file);

    assertEquals("Path should match", file, createdFile);
    checkPathAndPermission(initialPath, file, true, 0, PERSISTENT_STRATEGY);
    checkDeleteOnExit(file, true);
  }

  @Test
  public void testPermissionAndDeleteOnExitTrueForFileOnly() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);

    Path createdFile = TEMPORARY_STRATEGY.createFileAndApply(FS, file);

    assertEquals("Path should match", file, createdFile);
    checkPathAndPermission(initialPath, file, true, 0, TEMPORARY_STRATEGY);
    checkDeleteOnExit(file, false);
  }

  @Test(expected = IOException.class)
  public void testFailureOnExistentFile() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);
    FS.createNewFile(file);
    assertTrue("File should exist", FS.exists(file));
    try {
      PERSISTENT_STRATEGY.createFileAndApply(FS, file);
    } catch (IOException e) {
      assertEquals("Error message should match", String.format("File [%s] already exists on file system [%s].",
          file.toUri().getPath(), FS.getUri()), e.getMessage());
      throw e;
    }
  }

  @Test
  public void testCreatePathAndDeleteOnExitFalse() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path resultPath = addNLevelsAndFile(initialPath, 2, false);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = PERSISTENT_STRATEGY.createPathAndApply(FS, resultPath);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, resultPath, false, 2, PERSISTENT_STRATEGY);
    checkDeleteOnExit(firstCreatedParentPath, true);
  }

  @Test
  public void testCreatePathAndDeleteOnExitTrue() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path resultPath = addNLevelsAndFile(initialPath, 2, false);
    Path firstCreatedParentPath = addNLevelsAndFile(initialPath, 1, false);

    Path createdParentPath = TEMPORARY_STRATEGY.createPathAndApply(FS, resultPath);

    assertEquals("Path should match", firstCreatedParentPath, createdParentPath);
    checkPathAndPermission(initialPath, resultPath, false, 2, TEMPORARY_STRATEGY);
    checkDeleteOnExit(firstCreatedParentPath, false);
  }

  @Test
  public void testCreateNoPath() throws Exception {
    Path path = prepareStorageDirectory();

    Path createdParentPath = TEMPORARY_STRATEGY.createPathAndApply(FS, path);

    assertNull("Path should be null", createdParentPath);
    assertEquals("Permission should match", FULL_PERMISSION, FS.getFileStatus(path).getPermission());
  }

  @Test
  public void testStrategyForExistingFile() throws Exception {
    Path initialPath = prepareStorageDirectory();
    Path file = addNLevelsAndFile(initialPath, 0, true);
    FS.createNewFile(file);
    FS.setPermission(file, FULL_PERMISSION);

    assertTrue("File should exist", FS.exists(file));
    assertEquals("Permission should match", FULL_PERMISSION, FS.getFileStatus(file).getPermission());

    TEMPORARY_STRATEGY.applyToFile(FS, file);

    assertEquals("Permission should match", new FsPermission(TEMPORARY_STRATEGY.getFilePermission()),
        FS.getFileStatus(file).getPermission());
    checkDeleteOnExit(file, false);
  }

  @Test
  public void testInvalidUmask() throws Exception {
    for (String invalid : Lists.newArrayList("ABC", "999", null)) {
      StorageStrategy storageStrategy = new StorageStrategy(invalid, true);
      assertEquals("Umask value should match default", StorageStrategy.DEFAULT.getUmask(), storageStrategy.getUmask());
      assertTrue("deleteOnExit flag should be set to true", storageStrategy.isDeleteOnExit());
    }
  }

  private Path prepareStorageDirectory() throws IOException {
    File storageDirectory = Files.createTempDir();
    storageDirectory.deleteOnExit();
    Path path = new Path(storageDirectory.toURI().getPath());
    FS.setPermission(path, FULL_PERMISSION);
    return path;
  }

  private void initFileSystem() throws IOException {
    if (FS != null) {
      try {
        FS.close();
      } catch (Exception e) {
        // do nothing
      }
    }
    FS = FileSystem.get(CONFIGURATION);
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

    assertEquals("Path type should match", isFile, FS.isFile(resultPath));
    assertEquals("Permission should match", FULL_PERMISSION, FS.getFileStatus(initialPath).getPermission());

    if (isFile) {
      assertEquals("Permission should match", new FsPermission(storageStrategy.getFilePermission()),
          FS.getFileStatus(resultPath).getPermission());
    }
    Path startingPath = initialPath;
    FsPermission folderPermission = new FsPermission(storageStrategy.getFolderPermission());
    for (int i = 1; i <= levels; i++) {
      startingPath = new Path(startingPath, "level" + i);
      assertEquals("Permission should match", folderPermission, FS.getFileStatus(startingPath).getPermission());
    }
  }

  private void checkDeleteOnExit(Path path, boolean isPresent) throws IOException {
    assertTrue("Path should be present", FS.exists(path));
    // close and open file system to check for path presence
    initFileSystem();
    assertEquals("Path existence flag should match", isPresent, FS.exists(path));
  }
}
