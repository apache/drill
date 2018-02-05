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

package org.apache.drill.test;

import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * <p>
 * This is a {@link DirTestWatcher} which creates all the temporary directories required by a Drillbit and the various <b>dfs.*</b> storage workspaces. It also
 * provides convenience methods that do the following:
 * </p>
 *
 * <ol>
 *   <li>Copy project files to temp directories. This is useful for copying the sample data into a temp directory.</li>
 *   <li>Copy resource files to temp.</li>
 *   <li>Updating parquet metadata files.</li>
 * </ol>
 *
 * <p>
 *   The {@link BaseDirTestWatcher} is used in {@link BaseTestQuery} and an example of how it is used in conjunction with the {@link ClusterFixture} can be found in
 *   {@link ExampleTest}.
 * </p>
 */
public class BaseDirTestWatcher extends DirTestWatcher {
  /**
   * An enum used to represent the directories mapped to the <b>dfs.root</b> and <b>dfs.tmp</b> workspaces repectively.
   */
  public enum DirType {
    ROOT, // Corresponds to the directory that should be mapped to dfs.root
    TEST_TMP // Corresponds to the directory that should be mapped to dfs.tmp
  }

  private File codegenDir;
  private File tmpDir;
  private File storeDir;
  private File dfsTestTmpParentDir;
  private File dfsTestTmpDir;
  private File rootDir;

  /**
   * Creates a {@link BaseDirTestWatcher} which does not delete it's temp directories at the end of tests.
   */
  public BaseDirTestWatcher() {
    super();
  }

  /**
   * Creates a {@link BaseDirTestWatcher}.
   * @param deleteDirAtEnd If true, temp directories are deleted at the end of tests. If false, temp directories are not deleted at the end of tests.
   */
  public BaseDirTestWatcher(boolean deleteDirAtEnd) {
    super(deleteDirAtEnd);
  }

  @Override
  protected void starting(Description description) {
    super.starting(description);

    codegenDir = makeSubDir(Paths.get("codegen"));
    rootDir = makeSubDir(Paths.get("root"));
    tmpDir = makeSubDir(Paths.get("tmp"));
    storeDir = makeSubDir(Paths.get("store"));
    dfsTestTmpParentDir = makeSubDir(Paths.get("dfsTestTmp"));

    newDfsTestTmpDir();
  }

  /**
   * Clear contents of cluster directories
   */
  public void clear() {
    try {
      FileUtils.cleanDirectory(rootDir);
      FileUtils.cleanDirectory(tmpDir);
      FileUtils.cleanDirectory(storeDir);
      FileUtils.cleanDirectory(dfsTestTmpDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the temp directory that should be used as a Drillbit's tmp directory.
   * @return The temp directory that should be used as a Drillbit's tmp directory.
   */
  public File getTmpDir() {
    return tmpDir;
  }

  /**
   * Gets the temp directory that should be used by the {@link org.apache.drill.exec.store.sys.store.LocalPersistentStore}.
   * @return The temp directory that should be used by the {@link org.apache.drill.exec.store.sys.store.LocalPersistentStore}.
   */
  public File getStoreDir() {
    return storeDir;
  }

  /**
   * Gets the temp directory that should be used by the <b>dfs.tmp</b> workspace.
   * @return The temp directory that should be used by the <b>dfs.tmp</b> workspace.
   */
  public File getDfsTestTmpDir() {
    return dfsTestTmpDir;
  }

  /**
   * Gets the temp directory that should be used to hold the contents of the <b>dfs.root</b> workspace.
   * @return The temp directory that should be used to hold the contents of the <b>dfs.root</b> workspace.
   */
  public File getRootDir() {
    return rootDir;
  }

  /**
   * Gets the temp directory that should be used to save generated code files.
   * @return The temp directory that should be used to save generated code files.
   */
  public File getCodegenDir() {
    return codegenDir;
  }

  /**
   * This methods creates a new directory which can be mapped to <b>dfs.tmp</b>.
   */
  public void newDfsTestTmpDir() {
    dfsTestTmpDir = DirTestWatcher.createTempDir(dfsTestTmpParentDir);
  }

  /**
   * A helper method which returns the correct directory corresponding to the given {@link DirType}.
   * @param type The directory to return.
   * @return The directory corresponding to the given {@link DirType}.
   */
  private File getDir(DirType type) {
    switch (type) {
      case ROOT:
        return rootDir;
      case TEST_TMP:
        return dfsTestTmpDir;
      default:
        throw new IllegalArgumentException(String.format("Unsupported type %s", type));
    }
  }

  /**
   * Creates a directory in the temp root directory (corresponding to <b>dfs.root</b>) at the given relative path.
   * @param relPath The relative path in the temp root directory at which to create a directory.
   * @return The {@link java.io.File} corresponding to the sub directory that was created.
   */
  public File makeRootSubDir(Path relPath) {
    return makeSubDir(relPath, DirType.ROOT);
  }

  /**
   * Creates a directory in the temp tmp directory (corresponding to <b>dfs.tmp</b>) at the given relative path.
   * @param relPath The relative path in the temp tmp directory at which to create a directory.
   * @return The {@link java.io.File} corresponding to the sub directory that was created.
   */
  public File makeTestTmpSubDir(Path relPath) {
    return makeSubDir(relPath, DirType.TEST_TMP);
  }

  private File makeSubDir(Path relPath, DirType type) {
    File subDir = getDir(type)
      .toPath()
      .resolve(relPath)
      .toFile();
    subDir.mkdirs();
    return subDir;
  }

  /**
   * This copies a file or directory from <b>src/test/resources</b> into the temp root directory (corresponding to <b>dfs.root</b>). The relative path of the file or
   * directory in <b>src/test/resources</b> is preserved in the temp root directory.
   * @param relPath The relative path of the file or directory in <b>src/test/resources</b> to copy into the root temp folder.
   * @return The {@link java.io.File} corresponding to the copied file or directory in the temp root directory.
   */
  public File copyResourceToRoot(Path relPath) {
    return copyTo(relPath, relPath, TestTools.FileSource.RESOURCE, DirType.ROOT);
  }

  /**
   * This copies a filed or directory from the maven project into the temp root directory (corresponding to <b>dfs.root</b>). The relative path of the file or directory
   * in the maven module is preserved in the temp root directory.
   * @param relPath The relative path of the file or directory in the maven module to copy into the root temp folder.
   * @return The {@link java.io.File} corresponding to the copied file or directory in the temp root directory.
   */
  public File copyFileToRoot(Path relPath) {
    return copyTo(relPath, relPath, TestTools.FileSource.PROJECT, DirType.ROOT);
  }

  /**
   * This copies a file or directory from <b>src/test/resources</b> into the temp root directory (corresponding to <b>dfs.root</b>). The file or directory is copied
   * to the provided relative destPath in the temp root directory.
   * @param relPath The source relative path of a file or directory from <b>src/test/resources</b> that will be copied.
   * @param destPath The destination relative path of the file or directory in the temp root directory.
   * @return The {@link java.io.File} corresponding to the final copied file or directory in the temp root directory.
   */
  public File copyResourceToRoot(Path relPath, Path destPath) {
    return copyTo(relPath, destPath, TestTools.FileSource.RESOURCE, DirType.ROOT);
  }

  /**
   * This copies a file or directory from <b>src/test/resources</b> into the temp root directory (corresponding to <b>dfs.root</b>). The file or directory is copied
   * to the provided relative destPath in the temp root directory.
   * @param relPath The source relative path of a file or directory from <b>src/test/resources</b> that will be copied.
   * @param destPath The destination relative path of the file or directory in the temp root directory.
   * @return The {@link java.io.File} corresponding to the final copied file or directory in the temp root directory.
   */
  public File copyResourceToTestTmp(Path relPath, Path destPath) {
    return copyTo(relPath, destPath, TestTools.FileSource.RESOURCE, DirType.TEST_TMP);
  }

  private File copyTo(Path relPath, Path destPath, TestTools.FileSource fileSource, DirType dirType) {
    File file = TestTools.getFile(relPath, fileSource);

    if (file.isDirectory()) {
      File subDir = makeSubDir(destPath, dirType);
      TestTools.copyDirToDest(relPath, subDir, fileSource);
      return subDir;
    } else {
      File baseDir = getDir(dirType);

      baseDir.toPath()
        .resolve(destPath)
        .getParent()
        .toFile()
        .mkdirs();

      File destFile = baseDir.toPath()
        .resolve(destPath)
        .toFile();

      try {
        destFile.createNewFile();
        FileUtils.copyFile(file, destFile);
      } catch (IOException e) {
        throw new RuntimeException("This should not happen", e);
      }

      return destFile;
    }
  }

  /**
   * This is a convenience method that replaces placeholders in test parquet metadata files.
   * @param metaDataFile The parquet metadata file to do string replacement on.
   * @param replacePath The path to replace <b>REPLACED_IN_TEST</b> with in the parquet metadata file.
   * @param customStringReplacement If this is provided a <b>CUSTOM_STRING_REPLACEMENT</b> is replaced in the parquet metadata file with this string.
   */
  public void replaceMetaDataContents(File metaDataFile, File replacePath, String customStringReplacement) {
    try {
      String metadataFileContents = FileUtils.readFileToString(metaDataFile, Charsets.UTF_8);

      if (customStringReplacement != null) {
        metadataFileContents = metadataFileContents.replace("CUSTOM_STRING_REPLACEMENT", customStringReplacement);
      }

      metadataFileContents = metadataFileContents.replace("REPLACED_IN_TEST", replacePath.getCanonicalPath());
      FileUtils.write(metaDataFile, metadataFileContents, Charsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("This should not happen", e);
    }
  }
}
