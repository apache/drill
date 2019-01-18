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
package org.apache.drill.exec.hive;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

public class HiveTestUtilities {

  /**
   * Set of all posix permissions to be assigned to newly created file in
   * {@link HiveTestUtilities#createDirWithPosixPermissions(File, String)}
   */
  private static final Set<PosixFilePermission> ALL_POSIX_PERMISSIONS = EnumSet.allOf(PosixFilePermission.class);

  /**
   * Execute the give <i>query</i> on given <i>hiveDriver</i> instance. If a {@link CommandNeedRetryException}
   * exception is thrown, it tries upto 3 times before returning failure.
   * @param hiveDriver
   * @param query
   */
  public static void executeQuery(Driver hiveDriver, String query) {
    CommandProcessorResponse response = null;
    boolean failed = false;
    int retryCount = 3;

    try {
      response = hiveDriver.run(query);
    } catch(CommandNeedRetryException ex) {
      if (--retryCount == 0) {
        failed = true;
      }
    }

    if (failed || response.getResponseCode() != 0 ) {
      throw new RuntimeException(String.format("Failed to execute command '%s', errorMsg = '%s'",
          query, (response != null ? response.getErrorMessage() : "")));
    }
  }

  /**
   * Creates desired directory structure and
   * adds all posix permissions to created directory.
   *
   * @param parentDir parent directory
   * @param dirName directory name
   * @return file representing created dir with all posix permissions
   */
  public static File createDirWithPosixPermissions(File parentDir, String dirName) {
    File dir = new File(parentDir, dirName);
    dir.mkdirs();
    Path path = dir.toPath();
    try {
      Files.setPosixFilePermissions(path, ALL_POSIX_PERMISSIONS);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to set all posix permissions for directory [%s]", dir), e);
    }
    return dir;
  }

}
