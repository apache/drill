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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;

import org.apache.drill.shaded.guava.com.google.common.io.Files;

/**
 * Base class for "fixtures." Provides the basics such as the Drill
 * configuration, a memory allocator and so on.
 */

public class BaseFixture {

  protected DrillConfig config;
  protected BufferAllocator allocator;

  /**
   * Create a temp directory to store the given <i>dirName</i>. Directory will
   * be deleted on exit. Directory is created if it does not exist.
   *
   * @param dirName directory name
   * @return Full path including temp parent directory and given directory name.
   */

  public static File getTempDir(final String dirName) {
    final File dir = Files.createTempDir();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        FileUtils.deleteQuietly(dir);
      }
    });
    File tempDir = new File(dir, dirName);
    tempDir.mkdirs();
    return tempDir;
  }

  public BufferAllocator allocator() { return allocator; }
  public DrillConfig config() { return config; }
}
