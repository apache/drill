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

package org.apache.drill.testutils;

import org.apache.commons.io.FileUtils;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;

/**
 * This JUnit {@link TestWatcher} creates a unique directory for each JUnit test in the project's
 * target folder at the start of each test. This directory can be used as a temporary directory to store
 * files for the test. The directory and its contents are deleted at the end of the test.
 */
public class DirTestWatcher extends TestWatcher {
  private String dirPath;
  private File dir;

  @Override
  protected void starting(Description description) {
    dirPath = String.format("%s/target/%s/%s", new File(".").getAbsolutePath(),
      description.getClassName(), description.getMethodName());
    dir = new File(dirPath);
    dir.mkdirs();
  }

  @Override
  protected void finished(Description description) {
    FileUtils.deleteQuietly(dir);
  }

  @Override
  protected void failed(Throwable e, Description description) {
    FileUtils.deleteQuietly(dir);
  }

  public String getDirPath() {
    return dirPath;
  }

  public File getDir() {
    return dir;
  }
}