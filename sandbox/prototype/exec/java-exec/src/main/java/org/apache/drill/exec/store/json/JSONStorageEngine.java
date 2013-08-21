/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.json;

import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class JSONStorageEngine extends AbstractStorageEngine {
  private final JSONStorageEngineConfig config;
  private final Configuration conf;
  private FileSystem fileSystem;
  public static final String HADOOP_DEFAULT_NAME = "fs.default.name";

  public JSONStorageEngine(JSONStorageEngineConfig config, DrillbitContext context) {
    this.config = config;
    try {
      this.conf = new Configuration();
      this.conf.set(HADOOP_DEFAULT_NAME, config.getDfsName());
      this.fileSystem = FileSystem.get(conf);

    } catch (IOException ie) {
      throw new RuntimeException("Error setting up filesystem");
    }
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }
}
