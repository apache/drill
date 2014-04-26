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
package org.apache.drill.exec.store.dfs.shim;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Wraps the underlying filesystem to provide advanced file system features. Delegates to underlying file system if
 * those features are exposed.
 */
public abstract class DrillFileSystem implements AutoCloseable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFileSystem.class);

  public abstract FileSystem getUnderlying();
  
  public abstract BlockLocation[] getBlockLocations(FileStatus status, long start, long length) throws IOException;
  public abstract List<FileStatus> list(boolean recursive, Path... paths) throws IOException;
  public abstract FileStatus getFileStatus(Path p) throws IOException;
  public abstract DrillOutputStream create(Path p) throws IOException;
  public abstract DrillInputStream open(Path p) throws IOException;
  
}
