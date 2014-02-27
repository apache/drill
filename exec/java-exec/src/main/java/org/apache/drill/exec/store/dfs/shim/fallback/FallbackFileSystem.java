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
package org.apache.drill.exec.store.dfs.shim.fallback;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.dfs.shim.DrillInputStream;
import org.apache.drill.exec.store.dfs.shim.DrillOutputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

public class FallbackFileSystem extends DrillFileSystem {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FallbackFileSystem.class);

  final FileSystem fs;

  public FallbackFileSystem(DrillConfig config, FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public FileSystem getUnderlying() {
    return fs;
  }

  @Override
  public List<FileStatus> list(boolean recursive, Path... paths) throws IOException {
    if (recursive) {
      List<FileStatus> statuses = Lists.newArrayList();
      for (Path p : paths) {
        addRecursiveStatus(fs.getFileStatus(p), statuses);
      }
      return statuses;

    } else {
      return Lists.newArrayList(fs.listStatus(paths));
    }
  }

  
  private void addRecursiveStatus(FileStatus parent, List<FileStatus> listToFill) throws IOException {
    if (parent.isDir()) {
      Path pattern = new Path(parent.getPath(), "*");
      FileStatus[] sub = fs.globStatus(pattern);
      for(FileStatus s : sub){
        listToFill.add(s);
      }
    } else {
      listToFill.add(parent);
    }
  }

  @Override
  public FileStatus getFileStatus(Path p) throws IOException {
    return fs.getFileStatus(p);
  }

  @Override
  public DrillOutputStream create(Path p) throws IOException {
    return new Out(fs.create(p));
  }

  @Override
  public DrillInputStream open(Path p) throws IOException {
    return new In(fs.open(p));
  }

  @Override
  public void close() throws Exception {
    fs.close();
  }

  @Override
  public BlockLocation[] getBlockLocations(FileStatus status, long start, long len) throws IOException {
    return fs.getFileBlockLocations(status, start, len);
  }

  private class Out extends DrillOutputStream {

    private final FSDataOutputStream out;
    
    public Out(FSDataOutputStream out) {
      super();
      this.out = out;
    }

    @Override
    public void close() throws Exception {
      out.close();
    }

    @Override
    public FSDataOutputStream getOuputStream() {
      return out;
    }

  }

  private class In extends DrillInputStream {

    private final FSDataInputStream in;
    
    public In(FSDataInputStream in) {
      super();
      this.in = in;
    }

    @Override
    public FSDataInputStream getInputStream() {
      return in;
    }

    @Override
    public void close() throws Exception {
      in.close();
    }

  }



}
