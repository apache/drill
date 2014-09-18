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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Jackson serializable description of a file selection. Maintains an internal set of file statuses. However, also
 * serializes out as a list of Strings. All accessing methods first regenerate the FileStatus objects if they are not
 * available.  This allows internal movement of FileStatus and the ability to serialize if need be.
 */
public class FileSelection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);

  @JsonIgnore
  private List<FileStatus> statuses;

  public List<String> files;
  public String selectionRoot;

  public FileSelection() {
  }

  public FileSelection(List<String> files, String selectionRoot, boolean dummy) {
    this.files = files;
    this.selectionRoot = selectionRoot;
  }

  public FileSelection(List<String> files, boolean dummy) {
    this.files = files;
  }

  public FileSelection(List<FileStatus> statuses) {
    this(statuses, null);
  }

  public FileSelection(List<FileStatus> statuses, String selectionRoot) {
    this.statuses = statuses;
    this.files = Lists.newArrayList();
    for (FileStatus f : statuses) {
      files.add(f.getPath().toString());
    }
    this.selectionRoot = selectionRoot;
  }

  public boolean containsDirectories(DrillFileSystem fs) throws IOException {
    init(fs);
    for (FileStatus p : statuses) {
      if (p.isDir()) {
        return true;
      }
    }
    return false;
  }

  public FileSelection minusDirectories(DrillFileSystem fs) throws IOException {
    init(fs);
    List<FileStatus> newList = Lists.newArrayList();
    for (FileStatus p : statuses) {
      if (p.isDir()) {
        List<FileStatus> statuses = fs.list(true, p.getPath());
        for (FileStatus s : statuses) {
          newList.add(s);
        }
      } else {
        newList.add(p);
      }
    }
    return new FileSelection(newList, selectionRoot);
  }

  public FileStatus getFirstPath(DrillFileSystem fs) throws IOException {
    init(fs);
    return statuses.get(0);
  }

  public List<String> getAsFiles() {
    if (!files.isEmpty()) {
      return files;
    }
    if (statuses == null) {
      return Collections.emptyList();
    }
    List<String> files = Lists.newArrayList();
    for (FileStatus s : statuses) {
      files.add(s.getPath().toString());
    }
    return files;
  }

  private void init(DrillFileSystem fs) throws IOException {
    if (files != null && statuses == null) {
      statuses = Lists.newArrayList();
      for (String p : files) {
        statuses.add(fs.getFileStatus(new Path(p)));
      }
    }
  }

  public List<FileStatus> getFileStatusList(DrillFileSystem fs) throws IOException {
    init(fs);
    return statuses;
  }

  public static FileSelection create(DrillFileSystem fs, String parent, String path) throws IOException {
    if ( !(path.contains("*") || path.contains("?")) ) {
      Path p = new Path(parent, removeLeadingSlash(path));
      FileStatus status = fs.getFileStatus(p);
      return new FileSelection(Collections.singletonList(status), p.toUri().getPath());
    } else {
      Path p = new Path(parent,removeLeadingSlash(path));
      FileStatus[] status = fs.getUnderlying().globStatus(p);
      if (status == null || status.length == 0) {
        return null;
      }
      String[] s = p.toUri().getPath().split("/");
      String newPath = StringUtils.join(ArrayUtils.subarray(s, 0, s.length - 1), "/");
      Preconditions.checkState(!newPath.contains("*") && !newPath.contains("?"), String.format("Unsupported selection path: %s", p));
      return new FileSelection(Lists.newArrayList(status), newPath);
    }
  }

  private static String removeLeadingSlash(String path) {
    if (path.charAt(0) == '/') {
      String newPath = path.substring(1);
      return removeLeadingSlash(newPath);
    } else {
      return path;
    }
  }

}
