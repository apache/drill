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
package org.apache.drill.exec.planner;

import org.apache.hadoop.fs.Path;

/**
 * Class defines a single partition in a DFS table.
 */
public class DFSPartitionLocation implements PartitionLocation {
  private final String[] dirs;
  private final String file;

  public DFSPartitionLocation(int max, String selectionRoot, String file) {
    this.file = file;
    this.dirs = new String[max];

    // strip the scheme and authority if they exist
    selectionRoot = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString();

    int start = file.indexOf(selectionRoot) + selectionRoot.length();
    String postPath = file.substring(start);
    if (postPath.length() == 0) {
      return;
    }
    if(postPath.charAt(0) == '/'){
      postPath = postPath.substring(1);
    }
    String[] mostDirs = postPath.split("/");
    int maxLoop = Math.min(max, mostDirs.length - 1);
    for(int i =0; i < maxLoop; i++){
      this.dirs[i] = mostDirs[i];
    }
  }

  /**
   * Returns the value for a give partition key
   * @param index - Index of the partition key whose value is to be returned
   * @return
   */
  @Override
  public String getPartitionValue(int index) {
    assert index < dirs.length;
    return dirs[index];
  }

  /**
   * Return the full location of this partition
   * @return
   */
  @Override
  public String getEntirePartitionLocation() {
    return file;
  }
}

