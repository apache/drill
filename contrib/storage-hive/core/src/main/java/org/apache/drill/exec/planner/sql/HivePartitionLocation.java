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
package org.apache.drill.exec.planner.sql;

import org.apache.drill.exec.planner.PartitionLocation;

public class HivePartitionLocation implements PartitionLocation {
  private final String partitionLocation;
  private final String[] partitionValue;
  // The path names passed in to this class are already sanitised and use the forward slash as the separator
  private static final String fileSeparator = "/";
  public HivePartitionLocation(int max, String baseTableLocation, String entireLocation) {
    this.partitionLocation = entireLocation;
    partitionValue = new String[max];
    int start = partitionLocation.indexOf(baseTableLocation) + baseTableLocation.length();
    String postPath = entireLocation.substring(start);
    if (postPath.length() == 0) {
      return;
    }
    if (postPath.startsWith(fileSeparator)) {
      postPath = postPath.substring(postPath.indexOf(fileSeparator) + 1);
    }
    String[] mostDirs = postPath.split(fileSeparator);
    assert mostDirs.length <= max;
    for (int i = 0; i < mostDirs.length; i++) {
      this.partitionValue[i] = mostDirs[i].substring(mostDirs[i].indexOf("=") + 1);
    }
  }
  @Override
  public String getPartitionValue(int index) {
    assert index < partitionValue.length;
    return partitionValue[index];
  }

  @Override
  public String getEntirePartitionLocation() {
    return partitionLocation;
  }
}
