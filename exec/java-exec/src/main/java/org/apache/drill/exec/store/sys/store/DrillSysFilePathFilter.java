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
package org.apache.drill.exec.store.sys.store;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Filter for Drill System Files
 */
public class DrillSysFilePathFilter implements PathFilter {

  //NOTE: The filename is a combination of query ID (which is monotonically
  //decreasing value derived off epoch timestamp) and a random value. This
  //filter helps eliminate that list
  String cutoffFileName = null;
  public DrillSysFilePathFilter() {}

  public DrillSysFilePathFilter(String cutoffSysFileName) {
    if (cutoffSysFileName != null) {
      this.cutoffFileName = cutoffSysFileName + DRILL_SYS_FILE_SUFFIX;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.fs.PathFilter#accept(org.apache.hadoop.fs.Path)
   */
  @Override
  public boolean accept(Path file){
    if (file.getName().endsWith(DRILL_SYS_FILE_SUFFIX)) {
      if (cutoffFileName != null) {
        return (file.getName().compareTo(cutoffFileName) <= 0);
      } else {
        return true;
      }
    }
    return false;
  }
}