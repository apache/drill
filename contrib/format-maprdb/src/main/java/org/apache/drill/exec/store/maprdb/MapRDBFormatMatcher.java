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
package org.apache.drill.exec.store.maprdb;

import com.mapr.fs.MapRFileStatus;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public class MapRDBFormatMatcher extends FormatMatcher {

  private FormatPlugin plugin;
  private DrillFileSystem fs;

  public MapRDBFormatMatcher(FormatPlugin plugin, DrillFileSystem fs) {
    this.plugin = plugin;
    this.fs = fs;
  }

  @Override
  public boolean supportDirectoryReads() {
    return false;
  }

  @Override
  public FormatSelection isReadable(FileSelection selection) throws IOException {
    FileStatus status = selection.getFirstPath(fs);
    if (status instanceof MapRFileStatus) {
      if (((MapRFileStatus) status).isTable()) {
        return new FormatSelection(getFormatPlugin().getConfig(), selection);
      }
    }
    return null;
  }

  @Override
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }
}
