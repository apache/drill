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
package org.apache.drill.exec.store.mapr.db;

import java.io.IOException;
import java.util.List;

import com.mapr.fs.tables.TableProperties;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.mapr.TableFormatMatcher;
import org.apache.drill.exec.store.mapr.TableFormatPlugin;

import com.mapr.fs.MapRFileStatus;
import org.apache.drill.exec.store.mapr.db.binary.MapRDBBinaryTable;
import org.apache.hadoop.fs.Path;

public class MapRDBFormatMatcher extends TableFormatMatcher {

  public MapRDBFormatMatcher(TableFormatPlugin plugin) {
    super(plugin);
  }

  @Override
  protected boolean isSupportedTable(MapRFileStatus status) throws IOException {
    return !getFormatPlugin()
        .getMaprFS()
        .getTableProperties(status.getPath())
        .getAttr()
        .getIsMarlinTable();
  }

  @Override
  public DrillTable isReadable(DrillFileSystem fs,
                               FileSelection selection, FileSystemPlugin fsPlugin,
                               String storageEngineName, String userName) throws IOException {

    if (isFileReadable(fs, selection.getFirstPath(fs))) {
      List<String> files = selection.getFiles();
      assert (files.size() == 1);
      String tableName = files.get(0);
      TableProperties props = getFormatPlugin().getMaprFS().getTableProperties(new Path(tableName));

      if (props.getAttr().getJson()) {
        return new DynamicDrillTable(fsPlugin, storageEngineName, userName,
            new FormatSelection(getFormatPlugin().getConfig(), selection));
      } else {
        FormatSelection formatSelection = new FormatSelection(getFormatPlugin().getConfig(), selection);
        return new MapRDBBinaryTable(storageEngineName, fsPlugin, (MapRDBFormatPlugin) getFormatPlugin(), formatSelection);
      }
    }
    return null;
  }

}
