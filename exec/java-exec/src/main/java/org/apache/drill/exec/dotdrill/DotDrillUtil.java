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
package org.apache.drill.exec.dotdrill;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

public class DotDrillUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DotDrillUtil.class);

  private static List<DotDrillFile> getDrillFiles(DrillFileSystem fs, FileStatus[] statuses, DotDrillType... types){
    List<DotDrillFile> files = Lists.newArrayList();
    for(FileStatus s : statuses){
      DotDrillFile f = DotDrillFile.create(fs, s);
      if(f != null){
        if(types.length == 0){
          files.add(f);
        }else{
          for(DotDrillType t : types){
            if(t == f.getType()){
              files.add(f);
            }
          }
        }

      }
    }
    return files;
  }

  public static List<DotDrillFile> getDotDrills(DrillFileSystem fs, Path root, DotDrillType... types) throws IOException{
    return getDrillFiles(fs, fs.globStatus(new Path(root, "*.drill")), types);
  }

  public static List<DotDrillFile> getDotDrills(DrillFileSystem fs, Path root, String name, DotDrillType... types) throws IOException{
    if(!name.endsWith(".drill")) {
      name = name + DotDrillType.DOT_DRILL_GLOB;
    }

    return getDrillFiles(fs, fs.globStatus(new Path(root, name)), types);
  }
}
