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
package org.apache.drill.exec.store.hive;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.Size;
import org.apache.hadoop.hive.conf.HiveConf;

public class HiveReadEntry implements ReadEntry {
  private final HiveConf conf;
  private final String table;
  private Size size;

  public HiveReadEntry(HiveConf conf, String table) {
    this.conf = conf;
    this.table = table;
  }

  @Override
  public OperatorCost getCost() {
    // TODO: need to come up with way to calculate the cost for Hive tables
    return new OperatorCost(1, 1, 2, 2);
  }

  @Override
  public Size getSize() {
    if (size != null) {
      // TODO: contact the metastore and find the size of the data in table
      size = new Size(1, 1);
    }

    return size;
  }
}
