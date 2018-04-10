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
package org.apache.drill.exec.physical.rowSet.impl;

/**
 * Represents a wildcard: SELECT * when used at the root tuple.
 * When used with maps, means selection of all map columns, either
 * implicitly, or because the map itself is selected.
 */

public class NullProjectionSet implements ProjectionSet {

  private boolean allProjected;

  public NullProjectionSet(boolean allProjected) {
    this.allProjected = allProjected;
  }

  @Override
  public boolean isProjected(String colName) { return allProjected; }

  @Override
  public ProjectionSet mapProjection(String colName) {
    return new NullProjectionSet(allProjected);
  }
}
