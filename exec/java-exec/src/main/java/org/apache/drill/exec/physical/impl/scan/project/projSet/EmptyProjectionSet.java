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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.exec.physical.resultSet.ProjectionSet;
import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Handles simple cases in which either all columns are projected
 * or no columns are projected.
 */

public class EmptyProjectionSet implements ProjectionSet {

  public static final ProjectionSet PROJECT_NONE = new EmptyProjectionSet();

  @Override
  public ColumnReadProjection readProjection(ColumnMetadata col) {
    return new UnprojectedReadColumn(col);
  }

  @Override
  public ColumnReadProjection readDictProjection(ColumnMetadata col) {
    return readProjection(col);
  }

  @Override
  public void setErrorContext(CustomErrorContext errorContext) { }

  @Override
  public boolean isEmpty() { return true; }

  @Override
  public boolean isProjected(String colName) { return false; }
}
