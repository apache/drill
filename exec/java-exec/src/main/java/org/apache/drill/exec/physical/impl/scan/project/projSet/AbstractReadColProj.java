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

import org.apache.drill.exec.physical.rowSet.ProjectionSet;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.rowSet.project.ProjectionType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

public abstract class AbstractReadColProj implements ColumnReadProjection {
  protected final ColumnMetadata readSchema;

  public AbstractReadColProj(ColumnMetadata readSchema) {
    this.readSchema = readSchema;
  }

  @Override
  public ColumnMetadata readSchema() { return readSchema; }

  @Override
  public boolean isProjected() { return true; }

  @Override
  public ColumnConversionFactory conversionFactory() { return null; }

  @Override
  public ColumnMetadata providedSchema() { return readSchema; }

  @Override
  public ProjectionSet mapProjection() { return ProjectionSetFactory.projectAll(); }

  @Override
  public ProjectionType projectionType() { return null; }
}
