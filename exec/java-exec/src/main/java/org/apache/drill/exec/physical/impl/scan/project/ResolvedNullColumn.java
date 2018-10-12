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
package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.NullColumnLoader.NullColumnSpec;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Projected column that serves as both a resolved column (provides projection
 * mapping) and a null column spec (provides the information needed to create
 * the required null vectors.)
 */

public class ResolvedNullColumn extends ResolvedColumn implements NullColumnSpec {

  public static final int ID = 4;

  private final String name;
  private MajorType type;

  public ResolvedNullColumn(String name, MajorType type, VectorSource source, int sourceIndex) {
    super(source, sourceIndex);
    this.name = name;
    this.type = type;
  }

  @Override
  public String name() { return name; }

  @Override
  public MajorType type() { return type; }

  @Override
  public int nodeType() { return ID; }

  @Override
  public void setType(MajorType type) {

    // Update the actual type based on what the null-column
    // mechanism chose for this column.

    this.type = type;
  }

  @Override
  public MaterializedField schema() {
    return MaterializedField.create(name, type);
  }
}
