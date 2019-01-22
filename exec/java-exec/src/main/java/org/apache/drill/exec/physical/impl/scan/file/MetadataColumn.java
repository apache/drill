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
package org.apache.drill.exec.physical.impl.scan.file;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.VectorSource;
import org.apache.drill.exec.physical.impl.scan.project.ConstantColumnLoader.ConstantColumnSpec;
import org.apache.drill.exec.record.MaterializedField;

public abstract class MetadataColumn extends ResolvedColumn implements ConstantColumnSpec {

  public final MaterializedField schema;
  public final String value;

  public MetadataColumn(String name, MajorType type, String value, VectorSource source, int sourceIndex) {
    super(source, sourceIndex);
    schema = MaterializedField.create(name, type);
    this.value = value;
  }

  @Override
  public MaterializedField schema() { return schema; }

  @Override
  public String value() { return value; }

  @Override
  public String name() { return schema.getName(); }

  public abstract MetadataColumn resolve(FileMetadata fileInfo, VectorSource source, int sourceIndex);
}
