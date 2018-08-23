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
package org.apache.drill.exec.expr.fn;

import com.google.common.base.Preconditions;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

public class ValueReference {
  private final MajorType type;
  private final String name;
  private boolean isConstant = false;
  private boolean isFieldReader = false;
  private boolean isComplexWriter = false;

  public ValueReference(MajorType type, String name) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(name);
    this.type = type;
    this.name = name;
  }

  public void setConstant(boolean isConstant) {
    this.isConstant = isConstant;
  }

  public MajorType getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public boolean isConstant() {
    return isConstant;
  }

  public boolean isFieldReader() {
    return isFieldReader;
  }

  public boolean isComplexWriter() {
    return isComplexWriter;
  }

  @Override
  public String toString() {
    return "ValueReference [type=" + Types.toString(type) + ", name=" + name + "]";
  }

  public static ValueReference createFieldReaderRef(String name) {
    MajorType type = Types.required(MinorType.LATE);
    ValueReference ref = new ValueReference(type, name);
    ref.isFieldReader = true;
    return ref;
  }

  public static ValueReference createComplexWriterRef(String name) {
    MajorType type = Types.required(MinorType.LATE);
    ValueReference ref = new ValueReference(type, name);
    ref.isComplexWriter = true;
    return ref;
  }
}