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
package org.apache.drill.exec.ref;

import java.io.IOException;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.DataValue;


public interface RecordPointer {
  public DataValue getField(SchemaPath field);
  public void addField(SchemaPath field, DataValue value);
  public void addField(PathSegment segment, DataValue value);
  public void removeField(SchemaPath segment);
  public void write(DataWriter writer) throws IOException;
  public RecordPointer copy();
  public void copyFrom(RecordPointer r);
  
}
