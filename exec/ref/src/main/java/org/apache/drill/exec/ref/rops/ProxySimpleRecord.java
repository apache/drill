/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.rops;

import java.io.IOException;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.values.DataValue;

public class ProxySimpleRecord implements RecordPointer{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProxySimpleRecord.class);
  
  private RecordPointer record;

  @Override
  public DataValue getField(SchemaPath field) {
    return record.getField(field);
  }

  @Override
  public void addField(SchemaPath field, DataValue value) {
    record.addField(field, value);
  }
  

  @Override
  public void addField(PathSegment segment, DataValue value) {
    record.addField(segment, value);
  }

  @Override
  public void removeField(SchemaPath field) {
    record.removeField(field);
  }

  @Override
  public void write(DataWriter writer) throws IOException {
    record.write(writer);
  }

  @Override
  public void copyFrom(RecordPointer r) {
    record.copyFrom(r);
  }

  @Override
  public RecordPointer copy() {
    return record.copy();
  }

  public RecordPointer getRecord() {
    return record;
  }

  public void setRecord(RecordPointer record) {
    this.record = record;
  }
  
  
  
  
}
