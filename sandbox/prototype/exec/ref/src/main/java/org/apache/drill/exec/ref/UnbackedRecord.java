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
package org.apache.drill.exec.ref;

import java.io.IOException;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.CollisionBehavior;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.ContainerValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.apache.drill.exec.ref.values.ValueUtils;

public class UnbackedRecord implements RecordPointer{

  private DataValue root = new SimpleMapValue();
  
  public DataValue getField(SchemaPath field) {
    return root.getValue(field.getRootSegment());
  }

  public void addField(SchemaPath field, DataValue value) {
    addField(field.getRootSegment(), value);
  }

  @Override
  public void addField(PathSegment segment, DataValue value) {
    root.addValue(segment, value);
  }
  
  @Override
  public void write(DataWriter writer) throws IOException {
    writer.startRecord();
    root.write(writer);
    writer.endRecord();
  }

  public void merge(DataValue v){
    if(v instanceof ContainerValue){
      this.root = ValueUtils.getMergedDataValue(CollisionBehavior.MERGE_OVERRIDE, root, v);
    }else{
      this.root = v;
    }
  }

  @Override
  public RecordPointer copy() {
    // TODO: Make a deep copy.
    UnbackedRecord r = new UnbackedRecord();
    r.root = this.root;
    return r;
  }
  
  public void clear(){
    root = new SimpleMapValue();
  }
 
  public void setClearAndSetRoot(SchemaPath path, DataValue v){
    root = new SimpleMapValue();
    root.addValue(path.getRootSegment(), v);
  }

  @Override
  public void copyFrom(RecordPointer r) {
    if(r instanceof UnbackedRecord){
      this.root = ((UnbackedRecord)r).root.copy();
    }else{
      throw new UnsupportedOperationException(String.format("Unable to copy from a record of type %s to an UnbackedRecord.", r.getClass().getCanonicalName()));
    }
  }

  @Override
  public String toString() {
    return "UnbackedRecord [root=" + root + "]";
  }


  

  
  
}
