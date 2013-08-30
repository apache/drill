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
package org.apache.drill.exec.physical.impl;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.ScreenCreator.ScreenRoot;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import com.beust.jcommander.internal.Lists;

public class SimpleRootExec implements RootExec, Iterable<ValueVector>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleRootExec.class);

  private RecordBatch incoming;
  
  public SimpleRootExec(RootExec e){
    if(e instanceof ScreenRoot){
      incoming = ((ScreenRoot)e).getIncoming();  
    }else{
      throw new UnsupportedOperationException();
    }
    
  }

  public FragmentContext getContext(){
    return incoming.getContext();
  }
  
  public SelectionVector2 getSelectionVector2(){
    return incoming.getSelectionVector2();
  }

  public SelectionVector4 getSelectionVector4(){
    return incoming.getSelectionVector4();
  }

  @SuppressWarnings("unchecked")
  public <T extends ValueVector> T getValueVectorById(SchemaPath path, Class<?> vvClass){
    TypedFieldId tfid = incoming.getValueVectorId(path);
    return (T) incoming.getValueAccessorById(tfid.getFieldId(), vvClass).getValueVector();
  }
  
  @Override
  public boolean next() {
    return incoming.next() != IterOutcome.NONE;
  }

  @Override
  public void stop() {
    incoming.kill();
  }

  @Override
  public Iterator<ValueVector> iterator() {
    List<ValueVector> vv = Lists.newArrayList();
    for(VectorWrapper<?> vw : incoming){
      vv.add(vw.getValueVector());
    }
    return vv.iterator();
  }

  public int getRecordCount(){
    return incoming.getRecordCount();
  }
  
  
}
