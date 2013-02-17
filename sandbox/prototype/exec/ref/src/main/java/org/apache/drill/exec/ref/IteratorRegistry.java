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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.rops.SinkROP;

import com.google.common.collect.ArrayListMultimap;

public class IteratorRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IteratorRegistry.class);
  
  ArrayListMultimap<LogicalOperator, ROP> map = ArrayListMultimap.create();
  LinkedList<SinkROP> sinks = new LinkedList<SinkROP>();
  
  public void register(LogicalOperator logOp, ROP referenceOperator){
    map.put(logOp, referenceOperator);
    if(referenceOperator instanceof SinkROP) sinks.add((SinkROP) referenceOperator);
  }
  
  public void remap(LogicalOperator currentId, LogicalOperator newId){
    List<ROP> rops = map.removeAll(currentId);
    map.putAll(newId, rops);
  }
  
  public void swap(LogicalOperator op1, LogicalOperator op2){
    List<ROP> ops1 = map.removeAll(op1);
    List<ROP> ops2 = map.removeAll(op2);
    map.putAll(op1, ops2);
    map.putAll(op2, ops1);
  }
  
  public List<RecordIterator> getOperator(LogicalOperator o){
//    logger.debug("Getting iterator for Logical Operator {}", o);
    if(o == null) throw new SetupException("You requested a Iterator list for a null operator.  This doesn't make any sense.");
    List<ROP> refOps = map.get(o);
    List<RecordIterator> iterators = new ArrayList<RecordIterator>(refOps.size());
    for(ROP r : refOps){
      RecordIterator iterator = r.getOutput();
      if(iterator == null) throw new SetupException(String.format("The provided iterator for the reference operator %s is null.", r));
      iterators.add(iterator);
    }
    return iterators;
  }
  
  public List<SinkROP> getSinks(){
    return sinks;
  }
}
