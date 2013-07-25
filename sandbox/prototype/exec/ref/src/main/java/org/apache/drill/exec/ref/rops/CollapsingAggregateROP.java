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

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;

public class CollapsingAggregateROP extends SingleInputROPBase<CollapsingAggregate> implements BoundaryListener {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CollapsingAggregateROP.class);

  private SchemaPath[] boundaryPaths;
  private BasicEvaluator targetEvaluator;
  private boolean foundTarget = false;
  private DataValue currentValue;
  private DataValue previousValue;
  private BasicEvaluator boundaryKey;
  private RecordIterator incoming;
  private BasicEvaluator[] carryovers;
  private FieldReference[] carryoverNames;
  private DataValue[] carryoverValues;
  private AggregatingEvaluator[] aggs;
  private SchemaPath[] aggNames;
  private boolean boundaryCrossed = false;
  private final boolean targetMode;
  
  public CollapsingAggregateROP(CollapsingAggregate config) {
    super(config);
    targetMode = config.getTarget() != null;
  }
  
  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    
    if(config.getWithin() != null){
      boundaryPaths = new SchemaPath[]{config.getWithin()};
      boundaryKey = builder.getBasicEvaluator(record, config.getWithin());  
    }else{
      boundaryPaths = new SchemaPath[0];
      boundaryKey = new ScalarValues.IntegerScalar(0);
    }
    
    aggs = new AggregatingEvaluator[config.getAggregations().length];
    carryovers = new BasicEvaluator[config.getCarryovers().length];
    carryoverNames = new FieldReference[config.getCarryovers().length];
    carryoverValues = new DataValue[config.getCarryovers().length];

    if(targetMode){
      targetEvaluator = builder.getBasicEvaluator(record, config.getTarget());
    }
    aggNames = new SchemaPath[aggs.length];
    for(int i =0; i < aggs.length; i++){
      aggs[i] = builder.getAggregatingOperator(record, config.getAggregations()[i].getExpr());
      aggNames[i] = config.getAggregations()[i].getRef();
    }
    
    for(int i =0; i < carryovers.length; i++){
      carryovers[i] = builder.getBasicEvaluator(record, config.getCarryovers()[i]);
      carryoverNames[i] = config.getCarryovers()[i];
    }
    
    
    
  }
 
  @Override
  protected void setInput(RecordIterator incoming) {
    this.incoming = incoming;
  }

  @Override
  protected RecordIterator getIteratorInternal() {
    return new AggregatingIterator();
  }

  private boolean checkBoundaryCrossing() {
    if (boundaryCrossed) {
      boundaryCrossed = false;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void markBoundaryAsCrossed() {
    this.boundaryCrossed = true;
  }

  

  
  private class AggregatingIterator implements RecordIterator {
    private boolean remainder = false;
    private boolean more = true;
    private UnbackedRecord outputRecord = new UnbackedRecord();
    public AggregatingIterator() {
    }

    private NextOutcome readNext(){

      // copy over the previous values.
      previousValue = currentValue;
      
      // increment the parent forward.
      NextOutcome n = incoming.next();

      boolean changed = false;
      
      // read boundary values unless no new values were loaded.
      if(n != NextOutcome.NONE_LEFT){
        currentValue = boundaryKey.eval();
        if(!currentValue.equals(previousValue)) changed = true;
  
        // skip first boundary.
        if(previousValue == null) changed = false;
        
      }else{
        
        changed = true;
      }
      
      if(changed){
        markBoundaryAsCrossed();
      }
      
      return n;
    }
    
    private void consumeCurrent(){
      for(int x = 0; x < aggs.length; x++){
        aggs[x].addRecord();  
      }
      
      // if we're in target mode and this row matches the target criteria, we're going to copy carry over values and mark foundTarget = true.
      if(targetMode){
        DataValue v = targetEvaluator.eval();
        if(v.getDataType().getMinorType() == MinorType.BOOLEAN && v.getAsBooleanValue().getBoolean()){
          foundTarget = true;
          for(int i =0 ; i < carryovers.length; i++){
            carryoverValues[i] = carryovers[i].eval();
          }
        }
      }else{
        for(int i =0 ; i < carryovers.length; i++){
          carryoverValues[i] = carryovers[i].eval();
        }
      }
    }
    
    /** 
     * Write the output of the operation to the unbacked record.
     * @return Whether or not the record should be included in the output.
     */
    private boolean writeOutputRecord(){
      outputRecord.clear();
      for(int x = 0; x < aggs.length; x++){
        DataValue dv = aggs[x].eval();
//        logger.debug("Adding Aggregated Values named {} with value {}", outputNames[x], dv);
        outputRecord.addField(aggNames[x], dv);
      }
      
      // Add the carryover keys.
      if(targetMode){
        if(foundTarget){
          for(int y = 0; y < carryoverNames.length; y++){
            outputRecord.addField(carryoverNames[y], carryoverValues[y]);
          }
          foundTarget = false;
          return true;
        }else{
          return false;
        }
      }else{
        for(int y = 0; y < carryoverNames.length; y++){
          outputRecord.addField(carryoverNames[y], carryoverValues[y]);
        }
        return true;
      }
      

      
      
    }
    
    @Override
    public NextOutcome next() {
      outside: while(true){
      NextOutcome whatNext = null;
      
      // if we don't have more and there are no more values, exit.
      if (!more) return NextOutcome.NONE_LEFT;
      
      while (true) {
        
        // we shouldn't increment the iterator since we have to consume our remainder.
        if(!remainder){
          whatNext = readNext();  

          // if we've just crossed a boundary, we should output the previous values.
          if(checkBoundaryCrossing()){
            boolean wroteSomething = writeOutputRecord();
            
            // if there is no future input, we'll flag as !more.  Otherwise, will inform that there are pending records that should be consumed.
            if(whatNext == NextOutcome.NONE_LEFT){
              more = false;
            }else{
              remainder = true;
            }
            
            // if we didn't write something, we'll retry the outside loop
            if(!wroteSomething) continue outside;
            
            // always return a next outcome of true since we've just output a record.
            return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
            
          }
        }
        
        // we don't consume the current record until after we've output a record associated with a boundary (as necessary).
        consumeCurrent();
        remainder = false;
      }
      
      }
    }

    @Override
    public ROP getParent() {
      return CollapsingAggregateROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }

  }

  @Override
  public SchemaPath[] getOrderedBoundaryPaths() {
    return boundaryPaths;
  }

}
