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
import org.apache.drill.common.logical.data.Aggregate;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.DataValue;

public class AggregateSimpleROP extends SingleInputROPBase<Aggregate> implements BoundaryListener {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggregateSimpleROP.class);
  
  private AggregatingEvaluator[] eval;
  private SchemaPath[] outputNames;
  private boolean boundaryCrossed = false;
  private RecordIterator incoming;
  private FieldReference[] orderingBoundaries;
  private DataValue[] previousValues;
  private DataValue[] currentValues;
  private BasicEvaluator[] boundaryKeys;

  public AggregateSimpleROP(Aggregate config) {
    super(config);
    int len = config.getKeys().length;
    orderingBoundaries = new FieldReference[len];
    previousValues = new DataValue[len];
    currentValues = new DataValue[len];
    boundaryKeys = new BasicEvaluator[len];
    
    for(int i = 0; i < orderingBoundaries.length; i++){
      orderingBoundaries[i] = config.getKeys()[i];
    }
  }
  
  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    eval = new AggregatingEvaluator[config.getAggregations().length];
    outputNames = new SchemaPath[eval.length];
    for(int i =0; i < eval.length; i++){
      eval[i] = builder.getAggregatingOperator(record, config.getAggregations()[i].getExpr());
      outputNames[i] = config.getAggregations()[i].getRef();
    }
    
    for(int i =0; i < orderingBoundaries.length; i++){
      boundaryKeys[i] = builder.getBasicEvaluator(record, orderingBoundaries[i]);
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
      System.arraycopy(currentValues, 0, previousValues, 0, currentValues.length);  
      
      // increment the parent forward.
      NextOutcome n = incoming.next();

      boolean changed = false;
      
      // read boundary values unless no new values were loaded.
      if(n != NextOutcome.NONE_LEFT){
        // collect up the current values for each boundary key.
        for(int i =0; i < currentValues.length; i++){
          currentValues[i] = boundaryKeys[i].eval();
          
          // set a change if the boundary value doesn't equal the previous boundary value
          if(!currentValues[i].equals(previousValues[i])) changed = true;
        }
  
        // skip first boundary.
        if(previousValues[0] == null) changed = false;
        
      }else{
        
        changed = true;
      }
      
      if(changed){
//        logger.debug("Boundary checked, found a crossed boundary for current values: {}, previous values: {}", currentValues, previousValues);
        markBoundaryAsCrossed();
      }else{
//        logger.debug("Boundary checked, found no crossed boundary for current values: {}, previous values: {}", currentValues, previousValues);

      }
      
      return n;
    }
    
    private void consumeCurrent(){
      for(int x = 0; x < eval.length; x++){
        eval[x].addRecord();  
      }
    }
    
    private void writeOutputRecord(){
      outputRecord.clear();
      for(int x = 0; x < eval.length; x++){
        DataValue dv = eval[x].eval();
//        logger.debug("Adding Aggregated Values named {} with value {}", outputNames[x], dv);
        outputRecord.addField(outputNames[x], dv);
      }
      
      // Add the grouping keys.
      for(int y = 0; y < currentValues.length; y++){
        //logger.debug("Adding Grouping Keys {}", orderingBoundaries[y]);
        outputRecord.addField(orderingBoundaries[y], previousValues[y]);
      }
    }
    
    @Override
    public NextOutcome next() {
      
      NextOutcome whatNext = null;
      
      // if we don't have more and there are no more values, exit.
      if (!more) return NextOutcome.NONE_LEFT;
      
      while (true) {
        
        // we shouldn't increment the iterator since we have to consume our remainder.
        if(!remainder){
          whatNext = readNext();  

          // if we've just crossed a boundary, we should output the previous values.
          if(checkBoundaryCrossing()){
            writeOutputRecord();
            
            // if there is no future input, we'll flag as !more.  Otherwise, will inform that there are pending records that should be consumed.
            if(whatNext == NextOutcome.NONE_LEFT){
              more = false;
            }else{
              remainder = true;
            }
            
            // always return a next outcome of true since we've just output a record.
            return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
            
          }
        }
        
        // we don't consume the current record until after we've output a record associated with a boundary (as necessary).
        consumeCurrent();
        remainder = false;
      }
    }

    @Override
    public ROP getParent() {
      return AggregateSimpleROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }

  }

  @Override
  public SchemaPath[] getOrderedBoundaryPaths() {
    return this.orderingBoundaries;
  }

}
