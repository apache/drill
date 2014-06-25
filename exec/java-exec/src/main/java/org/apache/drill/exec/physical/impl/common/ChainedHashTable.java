/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright
 * ownership.  The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.common;

import java.io.IOException;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.fn.impl.BitFunctions;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import com.google.common.collect.ImmutableList;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;


public class ChainedHashTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ChainedHashTable.class);

  private static final GeneratorMapping KEY_MATCH_BUILD = 
	  GeneratorMapping.create("setupInterior" /* setup method */, "isKeyMatchInternalBuild" /* eval method */, 
                            null /* reset */, null /* cleanup */);

  private static final GeneratorMapping KEY_MATCH_PROBE = 
	  GeneratorMapping.create("setupInterior" /* setup method */, "isKeyMatchInternalProbe" /* eval method */, 
                            null /* reset */, null /* cleanup */);

  private static final GeneratorMapping GET_HASH_BUILD = 
	  GeneratorMapping.create("doSetup" /* setup method */, "getHashBuild" /* eval method */, 
                            null /* reset */, null /* cleanup */);

  private static final GeneratorMapping GET_HASH_PROBE = 
	  GeneratorMapping.create("doSetup" /* setup method */, "getHashProbe" /* eval method */, 
                            null /* reset */, null /* cleanup */);

  private static final GeneratorMapping SET_VALUE = 
	  GeneratorMapping.create("setupInterior" /* setup method */, "setValue" /* eval method */, 
                            null /* reset */, null /* cleanup */);
  
  private static final GeneratorMapping OUTPUT_KEYS = 
    GeneratorMapping.create("setupInterior" /* setup method */, "outputRecordKeys" /* eval method */,
                            null /* reset */, null /* cleanup */) ;

  private final MappingSet KeyMatchIncomingBuildMapping = new MappingSet("incomingRowIdx", null, "incomingBuild", null, KEY_MATCH_BUILD, KEY_MATCH_BUILD);
  private final MappingSet KeyMatchIncomingProbeMapping = new MappingSet("incomingRowIdx", null, "incomingProbe", null, KEY_MATCH_PROBE, KEY_MATCH_PROBE);
  private final MappingSet KeyMatchHtableMapping = new MappingSet("htRowIdx", null, "htContainer", null, KEY_MATCH_BUILD, KEY_MATCH_BUILD);
  private final MappingSet KeyMatchHtableProbeMapping = new MappingSet("htRowIdx", null, "htContainer", null, KEY_MATCH_PROBE, KEY_MATCH_PROBE);
  private final MappingSet GetHashIncomingBuildMapping = new MappingSet("incomingRowIdx", null, "incomingBuild", null, GET_HASH_BUILD, GET_HASH_BUILD);
  private final MappingSet GetHashIncomingProbeMapping = new MappingSet("incomingRowIdx", null, "incomingProbe", null, GET_HASH_PROBE, GET_HASH_PROBE);
  private final MappingSet SetValueMapping = new MappingSet("incomingRowIdx" /* read index */, "htRowIdx" /* write index */, "incomingBuild" /* read container */, "htContainer" /* write container */, SET_VALUE, SET_VALUE);

  private final MappingSet OutputRecordKeysMapping = new MappingSet("htRowIdx" /* read index */, "outRowIdx" /* write index */, "htContainer" /* read container */, "outgoing" /* write container */, OUTPUT_KEYS, OUTPUT_KEYS);

  private HashTableConfig htConfig;
  private final FragmentContext context;
  private final BufferAllocator allocator;
  private final RecordBatch incomingBuild;
  private final RecordBatch incomingProbe;
  private final RecordBatch outgoing;

  public ChainedHashTable(HashTableConfig htConfig, 
                          FragmentContext context,
                          BufferAllocator allocator,
                          RecordBatch incomingBuild, 
                          RecordBatch incomingProbe,
                          RecordBatch outgoing)  {

    this.htConfig = htConfig;
    this.context = context;
    this.allocator = allocator;
    this.incomingBuild = incomingBuild;
    this.incomingProbe = incomingProbe;
    this.outgoing = outgoing;
  }

  public HashTable createAndSetupHashTable (TypedFieldId[] outKeyFieldIds) throws ClassTransformationException, IOException, SchemaChangeException {
    CodeGenerator<HashTable> top = CodeGenerator.get(HashTable.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<HashTable> cg = top.getRoot();
    ClassGenerator<HashTable> cgInner = cg.getInnerGenerator("BatchHolder");

    LogicalExpression[] keyExprsBuild = new LogicalExpression[htConfig.getKeyExprsBuild().length];
    LogicalExpression[] keyExprsProbe = null;
    boolean isProbe = (htConfig.getKeyExprsProbe() != null) ;
    if (isProbe) {
      keyExprsProbe = new LogicalExpression[htConfig.getKeyExprsProbe().length];
    }
    
    ErrorCollector collector = new ErrorCollectorImpl();
    VectorContainer htContainerOrig = new VectorContainer(); // original ht container from which others may be cloned
    LogicalExpression[] htKeyExprs = new LogicalExpression[htConfig.getKeyExprsBuild().length];
    TypedFieldId[] htKeyFieldIds = new TypedFieldId[htConfig.getKeyExprsBuild().length];

    int i = 0;
    for (NamedExpression ne : htConfig.getKeyExprsBuild()) { 
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incomingBuild, collector, context.getFunctionRegistry());
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      if (expr == null) continue;
      keyExprsBuild[i] = expr;
      
      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      // create a type-specific ValueVector for this key
      ValueVector vv = TypeHelper.getNewVector(outputField, allocator);
      vv.allocateNew();
      htKeyFieldIds[i] = htContainerOrig.add(vv);
      
      i++;
    }

    if (isProbe) {
      i = 0;
      for (NamedExpression ne : htConfig.getKeyExprsProbe()) { 
        final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incomingProbe, collector, context.getFunctionRegistry());
        if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
        if (expr == null) continue;
        keyExprsProbe[i] = expr;
        i++;
      }
    }

    // generate code for isKeyMatch(), setValue(), getHash() and outputRecordKeys()
    setupIsKeyMatchInternal(cgInner, KeyMatchIncomingBuildMapping, KeyMatchHtableMapping, keyExprsBuild, htKeyFieldIds);
    setupIsKeyMatchInternal(cgInner, KeyMatchIncomingProbeMapping, KeyMatchHtableProbeMapping, keyExprsProbe, htKeyFieldIds) ;

    setupSetValue(cgInner, keyExprsBuild, htKeyFieldIds);
    if (outgoing != null) {

      if (outKeyFieldIds.length > htConfig.getKeyExprsBuild().length) {
        throw new IllegalArgumentException("Mismatched number of output key fields.");
      }
    }
    setupOutputRecordKeys(cgInner, htKeyFieldIds, outKeyFieldIds);

    setupGetHash(cg /* use top level code generator for getHash */,  GetHashIncomingBuildMapping, keyExprsBuild);
    setupGetHash(cg /* use top level code generator for getHash */,  GetHashIncomingProbeMapping, keyExprsProbe);

    HashTable ht = context.getImplementationClass(top);
    ht.setup(htConfig, context, allocator, incomingBuild, incomingProbe, outgoing, htContainerOrig);

    return ht;
  }
   

  private void setupIsKeyMatchInternal(ClassGenerator<HashTable> cg, MappingSet incomingMapping, MappingSet htableMapping, 
                                       LogicalExpression[] keyExprs, TypedFieldId[] htKeyFieldIds)
    throws SchemaChangeException {
    cg.setMappingSet(incomingMapping);

    if (keyExprs == null || keyExprs.length == 0) {
      cg.getEvalBlock()._return(JExpr.FALSE);
      return;
    }

    int i = 0;
    for (LogicalExpression expr : keyExprs) { 
      cg.setMappingSet(incomingMapping);
      HoldingContainer left = cg.addExpr(expr, false);
        
      cg.setMappingSet(htableMapping);
      ValueVectorReadExpression vvrExpr = new ValueVectorReadExpression(htKeyFieldIds[i++]);
      HoldingContainer right = cg.addExpr(vvrExpr, false);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression f = FunctionGenerationHelper.getComparator(left, right, context.getFunctionRegistry());
      HoldingContainer out = cg.addExpr(f, false);

      // check if two values are not equal (comparator result != 0)
      JConditional jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));
        
      jc._then()._return(JExpr.FALSE);
    }

    // All key expressions compared equal, so return TRUE
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  private void setupSetValue(ClassGenerator<HashTable> cg, LogicalExpression[] keyExprs, TypedFieldId[] htKeyFieldIds) 
    throws SchemaChangeException {

    cg.setMappingSet(SetValueMapping);

    int i = 0;
    for (LogicalExpression expr : keyExprs) {
      ValueVectorWriteExpression vvwExpr = new ValueVectorWriteExpression(htKeyFieldIds[i++], expr, true) ;

      HoldingContainer hc = cg.addExpr(vvwExpr, false); // this will write to the htContainer at htRowIdx
      cg.getEvalBlock()._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);      
    }

    cg.getEvalBlock()._return(JExpr.TRUE);

  }

  private void setupOutputRecordKeys(ClassGenerator<HashTable> cg, TypedFieldId[] htKeyFieldIds, TypedFieldId[] outKeyFieldIds) {

    cg.setMappingSet(OutputRecordKeysMapping);

    if (outKeyFieldIds != null) {
      for (int i = 0; i < outKeyFieldIds.length; i++) {
        ValueVectorReadExpression vvrExpr = new ValueVectorReadExpression(htKeyFieldIds[i]);
        ValueVectorWriteExpression vvwExpr = new ValueVectorWriteExpression(outKeyFieldIds[i], vvrExpr, true);
        HoldingContainer hc = cg.addExpr(vvwExpr);
        cg.getEvalBlock()._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
      }

      cg.getEvalBlock()._return(JExpr.TRUE);
    } else {
      cg.getEvalBlock()._return(JExpr.FALSE);
    }
  }

  private void setupGetHash(ClassGenerator<HashTable> cg, MappingSet incomingMapping, LogicalExpression[] keyExprs) throws SchemaChangeException {

    cg.setMappingSet(incomingMapping);

    if (keyExprs == null || keyExprs.length == 0) {
      cg.getEvalBlock()._return(JExpr.lit(0));
      return;
    }
     
    HoldingContainer combinedHashValue = null;

    for (int i = 0; i < keyExprs.length; i++) {
      LogicalExpression expr = keyExprs[i];
      
      cg.setMappingSet(incomingMapping);
      HoldingContainer input = cg.addExpr(expr, false);

      // compute the hash(expr)
      LogicalExpression hashfunc = FunctionGenerationHelper.getFunctionExpression("hash", Types.required(MinorType.INT), context.getFunctionRegistry(), input); 
      HoldingContainer hashValue = cg.addExpr(hashfunc, false);

      if (i == 0) {
        combinedHashValue = hashValue; // first expression..just use the hash value 
      } 
      else {

        // compute the combined hash value using XOR
        LogicalExpression xorfunc = FunctionGenerationHelper.getFunctionExpression("xor", Types.required(MinorType.INT), context.getFunctionRegistry(), hashValue, combinedHashValue);
        combinedHashValue = cg.addExpr(xorfunc, false);
      }
    }

    if (combinedHashValue != null) {
      cg.getEvalBlock()._return(combinedHashValue.getValue()) ;
    }
    else {
      cg.getEvalBlock()._return(JExpr.lit(0));
    }
  }
}

