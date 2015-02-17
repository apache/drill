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
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
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
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

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
      GeneratorMapping.create("doSetup" /* setup method */, "getHashProbe" /* eval method */, null /* reset */,
          null /* cleanup */);

  private static final GeneratorMapping SET_VALUE =
      GeneratorMapping.create("setupInterior" /* setup method */, "setValue" /* eval method */, null /* reset */,
          null /* cleanup */);

  private static final GeneratorMapping OUTPUT_KEYS =
      GeneratorMapping.create("setupInterior" /* setup method */, "outputRecordKeys" /* eval method */,
          null /* reset */, null /* cleanup */);

  // GM for putting constant expression into method "setupInterior"
  private static final GeneratorMapping SETUP_INTERIOR_CONSTANT =
      GeneratorMapping.create("setupInterior" /* setup method */, "setupInterior" /* eval method */,
          null /* reset */, null /* cleanup */);

  // GM for putting constant expression into method "doSetup"
  private static final GeneratorMapping DO_SETUP_CONSTANT =
      GeneratorMapping.create("doSetup" /* setup method */, "doSetup" /* eval method */, null /* reset */,
          null /* cleanup */);

  private final MappingSet KeyMatchIncomingBuildMapping =
      new MappingSet("incomingRowIdx", null, "incomingBuild", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_BUILD);
  private final MappingSet KeyMatchIncomingProbeMapping =
      new MappingSet("incomingRowIdx", null, "incomingProbe", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_PROBE);
  private final MappingSet KeyMatchHtableMapping =
      new MappingSet("htRowIdx", null, "htContainer", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_BUILD);
  private final MappingSet KeyMatchHtableProbeMapping =
      new MappingSet("htRowIdx", null, "htContainer", null, SETUP_INTERIOR_CONSTANT, KEY_MATCH_PROBE);
  private final MappingSet GetHashIncomingBuildMapping =
      new MappingSet("incomingRowIdx", null, "incomingBuild", null, DO_SETUP_CONSTANT, GET_HASH_BUILD);
  private final MappingSet GetHashIncomingProbeMapping =
      new MappingSet("incomingRowIdx", null, "incomingProbe", null, DO_SETUP_CONSTANT, GET_HASH_PROBE);
  private final MappingSet SetValueMapping =
      new MappingSet("incomingRowIdx" /* read index */, "htRowIdx" /* write index */,
          "incomingBuild" /* read container */, "htContainer" /* write container */, SETUP_INTERIOR_CONSTANT,
          SET_VALUE);

  private final MappingSet OutputRecordKeysMapping =
      new MappingSet("htRowIdx" /* read index */, "outRowIdx" /* write index */, "htContainer" /* read container */,
          "outgoing" /* write container */, SETUP_INTERIOR_CONSTANT, OUTPUT_KEYS);

  private HashTableConfig htConfig;
  private final FragmentContext context;
  private final BufferAllocator allocator;
  private final RecordBatch incomingBuild;
  private final RecordBatch incomingProbe;
  private final RecordBatch outgoing;
  private final boolean areNullsEqual;

  public ChainedHashTable(HashTableConfig htConfig, FragmentContext context, BufferAllocator allocator,
                          RecordBatch incomingBuild, RecordBatch incomingProbe, RecordBatch outgoing,
                          boolean areNullsEqual) {

    this.htConfig = htConfig;
    this.context = context;
    this.allocator = allocator;
    this.incomingBuild = incomingBuild;
    this.incomingProbe = incomingProbe;
    this.outgoing = outgoing;
    this.areNullsEqual = areNullsEqual;
  }

  public HashTable createAndSetupHashTable(TypedFieldId[] outKeyFieldIds) throws ClassTransformationException,
      IOException, SchemaChangeException {
    CodeGenerator<HashTable> top = CodeGenerator.get(HashTable.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<HashTable> cg = top.getRoot();
    ClassGenerator<HashTable> cgInner = cg.getInnerGenerator("BatchHolder");

    LogicalExpression[] keyExprsBuild = new LogicalExpression[htConfig.getKeyExprsBuild().length];
    LogicalExpression[] keyExprsProbe = null;
    boolean isProbe = (htConfig.getKeyExprsProbe() != null);
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
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      if (expr == null) {
        continue;
      }
      keyExprsBuild[i] = expr;

      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      // create a type-specific ValueVector for this key
      ValueVector vv = TypeHelper.getNewVector(outputField, allocator);
      htKeyFieldIds[i] = htContainerOrig.add(vv);

      i++;
    }

    if (isProbe) {
      i = 0;
      for (NamedExpression ne : htConfig.getKeyExprsProbe()) {
        final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incomingProbe, collector, context.getFunctionRegistry());
        if (collector.hasErrors()) {
          throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
        }
        if (expr == null) {
          continue;
        }
        keyExprsProbe[i] = expr;
        i++;
      }
    }

    // generate code for isKeyMatch(), setValue(), getHash() and outputRecordKeys()
    setupIsKeyMatchInternal(cgInner, KeyMatchIncomingBuildMapping, KeyMatchHtableMapping, keyExprsBuild, htKeyFieldIds);
    setupIsKeyMatchInternal(cgInner, KeyMatchIncomingProbeMapping, KeyMatchHtableProbeMapping, keyExprsProbe,
        htKeyFieldIds);

    setupSetValue(cgInner, keyExprsBuild, htKeyFieldIds);
    if (outgoing != null) {

      if (outKeyFieldIds.length > htConfig.getKeyExprsBuild().length) {
        throw new IllegalArgumentException("Mismatched number of output key fields.");
      }
    }
    setupOutputRecordKeys(cgInner, htKeyFieldIds, outKeyFieldIds);

    /* Before generating the code for hashing the build and probe expressions
     * examine the expressions to make sure they are of the same type, add casts if necessary.
     * If they are not of the same type, hashing the same value of different types will yield different hash values.
     * NOTE: We add the cast only for the hash function, comparator function can handle the case
     * when expressions are different (for eg we have comparator functions that compare bigint and float8)
     * However for the hash to work correctly we would need to apply the cast.
     */
    addLeastRestrictiveCasts(keyExprsBuild, keyExprsProbe);

    setupGetHash(cg /* use top level code generator for getHash */, GetHashIncomingBuildMapping, keyExprsBuild, false);
    setupGetHash(cg /* use top level code generator for getHash */, GetHashIncomingProbeMapping, keyExprsProbe, true);

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

      JConditional jc;

      // codegen for nullable columns if nulls are not equal
      if (!areNullsEqual && left.isOptional() && right.isOptional()) {
        jc = cg.getEvalBlock()._if(left.getIsSet().eq(JExpr.lit(0)).
            cand(right.getIsSet().eq(JExpr.lit(0))));
        jc._then()._return(JExpr.FALSE);
      }

      final LogicalExpression f =
          FunctionGenerationHelper
          .getOrderingComparatorNullsHigh(left, right, context.getFunctionRegistry());

      HoldingContainer out = cg.addExpr(f, false);

      // check if two values are not equal (comparator result != 0)
      jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      jc._then()._return(JExpr.FALSE);
    }

    // All key expressions compared equal, so return TRUE
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  private void setupSetValue(ClassGenerator<HashTable> cg, LogicalExpression[] keyExprs,
                             TypedFieldId[] htKeyFieldIds) throws SchemaChangeException {

    cg.setMappingSet(SetValueMapping);

    int i = 0;
    for (LogicalExpression expr : keyExprs) {
      boolean useSetSafe = !Types.isFixedWidthType(expr.getMajorType()) || Types.isRepeated(expr.getMajorType());
      ValueVectorWriteExpression vvwExpr = new ValueVectorWriteExpression(htKeyFieldIds[i++], expr, useSetSafe);

      cg.addExpr(vvwExpr, false); // this will write to the htContainer at htRowIdx
    }
  }

  private void setupOutputRecordKeys(ClassGenerator<HashTable> cg, TypedFieldId[] htKeyFieldIds, TypedFieldId[] outKeyFieldIds) {

    cg.setMappingSet(OutputRecordKeysMapping);

    if (outKeyFieldIds != null) {
      for (int i = 0; i < outKeyFieldIds.length; i++) {
        ValueVectorReadExpression vvrExpr = new ValueVectorReadExpression(htKeyFieldIds[i]);
        boolean useSetSafe = !Types.isFixedWidthType(vvrExpr.getMajorType()) || Types.isRepeated(vvrExpr.getMajorType());
        ValueVectorWriteExpression vvwExpr = new ValueVectorWriteExpression(outKeyFieldIds[i], vvrExpr, useSetSafe);
        cg.addExpr(vvwExpr);
      }

    }
  }

  private void addLeastRestrictiveCasts(LogicalExpression[] keyExprsBuild, LogicalExpression[] keyExprsProbe) {

    // If we don't have probe expressions then nothing to do get out
    if (keyExprsProbe == null) {
      return;
    }

    assert keyExprsBuild.length == keyExprsProbe.length;

    for (int i = 0; i < keyExprsBuild.length; i++) {
      LogicalExpression buildExpr = keyExprsBuild[i];
      LogicalExpression probeExpr = keyExprsProbe[i];
      MinorType buildType = buildExpr.getMajorType().getMinorType();
      MinorType probeType = probeExpr.getMajorType().getMinorType();

      if (buildType != probeType) {
        // We need to add a cast to one of the expressions
        List<MinorType> types = new LinkedList<>();
        types.add(buildType);
        types.add(probeType);
        MinorType result = TypeCastRules.getLeastRestrictiveType(types);
        ErrorCollector errorCollector = new ErrorCollectorImpl();

        if (result == null) {
          throw new DrillRuntimeException(String.format("Join conditions cannot be compared failing build " +
                  "expression:" + " %s failing probe expression: %s", buildExpr.getMajorType().toString(),
              probeExpr.getMajorType().toString()));
        } else if (result != buildType) {
          // Add a cast expression on top of the build expression
          LogicalExpression castExpr = ExpressionTreeMaterializer.addCastExpression(buildExpr, probeExpr.getMajorType(), context.getFunctionRegistry(), errorCollector);
          // Store the newly casted expression
          keyExprsBuild[i] =
              ExpressionTreeMaterializer.materialize(castExpr, incomingBuild, errorCollector,
                  context.getFunctionRegistry());
        } else if (result != probeType) {
          // Add a cast expression on top of the probe expression
          LogicalExpression castExpr = ExpressionTreeMaterializer.addCastExpression(probeExpr, buildExpr.getMajorType(), context.getFunctionRegistry(), errorCollector);
          // store the newly casted expression
          keyExprsProbe[i] =
              ExpressionTreeMaterializer.materialize(castExpr, incomingProbe, errorCollector,
                  context.getFunctionRegistry());
        }
      }
    }
  }

  private void setupGetHash(ClassGenerator<HashTable> cg, MappingSet incomingMapping, LogicalExpression[] keyExprs,
                            boolean isProbe) throws SchemaChangeException {

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
      LogicalExpression hashfunc =
          FunctionGenerationHelper.getFunctionExpression("hash", Types.required(MinorType.INT),
              context.getFunctionRegistry(), input);
      HoldingContainer hashValue = cg.addExpr(hashfunc, false);

      if (i == 0) {
        combinedHashValue = hashValue; // first expression..just use the hash value
      } else {

        // compute the combined hash value using XOR
        LogicalExpression xorfunc =
            FunctionGenerationHelper.getFunctionExpression("xor", Types.required(MinorType.INT),
                context.getFunctionRegistry(), hashValue, combinedHashValue);
        combinedHashValue = cg.addExpr(xorfunc, false);
      }
    }

    if (combinedHashValue != null) {
      cg.getEvalBlock()._return(combinedHashValue.getValue());
    } else {
      cg.getEvalBlock()._return(JExpr.lit(0));
    }
  }
}
