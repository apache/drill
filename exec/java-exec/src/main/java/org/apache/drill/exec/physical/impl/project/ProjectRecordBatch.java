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
package org.apache.drill.exec.physical.impl.project;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.DrillComplexWriterFuncHolder;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.codemodel.JExpr;

public class ProjectRecordBatch extends AbstractSingleRecordBatch<Project> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectRecordBatch.class);

  private Projector projector;
  private List<ValueVector> allocationVectors;
  private List<ComplexWriter> complexWriters;
  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private int recordCount;

  private static final String EMPTY_STRING = "";

  private class ClassifierResult {
    public boolean isStar = false;
    public List<String> outputNames;
    public String prefix = "";
    public HashMap<String, Integer> prefixMap = Maps.newHashMap();
    public CaseInsensitiveMap outputMap = new CaseInsensitiveMap();
    private CaseInsensitiveMap sequenceMap = new CaseInsensitiveMap();

    private void clear() {
      isStar = false;
      prefix = "";
      if (outputNames != null) {
        outputNames.clear();
      }

      // note:  don't clear the internal maps since they have cumulative data..
    }
  }

  public ProjectRecordBatch(Project pop, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(pop, context, incoming);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }


  @Override
  protected void killIncoming(boolean sendUpstream) {
    super.killIncoming(sendUpstream);
    hasRemainder = false;
  }


  @Override
  public IterOutcome innerNext() {
    if (hasRemainder) {
      handleRemainder();
      return IterOutcome.OK;
    }
    return super.innerNext();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  @Override
  protected IterOutcome doWork() {
//    VectorUtil.showVectorAccessibleContent(incoming, ",");
    int incomingRecordCount = incoming.getRecordCount();

    if (!doAlloc()) {
      outOfMemory = true;
      return IterOutcome.OUT_OF_MEMORY;
    }

    int outputRecords = projector.projectRecords(0, incomingRecordCount, 0);
    if (outputRecords < incomingRecordCount) {
      setValueCount(outputRecords);
      hasRemainder = true;
      remainderIndex = outputRecords;
      this.recordCount = remainderIndex;
    } else {
      setValueCount(incomingRecordCount);
      for(VectorWrapper<?> v: incoming) {
        v.clear();
      }
      this.recordCount = outputRecords;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    return IterOutcome.OK;
  }

  private void handleRemainder() {
    int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    if (!doAlloc()) {
      outOfMemory = true;
      return;
    }
    int projRecords = projector.projectRecords(remainderIndex, remainingRecordCount, 0);
    if (projRecords < remainingRecordCount) {
      setValueCount(projRecords);
      this.recordCount = projRecords;
      remainderIndex += projRecords;
    } else {
      setValueCount(remainingRecordCount);
      hasRemainder = false;
      remainderIndex = 0;
      for (VectorWrapper<?> v : incoming) {
        v.clear();
      }
      this.recordCount = remainingRecordCount;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }
  }

  public void addComplexWriter(ComplexWriter writer) {
    complexWriters.add(writer);
  }

  private boolean doAlloc() {
    //Allocate vv in the allocationVectors.
    for (ValueVector v : this.allocationVectors) {
      //AllocationHelper.allocate(v, remainingRecordCount, 250);
      if (!v.allocateNewSafe()) {
        return false;
      }
    }

    //Allocate vv for complexWriters.
    if (complexWriters == null) {
      return true;
    }

    for (ComplexWriter writer : complexWriters) {
      writer.allocate();
    }

    return true;
  }

  private void setValueCount(int count) {
    for (ValueVector v : allocationVectors) {
      ValueVector.Mutator m = v.getMutator();
      m.setValueCount(count);
    }

    if (complexWriters == null) {
      return;
    }

    for (ComplexWriter writer : complexWriters) {
      writer.setValueCount(count);
    }
  }

  /** hack to make ref and full work together... need to figure out if this is still necessary. **/
  private FieldReference getRef(NamedExpression e) {
    FieldReference ref = e.getRef();
    PathSegment seg = ref.getRootSegment();

//    if (seg.isNamed() && "output".contentEquals(seg.getNameSegment().getPath())) {
//      return new FieldReference(ref.getPath().toString().subSequence(7, ref.getPath().length()), ref.getPosition());
//    }
    return ref;
  }

  private boolean isAnyWildcard(List<NamedExpression> exprs) {
    for (NamedExpression e : exprs) {
      if (isWildcard(e)) {
        return true;
      }
    }
    return false;
  }

  private boolean isWildcard(NamedExpression ex) {
    if ( !(ex.getExpr() instanceof SchemaPath)) {
      return false;
    }
    NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
    NameSegment ref = ex.getRef().getRootSegment();
    return ref.getPath().equals("*") && expr.getPath().equals("*");
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    this.allocationVectors = Lists.newArrayList();
    container.clear();
    final List<NamedExpression> exprs = getExpressionList();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<Projector> cg = CodeGenerator.getRoot(Projector.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    IntOpenHashSet transferFieldIds = new IntOpenHashSet();

    boolean isAnyWildcard = false;

    ClassifierResult result = new ClassifierResult();
    boolean classify = isClassificationNeeded(exprs);

    for (int i = 0; i < exprs.size(); i++) {
      final NamedExpression namedExpression = exprs.get(i);
      result.clear();

      if (classify && namedExpression.getExpr() instanceof SchemaPath) {
        classifyExpr(namedExpression, incoming, result);

        if (result.isStar) {
          isAnyWildcard = true;
          Integer value = result.prefixMap.get(result.prefix);
          if (value != null && value.intValue() == 1) {
            int k = 0;
            for (VectorWrapper<?> wrapper : incoming) {
              ValueVector vvIn = wrapper.getValueVector();
              SchemaPath originalPath = vvIn.getField().getPath();
              if (k > result.outputNames.size()-1) {
                assert false;
              }
              String name = result.outputNames.get(k++);  // get the renamed column names
              if (name == EMPTY_STRING) {
                continue;
              }
              FieldReference ref = new FieldReference(name);
              TransferPair tp = wrapper.getValueVector().getTransferPair(ref);
              transfers.add(tp);
              container.add(tp.getTo());
            }
          } else if (value != null && value.intValue() > 1) { // subsequent wildcards should do a copy of incoming valuevectors
            int k = 0;
            for (VectorWrapper<?> wrapper : incoming) {
              ValueVector vvIn = wrapper.getValueVector();
              SchemaPath originalPath = vvIn.getField().getPath();
              if (k > result.outputNames.size()-1) {
                assert false;
              }
              String name = result.outputNames.get(k++);  // get the renamed column names
              if (name == EMPTY_STRING) {
                continue;
              }

              final LogicalExpression expr = ExpressionTreeMaterializer.materialize(originalPath, incoming, collector, context.getFunctionRegistry() );
              if (collector.hasErrors()) {
                throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
              }

              MaterializedField outputField = MaterializedField.create(name, expr.getMajorType());
              ValueVector vv = TypeHelper.getNewVector(outputField, oContext.getAllocator());
              allocationVectors.add(vv);
              TypedFieldId fid = container.add(vv);
              ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
              HoldingContainer hc = cg.addExpr(write);

              cg.getEvalBlock()._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
            }
          }
          continue;
        }
      }

      String outputName = getRef(namedExpression).getRootSegment().getPath();
      if (result != null && result.outputNames != null && result.outputNames.size() > 0) {
        for (int j = 0; j < result.outputNames.size(); j++) {
          if (!result.outputNames.get(j).equals(EMPTY_STRING)) {
            outputName = result.outputNames.get(j);
            break;
          }
        }
      }

      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incoming, collector, context.getFunctionRegistry(), true);
      final MaterializedField outputField = MaterializedField.create(outputName, expr.getMajorType());
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
      if (expr instanceof ValueVectorReadExpression && incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE
          && !((ValueVectorReadExpression) expr).hasReadPath()
          && !isAnyWildcard
          && !transferFieldIds.contains(((ValueVectorReadExpression) expr).getFieldId().getFieldIds()[0])) {

        ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        TypedFieldId id = vectorRead.getFieldId();
        ValueVector vvIn = incoming.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
        Preconditions.checkNotNull(incoming);

        TransferPair tp = vvIn.getTransferPair(getRef(namedExpression));
        transfers.add(tp);
        container.add(tp.getTo());
        transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
        logger.debug("Added transfer for project expression.");
      } else if (expr instanceof DrillFuncHolderExpr &&
          ((DrillFuncHolderExpr) expr).isComplexWriterFuncHolder())  {
        // Need to process ComplexWriter function evaluation.
        // Lazy initialization of the list of complex writers, if not done yet.
        if (complexWriters == null) {
          complexWriters = Lists.newArrayList();
        }

        // The reference name will be passed to ComplexWriter, used as the name of the output vector from the writer.
        ((DrillComplexWriterFuncHolder) ((DrillFuncHolderExpr) expr).getHolder()).setReference(namedExpression.getRef());
        cg.addExpr(expr);
      } else{
        // need to do evaluation.
        ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());
        allocationVectors.add(vector);
        TypedFieldId fid = container.add(vector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
        HoldingContainer hc = cg.addExpr(write);

        cg.getEvalBlock()._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
        logger.debug("Added eval for project expression.");
      }
    }

    cg.rotateBlock();
    cg.getEvalBlock()._return(JExpr.TRUE);

    container.buildSchema(SelectionVectorMode.NONE);

    try {
      this.projector = context.getImplementationClass(cg.getCodeGenerator());
      projector.setup(context, incoming, this, transfers);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }

  private List<NamedExpression> getExpressionList() {
    if (popConfig.getExprs() != null) {
      return popConfig.getExprs();
    }

    List<NamedExpression> exprs = Lists.newArrayList();
    for (MaterializedField field : incoming.getSchema()) {
      if (Types.isComplex(field.getType()) || Types.isRepeated(field.getType())) {
        LogicalExpression convertToJson = FunctionCallFactory.createConvert(ConvertExpression.CONVERT_TO, "JSON", field.getPath(), ExpressionPosition.UNKNOWN);
        String castFuncName = CastFunctions.getCastFunc(MinorType.VARCHAR);
        List<LogicalExpression> castArgs = Lists.newArrayList();
        castArgs.add(convertToJson);  //input_expr
        /*
         * We are implicitly casting to VARCHAR so we don't have a max length,
         * using an arbitrary value. We trim down the size of the stored bytes
         * to the actual size so this size doesn't really matter.
         */
        castArgs.add(new ValueExpressions.LongExpression(65536, null)); //
        FunctionCall castCall = new FunctionCall(castFuncName, castArgs, ExpressionPosition.UNKNOWN);
        exprs.add(new NamedExpression(castCall, new FieldReference(field.getPath())));
      } else {
        exprs.add(new NamedExpression(field.getPath(), new FieldReference(field.getPath())));
      }
    }
    return exprs;
  }

  private boolean isClassificationNeeded(List<NamedExpression> exprs) {
    boolean needed = false;
    for (int i = 0; i < exprs.size(); i++) {
      final NamedExpression ex = exprs.get(i);
      if (!(ex.getExpr() instanceof SchemaPath)) {
        continue;
      }
      NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
      NameSegment ref = ex.getRef().getRootSegment();
      boolean refHasPrefix = ref.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
      boolean exprContainsStar = expr.getPath().contains(StarColumnHelper.STAR_COLUMN);

      if (refHasPrefix || exprContainsStar) {
        needed = true;
        break;
      }
    }
    return needed;
  }

  private String getUniqueName(String name, ClassifierResult result) {
    Integer currentSeq = (Integer) result.sequenceMap.get(name);
    if (currentSeq == null) { // name is unique, so return the original name
      Integer n = -1;
      result.sequenceMap.put(name, n);
      return name;
    }
    // create a new name
    Integer newSeq = currentSeq + 1;
    result.sequenceMap.put(name, newSeq);

    String newName = name + newSeq;
    return newName;
  }

  private void addToResultMaps(String origName, ClassifierResult result, boolean allowDupsWithRename) {
    String name = origName;
    if (allowDupsWithRename) {
      name = getUniqueName(origName, result);
    }
    if (!result.outputMap.containsKey(name)) {
      result.outputNames.add(name);
      result.outputMap.put(name,  name);
    } else {
      result.outputNames.add(EMPTY_STRING);
    }
  }

  private void classifyExpr(NamedExpression ex, RecordBatch incoming, ClassifierResult result)  {
    NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
    NameSegment ref = ex.getRef().getRootSegment();
    boolean exprHasPrefix = expr.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
    boolean refHasPrefix = ref.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
    boolean exprIsStar = expr.getPath().equals(StarColumnHelper.STAR_COLUMN);
    boolean refContainsStar = ref.getPath().contains(StarColumnHelper.STAR_COLUMN);
    boolean exprContainsStar = expr.getPath().contains(StarColumnHelper.STAR_COLUMN);
    boolean refEndsWithStar = ref.getPath().endsWith(StarColumnHelper.STAR_COLUMN);

    String exprPrefix = EMPTY_STRING;
    String exprSuffix = expr.getPath();

    if (exprHasPrefix) {
      // get the prefix of the expr
      String[] exprComponents = expr.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert(exprComponents.length == 2);
      exprPrefix = exprComponents[0];
      exprSuffix = exprComponents[1];
      result.prefix = exprPrefix;
    }

    if (exprContainsStar) {
      result.isStar = true;
      Integer value = (Integer) result.prefixMap.get(exprPrefix);
      if (value == null) {
        Integer n = 1;
        result.prefixMap.put(exprPrefix, n);
      } else {
        Integer n = value + 1;
        result.prefixMap.put(exprPrefix, n);
      }
    }

    int incomingSchemaSize = incoming.getSchema().getFieldCount();

    // for debugging..
    // if (incomingSchemaSize > 9) {
    // assert false;
    // }

    // input is '*' and output is 'prefix_*'
    if (exprIsStar && refHasPrefix && refEndsWithStar) {
      String[] components = ref.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert(components.length == 2);
      String prefix = components[0];
      result.outputNames = Lists.newArrayList();
      for(VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String name = vvIn.getField().getPath().getRootSegment().getPath();

        // add the prefix to the incoming column name
        String newName = prefix + StarColumnHelper.PREFIX_DELIMITER + name;
        addToResultMaps(newName, result, false);
      }
    }
    // input and output are the same
    else if (expr.getPath().equals(ref.getPath())) {
      if (exprContainsStar && exprHasPrefix) {
        assert exprPrefix != null;

        int k = 0;
        result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
        for (int j=0; j < incomingSchemaSize; j++) {
          result.outputNames.add(EMPTY_STRING);  // initialize
        }

        for (VectorWrapper<?> wrapper : incoming) {
          ValueVector vvIn = wrapper.getValueVector();
          String incomingName = vvIn.getField().getPath().getRootSegment().getPath();
          // get the prefix of the name
          String[] nameComponents = incomingName.split(StarColumnHelper.PREFIX_DELIMITER, 2);
          // if incoming valuevector does not have a prefix, ignore it since this expression is not referencing it
          if (nameComponents.length <= 1) {
            k++;
            continue;
          }
          String namePrefix = nameComponents[0];
          if (exprPrefix.equals(namePrefix)) {
            String newName = incomingName;
            if (!result.outputMap.containsKey(newName)) {
              result.outputNames.set(k, newName);
              result.outputMap.put(newName,  newName);
            }
          }
          k++;
        }
      } else {
        result.outputNames = Lists.newArrayList();
        if (exprContainsStar) {
          for (VectorWrapper<?> wrapper : incoming) {
            ValueVector vvIn = wrapper.getValueVector();
            String incomingName = vvIn.getField().getPath().getRootSegment().getPath();
            if (refContainsStar) {
              addToResultMaps(incomingName, result, true); // allow dups since this is likely top-level project
            } else {
              addToResultMaps(incomingName, result, false);
            }
          }
        } else {
          String newName = expr.getPath();
          if (!refHasPrefix && !exprHasPrefix) {
            addToResultMaps(newName, result, true); // allow dups since this is likely top-level project
          } else {
            addToResultMaps(newName, result, false);
          }
        }
      }
    }
    // only the output has prefix
    else if (!exprHasPrefix && refHasPrefix) {
      result.outputNames = Lists.newArrayList();
      String newName = ref.getPath();
      addToResultMaps(newName, result, false);
    }
    // input has prefix but output does not
    else if (exprHasPrefix && !refHasPrefix) {
      int k = 0;
      result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
      for (int j=0; j < incomingSchemaSize; j++) {
        result.outputNames.add(EMPTY_STRING);  // initialize
      }

      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String name = vvIn.getField().getPath().getRootSegment().getPath();
        String[] components = name.split(StarColumnHelper.PREFIX_DELIMITER, 2);
        if (components.length <= 1)  {
          k++;
          continue;
        }
        String namePrefix = components[0];
        String nameSuffix = components[1];
        if (exprPrefix.equals(namePrefix)) {
          if (refContainsStar) {
            // remove the prefix from the incoming column names
            String newName = getUniqueName(nameSuffix, result);  // for top level we need to make names unique
            result.outputNames.set(k, newName);
          } else if (exprSuffix.equals(nameSuffix)) {
            // example: ref: $f1, expr: T0<PREFIX><column_name>
            String newName = ref.getPath();
            result.outputNames.set(k, newName);
          }
        } else {
          result.outputNames.add(EMPTY_STRING);
        }
        k++;
      }
    }
    // input and output have prefixes although they could be different...
    else if (exprHasPrefix && refHasPrefix) {
      String[] input = expr.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert(input.length == 2);
      assert false : "Unexpected project expression or reference";  // not handled yet
    }
    else {
      // if the incoming schema's column name matches the expression name of the Project,
      // then we just want to pick the ref name as the output column name
      result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
      for (int j=0; j < incomingSchemaSize; j++) {
        result.outputNames.add(EMPTY_STRING);  // initialize
      }

      int k = 0;
      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String incomingName = vvIn.getField().getPath().getRootSegment().getPath();

        if (expr.getPath().equals(incomingName)) {
          String newName = ref.getPath();
          if (!result.outputMap.containsKey(newName)) {
            result.outputNames.set(k, newName);
            result.outputMap.put(newName,  newName);
          }
        }
        k++;
      }
    }
  }

}
