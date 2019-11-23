/*
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

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;

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
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.FunctionReplacementUtils;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SimpleRecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.UntypedNullHolder;
import org.apache.drill.exec.vector.UntypedNullVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntHashSet;

public class ProjectRecordBatch extends AbstractSingleRecordBatch<Project> {
  private static final Logger logger = LoggerFactory.getLogger(ProjectRecordBatch.class);

  private static final String EMPTY_STRING = "";

  private Projector projector;
  private List<ValueVector> allocationVectors;
  private List<ComplexWriter> complexWriters;
  private List<FieldReference> complexFieldReferencesList;
  private boolean hasRemainder;
  private int remainderIndex;
  private int recordCount;
  private ProjectMemoryManager memoryManager;
  private boolean first = true;
  private boolean wasNone; // whether a NONE iter outcome was already seen
  private final ColumnExplorer columnExplorer;

  private class ClassifierResult {
    public boolean isStar;
    public List<String> outputNames;
    public String prefix = "";
    public HashMap<String, Integer> prefixMap = Maps.newHashMap();
    public CaseInsensitiveMap outputMap = new CaseInsensitiveMap();
    private final CaseInsensitiveMap sequenceMap = new CaseInsensitiveMap();

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
    columnExplorer = new ColumnExplorer(context.getOptions());
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
    if (wasNone) {
      return IterOutcome.NONE;
    }
    recordCount = 0;
    if (hasRemainder) {
      handleRemainder();
      // Check if we are supposed to return EMIT outcome and have consumed entire batch
      return getFinalOutcome(hasRemainder);
    }
    return super.innerNext();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return container;
  }

  @Override
  protected IterOutcome doWork() {
    if (wasNone) {
      return IterOutcome.NONE;
    }

    int incomingRecordCount = incoming.getRecordCount();

    logger.trace("doWork(): incoming rc {}, incoming {}, Project {}", incomingRecordCount, incoming, this);
    //calculate the output row count
    memoryManager.update();

    if (first && incomingRecordCount == 0) {
      if (complexWriters != null) {
        IterOutcome next = null;
        while (incomingRecordCount == 0) {
          if (getLastKnownOutcome() == EMIT) {
            throw new UnsupportedOperationException("Currently functions producing complex types as output are not " +
                    "supported in project list for subquery between LATERAL and UNNEST. Please re-write the query using this " +
                    "function in the projection list of outermost query.");
          }

          next = next(incoming);
          setLastKnownOutcome(next);
          if (next == IterOutcome.OUT_OF_MEMORY) {
            outOfMemory = true;
            return next;
          } else if (next == IterOutcome.NONE) {
            // since this is first batch and we already got a NONE, need to set up the schema
            doAlloc(0);
            setValueCount(0);

            // Only need to add the schema for the complex exprs because others should already have
            // been setup during setupNewSchema
            for (FieldReference fieldReference : complexFieldReferencesList) {
              MaterializedField field = MaterializedField.create(fieldReference.getAsNamePart().getName(),
                      UntypedNullHolder.TYPE);
              container.add(new UntypedNullVector(field, container.getAllocator()));
            }
            container.buildSchema(SelectionVectorMode.NONE);
            wasNone = true;
            return IterOutcome.OK_NEW_SCHEMA;
          } else if (next != IterOutcome.OK && next != IterOutcome.OK_NEW_SCHEMA && next != EMIT) {
            return next;
          } else if (next == IterOutcome.OK_NEW_SCHEMA) {
            try {
              setupNewSchema();
            } catch (SchemaChangeException e) {
              throw new RuntimeException(e);
            }
          }
          incomingRecordCount = incoming.getRecordCount();
          memoryManager.update();
          logger.trace("doWork():[1] memMgr RC {}, incoming rc {}, incoming {}, Project {}",
                       memoryManager.getOutputRowCount(), incomingRecordCount, incoming, this);
        }
      }
    }

    if (complexWriters != null && getLastKnownOutcome() == EMIT) {
      throw new UnsupportedOperationException("Currently functions producing complex types as output are not " +
        "supported in project list for subquery between LATERAL and UNNEST. Please re-write the query using this " +
        "function in the projection list of outermost query.");
    }

    first = false;
    container.zeroVectors();

    int maxOuputRecordCount = memoryManager.getOutputRowCount();
    logger.trace("doWork():[2] memMgr RC {}, incoming rc {}, incoming {}, project {}",
                 memoryManager.getOutputRowCount(), incomingRecordCount, incoming, this);

    doAlloc(maxOuputRecordCount);
    long projectStartTime = System.currentTimeMillis();
    int outputRecords = projector.projectRecords(incoming, 0, maxOuputRecordCount, 0);
    long projectEndTime = System.currentTimeMillis();
    logger.trace("doWork(): projection: records {}, time {} ms", outputRecords, (projectEndTime - projectStartTime));

    setValueCount(outputRecords);
    recordCount = outputRecords;
    if (outputRecords < incomingRecordCount) {
      hasRemainder = true;
      remainderIndex = outputRecords;
    } else {
      assert outputRecords == incomingRecordCount;
      incoming.getContainer().zeroVectors();
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    memoryManager.updateOutgoingStats(outputRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());

    // Get the final outcome based on hasRemainder since that will determine if all the incoming records were
    // consumed in current output batch or not
    return getFinalOutcome(hasRemainder);
  }

  private void handleRemainder() {
    int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    assert this.memoryManager.incomingBatch == incoming;
    int recordsToProcess = Math.min(remainingRecordCount, memoryManager.getOutputRowCount());
    doAlloc(recordsToProcess);

    logger.trace("handleRemainder: remaining RC {}, toProcess {}, remainder index {}, incoming {}, Project {}",
                 remainingRecordCount, recordsToProcess, remainderIndex, incoming, this);

    long projectStartTime = System.currentTimeMillis();
    int projRecords = projector.projectRecords(this.incoming, remainderIndex, recordsToProcess, 0);
    long projectEndTime = System.currentTimeMillis();

    logger.trace("handleRemainder: projection: records {}, time {} ms", projRecords,(projectEndTime - projectStartTime));

    if (projRecords < remainingRecordCount) {
      setValueCount(projRecords);
      recordCount = projRecords;
      remainderIndex += projRecords;
    } else {
      setValueCount(remainingRecordCount);
      hasRemainder = false;
      remainderIndex = 0;
      incoming.getContainer().zeroVectors();
      recordCount = remainingRecordCount;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    memoryManager.updateOutgoingStats(projRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());
  }

  public void addComplexWriter(ComplexWriter writer) {
    complexWriters.add(writer);
  }

  private void doAlloc(int recordCount) {
    // Allocate vv in the allocationVectors.
    for (ValueVector v : allocationVectors) {
      AllocationHelper.allocateNew(v, recordCount);
    }

    // Allocate vv for complexWriters.
    if (complexWriters != null) {
      for (ComplexWriter writer : complexWriters) {
        writer.allocate();
      }
    }
  }

  private void setValueCount(int count) {
    if (count == 0) {
      container.setEmpty();
      return;
    }
    for (ValueVector v : allocationVectors) {
      v.getMutator().setValueCount(count);
    }

    // Value counts for vectors should have been set via
    // the transfer pairs or vector copies.
    container.setRecordCount(count);

    if (complexWriters == null) {
      return;
    }

    for (ComplexWriter writer : complexWriters) {
      writer.setValueCount(count);
    }
  }

  // hack to make ref and full work together... need to figure out if this is
  // still necessary.
  private FieldReference getRef(NamedExpression e) {
    return e.getRef();
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
    if (!(ex.getExpr() instanceof SchemaPath)) {
      return false;
    }
    NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
    return expr.getPath().contains(SchemaPath.DYNAMIC_STAR);
  }

  private void setupNewSchemaFromInput(RecordBatch incomingBatch) throws SchemaChangeException {
    long setupNewSchemaStartTime = System.currentTimeMillis();
    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    memoryManager = new ProjectMemoryManager(configuredBatchSize);
    memoryManager.init(incomingBatch, this);
    if (allocationVectors != null) {
      for (ValueVector v : allocationVectors) {
        v.clear();
      }
    }
    allocationVectors = Lists.newArrayList();

    if (complexWriters != null) {
      container.clear();
    } else {
      // Release the underlying DrillBufs and reset the ValueVectors to empty
      // Not clearing the container here is fine since Project output schema is
      // not determined solely based on incoming batch. It is defined by the
      // expressions it has to evaluate.
      //
      // If there is a case where only the type of ValueVector already present
      // in container is changed then addOrGet method takes care of it by
      // replacing the vectors.
      container.zeroVectors();
    }

    List<NamedExpression> exprs = getExpressionList();
    ErrorCollector collector = new ErrorCollectorImpl();
    List<TransferPair> transfers = Lists.newArrayList();

    ClassGenerator<Projector> cg = CodeGenerator.getRoot(Projector.TEMPLATE_DEFINITION, context.getOptions());
    cg.getCodeGenerator().plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
    // cg.getCodeGenerator().saveCodeForDebugging(true);

    IntHashSet transferFieldIds = new IntHashSet();

    boolean isAnyWildcard = isAnyWildcard(exprs);

    ClassifierResult result = new ClassifierResult();
    boolean classify = isClassificationNeeded(exprs);

    for (NamedExpression namedExpression : exprs) {
      result.clear();
      if (classify && namedExpression.getExpr() instanceof SchemaPath) {
        classifyExpr(namedExpression, incomingBatch, result);

        if (result.isStar) {
          // The value indicates which wildcard we are processing now
          Integer value = result.prefixMap.get(result.prefix);
          if (value != null && value == 1) {
            int k = 0;
            for (VectorWrapper<?> wrapper : incomingBatch) {
              ValueVector vvIn = wrapper.getValueVector();
              if (k > result.outputNames.size() - 1) {
                assert false;
              }
              String name = result.outputNames.get(k++);  // get the renamed column names
              if (name.isEmpty()) {
                continue;
              }

              if (isImplicitFileColumn(vvIn)) {
                continue;
              }

              FieldReference ref = new FieldReference(name);
              ValueVector vvOut = container.addOrGet(MaterializedField.create(ref.getAsNamePart().getName(),
                vvIn.getField().getType()), callBack);
              TransferPair tp = vvIn.makeTransferPair(vvOut);
              memoryManager.addTransferField(vvIn, vvIn.getField().getName(), vvOut.getField().getName());
              transfers.add(tp);
            }
          } else if (value != null && value > 1) { // subsequent wildcards should do a copy of incoming value vectors
            int k = 0;
            for (VectorWrapper<?> wrapper : incomingBatch) {
              ValueVector vvIn = wrapper.getValueVector();
              SchemaPath originalPath = SchemaPath.getSimplePath(vvIn.getField().getName());
              if (k > result.outputNames.size() - 1) {
                assert false;
              }
              String name = result.outputNames.get(k++);  // get the renamed column names
              if (name.isEmpty()) {
                continue;
              }

              if (isImplicitFileColumn(vvIn)) {
                continue;
              }

              LogicalExpression expr = ExpressionTreeMaterializer.materialize(originalPath, incomingBatch, collector, context.getFunctionRegistry() );
              if (collector.hasErrors()) {
                throw new SchemaChangeException(String.format("Failure while trying to materialize incomingBatch schema.  Errors:\n %s.", collector.toErrorString()));
              }

              MaterializedField outputField = MaterializedField.create(name, expr.getMajorType());
              ValueVector vv = container.addOrGet(outputField, callBack);
              allocationVectors.add(vv);
              TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
              ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
              memoryManager.addNewField(vv, write);
              cg.addExpr(write, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
            }
          }
          continue;
        }
      } else {
        // For the columns which do not needed to be classified,
        // it is still necessary to ensure the output column name is unique
        result.outputNames = Lists.newArrayList();
        String outputName = getRef(namedExpression).getRootSegment().getPath(); //moved to before the if
        addToResultMaps(outputName, result, true);
      }
      String outputName = getRef(namedExpression).getRootSegment().getPath();
      if (result != null && result.outputNames != null && result.outputNames.size() > 0) {
        boolean isMatched = false;
        for (int j = 0; j < result.outputNames.size(); j++) {
          if (!result.outputNames.get(j).isEmpty()) {
            outputName = result.outputNames.get(j);
            isMatched = true;
            break;
          }
        }

        if (!isMatched) {
          continue;
        }
      }

      LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incomingBatch,
          collector, context.getFunctionRegistry(), true, unionTypeEnabled);
      MaterializedField outputField = MaterializedField.create(outputName, expr.getMajorType());
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
      if (expr instanceof ValueVectorReadExpression && incomingBatch.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE
          && !((ValueVectorReadExpression) expr).hasReadPath()
          && !isAnyWildcard
          && !transferFieldIds.contains(((ValueVectorReadExpression) expr).getFieldId().getFieldIds()[0])) {

        ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        TypedFieldId id = vectorRead.getFieldId();
        ValueVector vvIn = incomingBatch.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
        Preconditions.checkNotNull(incomingBatch);

        FieldReference ref = getRef(namedExpression);
        ValueVector vvOut =
          container.addOrGet(MaterializedField.create(ref.getLastSegment().getNameSegment().getPath(),
            vectorRead.getMajorType()), callBack);
        TransferPair tp = vvIn.makeTransferPair(vvOut);
        memoryManager.addTransferField(vvIn, TypedFieldId.getPath(id, incomingBatch), vvOut.getField().getName());
        transfers.add(tp);
        transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
      } else if (expr instanceof DrillFuncHolderExpr &&
          ((DrillFuncHolderExpr) expr).getHolder().isComplexWriterFuncHolder()) {
        // Need to process ComplexWriter function evaluation.
        // Lazy initialization of the list of complex writers, if not done yet.
        if (complexWriters == null) {
          complexWriters = Lists.newArrayList();
        } else {
          complexWriters.clear();
        }

        // The reference name will be passed to ComplexWriter, used as the name of the output vector from the writer.
        ((DrillFuncHolderExpr) expr).getFieldReference(namedExpression.getRef());
        cg.addExpr(expr, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
        if (complexFieldReferencesList == null) {
          complexFieldReferencesList = Lists.newArrayList();
        } else {
          complexFieldReferencesList.clear();
        }

        // save the field reference for later for getting schema when input is empty
        complexFieldReferencesList.add(namedExpression.getRef());
        memoryManager.addComplexField(null); // this will just add an estimate to the row width
      } else {
        // need to do evaluation.
        ValueVector ouputVector = container.addOrGet(outputField, callBack);
        allocationVectors.add(ouputVector);
        TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));
        boolean useSetSafe = !(ouputVector instanceof FixedWidthVector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        cg.addExpr(write, ClassGenerator.BlkCreateMode.TRUE_IF_BOUND);
        memoryManager.addNewField(ouputVector, write);

        // We cannot do multiple transfers from the same vector. However we still need to instantiate the output vector.
        if (expr instanceof ValueVectorReadExpression) {
          ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
          if (!vectorRead.hasReadPath()) {
            TypedFieldId id = vectorRead.getFieldId();
            ValueVector vvIn = incomingBatch.getValueAccessorById(id.getIntermediateClass(),
                    id.getFieldIds()).getValueVector();
            vvIn.makeTransferPair(ouputVector);
          }
        }
      }
    }

    try {
      CodeGenerator<Projector> codeGen = cg.getCodeGenerator();
      codeGen.plainJavaCapable(true);
      // Uncomment out this line to debug the generated code.
      //codeGen.saveCodeForDebugging(true);
      this.projector = context.getImplementationClass(codeGen);
      projector.setup(context, incomingBatch, this, transfers);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }

    long setupNewSchemaEndTime = System.currentTimeMillis();
      logger.trace("setupNewSchemaFromInput: time {}  ms, Project {}, incoming {}",
                  (setupNewSchemaEndTime - setupNewSchemaStartTime), this, incomingBatch);
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    setupNewSchemaFromInput(this.incoming);
    if (container.isSchemaChanged() || callBack.getSchemaChangedAndReset()) {
      container.buildSchema(SelectionVectorMode.NONE);
      return true;
    } else {
      return false;
    }
  }

  private boolean isImplicitFileColumn(ValueVector vvIn) {
    return columnExplorer.isImplicitFileColumn(vvIn.getField().getName());
  }

  private List<NamedExpression> getExpressionList() {
    if (popConfig.getExprs() != null) {
      return popConfig.getExprs();
    }

    List<NamedExpression> exprs = Lists.newArrayList();
    for (MaterializedField field : incoming.getSchema()) {
      String fieldName = field.getName();
      if (Types.isComplex(field.getType()) || Types.isRepeated(field.getType())) {
        LogicalExpression convertToJson = FunctionCallFactory.createConvert(ConvertExpression.CONVERT_TO, "JSON",
                                                            SchemaPath.getSimplePath(fieldName), ExpressionPosition.UNKNOWN);
        String castFuncName = FunctionReplacementUtils.getCastFunc(MinorType.VARCHAR);
        List<LogicalExpression> castArgs = Lists.newArrayList();
        castArgs.add(convertToJson);  //input_expr
        // implicitly casting to varchar, since we don't know actual source length, cast to undefined length, which will preserve source length
        castArgs.add(new ValueExpressions.LongExpression(Types.MAX_VARCHAR_LENGTH, null));
        FunctionCall castCall = new FunctionCall(castFuncName, castArgs, ExpressionPosition.UNKNOWN);
        exprs.add(new NamedExpression(castCall, new FieldReference(fieldName)));
      } else {
        exprs.add(new NamedExpression(SchemaPath.getSimplePath(fieldName), new FieldReference(fieldName)));
      }
    }
    return exprs;
  }

  private boolean isClassificationNeeded(List<NamedExpression> exprs) {
    boolean needed = false;
    for (NamedExpression ex : exprs) {
      if (!(ex.getExpr() instanceof SchemaPath)) {
        continue;
      }
      NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
      NameSegment ref = ex.getRef().getRootSegment();
      boolean refHasPrefix = ref.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
      boolean exprContainsStar = expr.getPath().contains(SchemaPath.DYNAMIC_STAR);

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
      result.sequenceMap.put(name, -1);
      return name;
    }
    // create a new name
    int newSeq = currentSeq + 1;
    String newName = name + newSeq;
    result.sequenceMap.put(name, newSeq);
    result.sequenceMap.put(newName, -1);

    return newName;
  }

  /**
   * Helper method to ensure unique output column names. If allowDupsWithRename
   * is set to true, the original name will be appended with a suffix number to
   * ensure uniqueness. Otherwise, the original column would not be renamed even
   * even if it has been used
   *
   * @param origName
   *          the original input name of the column
   * @param result
   *          the data structure to keep track of the used names and decide what
   *          output name should be to ensure uniqueness
   * @param allowDupsWithRename
   *          if the original name has been used, is renaming allowed to ensure
   *          output name unique
   */
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
    boolean exprIsStar = expr.getPath().equals(SchemaPath.DYNAMIC_STAR);
    boolean refContainsStar = ref.getPath().contains(SchemaPath.DYNAMIC_STAR);
    boolean exprContainsStar = expr.getPath().contains(SchemaPath.DYNAMIC_STAR);
    boolean refEndsWithStar = ref.getPath().endsWith(SchemaPath.DYNAMIC_STAR);

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

    boolean exprIsFirstWildcard = false;
    if (exprContainsStar) {
      result.isStar = true;
      Integer value = result.prefixMap.get(exprPrefix);
      if (value == null) {
        result.prefixMap.put(exprPrefix, 1);
        exprIsFirstWildcard = true;
      } else {
        result.prefixMap.put(exprPrefix, value + 1);
      }
    }

    int incomingSchemaSize = incoming.getSchema().getFieldCount();

    // input is '*' and output is 'prefix_*'
    if (exprIsStar && refHasPrefix && refEndsWithStar) {
      String[] components = ref.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert(components.length == 2);
      String prefix = components[0];
      result.outputNames = Lists.newArrayList();
      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String name = vvIn.getField().getName();

        // add the prefix to the incoming column name
        String newName = prefix + StarColumnHelper.PREFIX_DELIMITER + name;
        addToResultMaps(newName, result, false);
      }
    }
    // input and output are the same
    else if (expr.getPath().equalsIgnoreCase(ref.getPath()) && (!exprContainsStar || exprIsFirstWildcard)) {
      if (exprContainsStar && exprHasPrefix) {
        assert exprPrefix != null;

        int k = 0;
        result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
        for (int j=0; j < incomingSchemaSize; j++) {
          result.outputNames.add(EMPTY_STRING);  // initialize
        }

        for (VectorWrapper<?> wrapper : incoming) {
          ValueVector vvIn = wrapper.getValueVector();
          String incomingName = vvIn.getField().getName();
          // get the prefix of the name
          String[] nameComponents = incomingName.split(StarColumnHelper.PREFIX_DELIMITER, 2);
          // if incoming value vector does not have a prefix, ignore it since
          // this expression is not referencing it
          if (nameComponents.length <= 1) {
            k++;
            continue;
          }
          String namePrefix = nameComponents[0];
          if (exprPrefix.equalsIgnoreCase(namePrefix)) {
            if (!result.outputMap.containsKey(incomingName)) {
              result.outputNames.set(k, incomingName);
              result.outputMap.put(incomingName, incomingName);
            }
          }
          k++;
        }
      } else {
        result.outputNames = Lists.newArrayList();
        if (exprContainsStar) {
          for (VectorWrapper<?> wrapper : incoming) {
            ValueVector vvIn = wrapper.getValueVector();
            String incomingName = vvIn.getField().getName();
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

    // input is wildcard and it is not the first wildcard
    else if (exprIsStar) {
      result.outputNames = Lists.newArrayList();
      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String incomingName = vvIn.getField().getName();
        addToResultMaps(incomingName, result, true); // allow dups since this is likely top-level project
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
        String name = vvIn.getField().getName();
        String[] components = name.split(StarColumnHelper.PREFIX_DELIMITER, 2);
        if (components.length <= 1)  {
          k++;
          continue;
        }
        String namePrefix = components[0];
        String nameSuffix = components[1];
        if (exprPrefix.equalsIgnoreCase(namePrefix)) {  // // case insensitive matching of prefix.
          if (refContainsStar) {
            // remove the prefix from the incoming column names
            String newName = getUniqueName(nameSuffix, result);  // for top level we need to make names unique
            result.outputNames.set(k, newName);
          } else if (exprSuffix.equalsIgnoreCase(nameSuffix)) { // case insensitive matching of field name.
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

      result.outputNames = Lists.newArrayList();
      for (VectorWrapper<?> wrapper : incoming) {
        ValueVector vvIn = wrapper.getValueVector();
        String incomingName = vvIn.getField().getName();
        if (expr.getPath().equalsIgnoreCase(incomingName)) {  // case insensitive matching of field name.
          String newName = ref.getPath();
          addToResultMaps(newName, result, true);
        }
      }
    }
  }

  /**
   * Handle Null input specially when Project operator is for query output. This happens when input return 0 batch
   * (returns a FAST NONE directly).
   *
   * <p>
   * Project operator has to return a batch with schema derived using the following 3 rules:
   * </p>
   * <ul>
   *  <li>Case 1:  *  ==>  expand into an empty list of columns. </li>
   *  <li>Case 2:  regular column reference ==> treat as nullable-int column </li>
   *  <li>Case 3:  expressions => Call ExpressionTreeMaterialization over an empty vector contain.
   *           Once the expression is materialized without error, use the output type of materialized
   *           expression. </li>
   * </ul>
   *
   * <p>
   * The batch is constructed with the above rules, and recordCount = 0.
   * Returned with OK_NEW_SCHEMA to down-stream operator.
   * </p>
   */
  @Override
  protected IterOutcome handleNullInput() {
    if (!popConfig.isOutputProj()) {
      return super.handleNullInput();
    }

    VectorContainer emptyVC = new VectorContainer();
    emptyVC.buildSchema(SelectionVectorMode.NONE);
    RecordBatch emptyIncomingBatch = new SimpleRecordBatch(emptyVC, context);

    try {
      setupNewSchemaFromInput(emptyIncomingBatch);
    } catch (SchemaChangeException e) {
      kill(false);
      logger.error("Failure during query", e);
      context.getExecutorState().fail(e);
      return IterOutcome.STOP;
    }

    doAlloc(0);
    container.buildSchema(SelectionVectorMode.NONE);
    container.setEmpty();
    wasNone = true;
    return IterOutcome.OK_NEW_SCHEMA;
  }

  @Override
  public void dump() {
    logger.error("ProjectRecordBatch[projector={}, hasRemainder={}, remainderIndex={}, recordCount={}, container={}]",
        projector, hasRemainder, remainderIndex, recordCount, container);
  }
}
