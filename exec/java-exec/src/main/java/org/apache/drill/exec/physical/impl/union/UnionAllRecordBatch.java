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
package org.apache.drill.exec.physical.impl.union;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

public class UnionAllRecordBatch extends AbstractRecordBatch<UnionAll> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionAllRecordBatch.class);

  private List<MaterializedField> outputFields;
  private UnionAller unionall;
  private final UnionAllInput unionAllInput;
  private RecordBatch current;

  private final List<TransferPair> transfers = Lists.newArrayList();
  private List<ValueVector> allocationVectors;
  protected final SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  private int recordCount = 0;
  private boolean schemaAvailable = false;

  public UnionAllRecordBatch(UnionAll config, List<RecordBatch> children, FragmentContext context)
      throws OutOfMemoryException {
    super(config, context, false);
    assert children.size() == 2 : "The number of the operands of Union must be 2";
    unionAllInput = new UnionAllInput(this, children.get(0), children.get(1));
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public void kill(boolean sendUpstream) {
    if(current != null) {
      current.kill(sendUpstream);
      current = null;
    }
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    unionAllInput.getLeftRecordBatch().kill(sendUpstream);
    unionAllInput.getRightRecordBatch().kill(sendUpstream);
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException("UnionAllRecordBatch does not support selection vector");
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException("UnionAllRecordBatch does not support selection vector");
  }

  @Override
  public IterOutcome innerNext() {
    try {
      IterOutcome upstream = unionAllInput.nextBatch();
      logger.debug("Upstream of Union-All: ", upstream.toString());
      switch(upstream) {
        case NONE:
        case OUT_OF_MEMORY:
        case STOP:
          return upstream;

        case OK_NEW_SCHEMA:
          outputFields = unionAllInput.getOutputFields();
        case OK:
          IterOutcome workOutcome = doWork();

          if(workOutcome != IterOutcome.OK) {
            return workOutcome;
          } else {
            return upstream;
          }
        default:
          throw new IllegalStateException(String.format("Unknown state %s.", upstream));
      }
    } catch (ClassTransformationException | IOException | SchemaChangeException ex) {
      context.fail(ex);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  private void setValueCount(int count) {
    for (final ValueVector v : allocationVectors) {
      final ValueVector.Mutator m = v.getMutator();
      m.setValueCount(count);
    }
  }

  private boolean doAlloc() {
    for (final ValueVector v : allocationVectors) {
      try {
        AllocationHelper.allocateNew(v, current.getRecordCount());
      } catch (OutOfMemoryRuntimeException ex) {
        return false;
      }
    }
    return true;
  }

  private IterOutcome doWork() throws ClassTransformationException, IOException, SchemaChangeException {
    if (allocationVectors != null) {
      for (final ValueVector v : allocationVectors) {
        v.clear();
      }
    }

    allocationVectors = Lists.newArrayList();
    transfers.clear();

    final ClassGenerator<UnionAller> cg = CodeGenerator.getRoot(UnionAller.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    int index = 0;
    for(VectorWrapper<?> vw : current) {
      final ValueVector vvIn = vw.getValueVector();
      // get the original input column names
      final SchemaPath inputPath = vvIn.getField().getPath();
      // get the renamed column names
      final SchemaPath outputPath = outputFields.get(index).getPath();

      final ErrorCollector collector = new ErrorCollectorImpl();
      // According to input data names, Minortypes, Datamodes, choose to
      // transfer directly,
      // rename columns or
      // cast data types (Minortype or DataMode)
      if(hasSameTypeAndMode(outputFields.get(index), vw.getValueVector().getField())) {
        // Transfer column
        if(outputFields.get(index).getPath().equals(inputPath)) {
          final LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath, current, collector, context.getFunctionRegistry());
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }

          final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
          final ValueVector vvOut = container.addOrGet(MaterializedField.create(outputPath, vectorRead.getMajorType()));
          final TransferPair tp = vvIn.makeTransferPair(vvOut);
          transfers.add(tp);
        // Copy data in order to rename the column
        } else {
          final LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath, current, collector, context.getFunctionRegistry() );
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }

          final MaterializedField outputField = MaterializedField.create(outputPath, expr.getMajorType());
          final ValueVector vv = container.addOrGet(outputField, callBack);
          allocationVectors.add(vv);
          final TypedFieldId fid = container.getValueVectorId(outputField.getPath());
          final ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
          cg.addExpr(write);
        }
      // Cast is necessary
      } else {
        LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath, current, collector, context.getFunctionRegistry());
        if (collector.hasErrors()) {
          throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
        }

        // If the inputs' DataMode is required and the outputs' DataMode is not required
        // cast to the one with the least restriction
        if(vvIn.getField().getType().getMode() == DataMode.REQUIRED
            && outputFields.get(index).getType().getMode() != DataMode.REQUIRED) {
          expr = ExpressionTreeMaterializer.convertToNullableType(expr, vvIn.getField().getType().getMinorType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        // If two inputs' MinorTypes are different,
        // Insert a cast before the Union operation
        if(vvIn.getField().getType().getMinorType() != outputFields.get(index).getType().getMinorType()) {
          expr = ExpressionTreeMaterializer.addCastExpression(expr, outputFields.get(index).getType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        final MaterializedField outputField = MaterializedField.create(outputPath, expr.getMajorType());
        final ValueVector vector = container.addOrGet(outputField, callBack);
        allocationVectors.add(vector);
        final TypedFieldId fid = container.getValueVectorId(outputField.getPath());

        final boolean useSetSafe = !(vector instanceof FixedWidthVector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        cg.addExpr(write);
      }
      ++index;
    }

    unionall = context.getImplementationClass(cg.getCodeGenerator());
    unionall.setup(context, current, this, transfers);

    if(!schemaAvailable) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      schemaAvailable = true;
    }

    if(!doAlloc()) {
      return IterOutcome.OUT_OF_MEMORY;
    }

    recordCount = unionall.unionRecords(0, current.getRecordCount(), 0);
    setValueCount(recordCount);
    return IterOutcome.OK;
  }

  public static boolean hasSameTypeAndMode(MaterializedField leftField, MaterializedField rightField) {
    return (leftField.getType().getMinorType() == rightField.getType().getMinorType())
        && (leftField.getType().getMode() == rightField.getType().getMode());
  }

  // This method is used by inner class to point the reference `current` to the correct record batch
  private void setCurrentRecordBatch(RecordBatch target) {
    this.current = target;
  }

  // This method is used by inner class to clear the current record batch
  private void clearCurrentRecordBatch() {
    if (current != null) {
      for(final VectorWrapper<?> v: current) {
        v.clear();
      }
    }
  }

  public static class UnionAllInput implements AutoCloseable {
    private UnionAllRecordBatch unionAllRecordBatch;
    private List<MaterializedField> outputFields;
    private OneSideInput leftSide;
    private OneSideInput rightSide;
    private IterOutcome upstream = IterOutcome.NOT_YET;
    private boolean leftIsFinish = false;
    private boolean rightIsFinish = false;

    // These two schemas are obtained from the first record batches of the left and right inputs
    // They are used to check if the schema is changed between recordbatches
    private BatchSchema leftSchema;
    private BatchSchema rightSchema;

    public UnionAllInput(UnionAllRecordBatch unionAllRecordBatch, RecordBatch left, RecordBatch right) {
      this.unionAllRecordBatch = unionAllRecordBatch;
      leftSide = new OneSideInput(left);
      rightSide = new OneSideInput(right);
    }

    @Override
    public void close() throws Exception {
    }

    public IterOutcome nextBatch() throws SchemaChangeException {
      if(upstream == RecordBatch.IterOutcome.NOT_YET) {
        IterOutcome iterLeft = leftSide.nextBatch();
        switch(iterLeft) {
          case OK_NEW_SCHEMA:
            break;

          case STOP:
          case OUT_OF_MEMORY:
            return iterLeft;

          case NONE:
            throw new SchemaChangeException("The left input of Union-All should not come from an empty data source");

          default:
            throw new IllegalStateException(String.format("Unknown state %s.", iterLeft));
        }

        IterOutcome iterRight = rightSide.nextBatch();
        switch(iterRight) {
          case OK_NEW_SCHEMA:
            // Unless there is no record batch on the left side of the inputs,
            // always start processing from the left side
            unionAllRecordBatch.setCurrentRecordBatch(leftSide.getRecordBatch());
            inferOutputFields();
            break;

          case NONE:
            // If the right input side comes from an empty data source,
            // use the left input side's schema directly
            unionAllRecordBatch.setCurrentRecordBatch(leftSide.getRecordBatch());
            inferOutputFieldsFromLeftSide();
            rightIsFinish = true;
            break;

          case STOP:
          case OUT_OF_MEMORY:
            return iterRight;

          default:
            throw new IllegalStateException(String.format("Unknown state %s.", iterRight));
        }

        upstream = IterOutcome.OK_NEW_SCHEMA;
        return upstream;
      } else {
        unionAllRecordBatch.clearCurrentRecordBatch();

        if(leftIsFinish && rightIsFinish) {
          upstream = IterOutcome.NONE;
          return upstream;
        } else if(leftIsFinish) {
          IterOutcome iterOutcome = rightSide.nextBatch();

          switch(iterOutcome) {
            case NONE:
              rightIsFinish = true;
              // fall through
            case STOP:
            case OUT_OF_MEMORY:
              upstream = iterOutcome;
              return upstream;

            case OK_NEW_SCHEMA:
              if(!rightSide.getRecordBatch().getSchema().equals(rightSchema)) {
                throw new SchemaChangeException("Schema change detected in the right input of Union-All. This is not currently supported");
              }
              iterOutcome = IterOutcome.OK;
              // fall through
            case OK:
              unionAllRecordBatch.setCurrentRecordBatch(rightSide.getRecordBatch());
              upstream = iterOutcome;
              return upstream;

            default:
              throw new IllegalStateException(String.format("Unknown state %s.", upstream));
          }
        } else if(rightIsFinish) {
          IterOutcome iterOutcome = leftSide.nextBatch();
          switch(iterOutcome) {
            case STOP:
            case OUT_OF_MEMORY:
            case NONE:
              upstream = iterOutcome;
              return upstream;

            case OK:
              unionAllRecordBatch.setCurrentRecordBatch(leftSide.getRecordBatch());
              upstream = iterOutcome;
              return upstream;

            default:
              throw new SchemaChangeException("Schema change detected in the left input of Union-All. This is not currently supported");
          }
        } else {
          IterOutcome iterOutcome = leftSide.nextBatch();

          switch(iterOutcome) {
            case STOP:
            case OUT_OF_MEMORY:
              upstream = iterOutcome;
              return upstream;

            case OK_NEW_SCHEMA:
              if(!leftSide.getRecordBatch().getSchema().equals(leftSchema)) {
                throw new SchemaChangeException("Schema change detected in the left input of Union-All. This is not currently supported");
              }

              iterOutcome = IterOutcome.OK;
              // fall through
            case OK:
              unionAllRecordBatch.setCurrentRecordBatch(leftSide.getRecordBatch());
              upstream = iterOutcome;
              return upstream;

            case NONE:
              unionAllRecordBatch.setCurrentRecordBatch(rightSide.getRecordBatch());
              upstream = IterOutcome.OK;
              leftIsFinish = true;
              return upstream;

            default:
              throw new IllegalStateException(String.format("Unknown state %s.", upstream));
          }
        }
      }
    }

    // The output table's column names always follow the left table,
    // where the output type is chosen based on DRILL's implicit casting rules
    private void inferOutputFields() {
      outputFields = Lists.newArrayList();
      leftSchema = leftSide.getRecordBatch().getSchema();
      rightSchema = rightSide.getRecordBatch().getSchema();
      Iterator<MaterializedField> leftIter = leftSchema.iterator();
      Iterator<MaterializedField> rightIter = rightSchema.iterator();

      int index = 1;
      while(leftIter.hasNext() && rightIter.hasNext()) {
        MaterializedField leftField  = leftIter.next();
        MaterializedField rightField = rightIter.next();

        if(hasSameTypeAndMode(leftField, rightField)) {
          outputFields.add(MaterializedField.create(leftField.getPath(), leftField.getType()));
        } else {
          // If the output type is not the same,
          // cast the column of one of the table to a data type which is the Least Restrictive
          MinorType outputMinorType;
          if(leftField.getType().getMinorType() == rightField.getType().getMinorType()) {
            outputMinorType = leftField.getType().getMinorType();
          } else {
            List<MinorType> types = Lists.newLinkedList();
            types.add(leftField.getType().getMinorType());
            types.add(rightField.getType().getMinorType());
            outputMinorType = TypeCastRules.getLeastRestrictiveType(types);
            if(outputMinorType == null) {
              throw new DrillRuntimeException("Type mismatch between " + leftField.getType().getMinorType().toString() +
                  " on the left side and " + rightField.getType().getMinorType().toString() +
                  " on the right side in column " + index + " of UNION ALL");
            }
          }

          // The output data mode should be as flexible as the more flexible one from the two input tables
          List<DataMode> dataModes = Lists.newLinkedList();
          dataModes.add(leftField.getType().getMode());
          dataModes.add(rightField.getType().getMode());
          DataMode dataMode = TypeCastRules.getLeastRestrictiveDataMode(dataModes);

          MajorType.Builder builder = MajorType.newBuilder();
          builder.setMinorType(outputMinorType);
          builder.setMode(dataMode);
          outputFields.add(MaterializedField.create(leftField.getPath(), builder.build()));
        }
        ++index;
      }

      assert !leftIter.hasNext() && ! rightIter.hasNext() : "Mis-match of column count should have been detected when validating sqlNode at planning";
    }

    private void inferOutputFieldsFromLeftSide() {
      outputFields = Lists.newArrayList();
      Iterator<MaterializedField> iter = leftSide.getRecordBatch().getSchema().iterator();
      while(iter.hasNext()) {
        MaterializedField field = iter.next();
        outputFields.add(MaterializedField.create(field.getPath(), field.getType()));
      }
    }

    public List<MaterializedField> getOutputFields() {
      if(outputFields == null) {
        throw new NullPointerException("Output fields have not been inferred");
      }

      return outputFields;
    }

    public void killIncoming(boolean sendUpstream) {
      leftSide.getRecordBatch().kill(sendUpstream);
      rightSide.getRecordBatch().kill(sendUpstream);
    }

    public RecordBatch getLeftRecordBatch() {
      return leftSide.getRecordBatch();
    }

    public RecordBatch getRightRecordBatch() {
      return rightSide.getRecordBatch();
    }

    private class OneSideInput {
      private IterOutcome upstream = IterOutcome.NOT_YET;
      private final RecordBatch recordBatch;

      public OneSideInput(RecordBatch recordBatch) {
        this.recordBatch = recordBatch;
      }

      public RecordBatch getRecordBatch() {
        return recordBatch;
      }

      public IterOutcome nextBatch() {
        if(upstream == IterOutcome.NONE) {
          throw new IllegalStateException(String.format("Unknown state %s.", upstream));
        }

        if(upstream == IterOutcome.NOT_YET) {
          upstream = unionAllRecordBatch.next(recordBatch);

          return upstream;
        } else {
          do {
            upstream = unionAllRecordBatch.next(recordBatch);
          } while (upstream == IterOutcome.OK && recordBatch.getRecordCount() == 0);

          return upstream;
        }
      }
    }
  }
}
