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
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
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
  private UnionAllInput unionAllInput;
  private RecordBatch current;

  private final List<TransferPair> transfers = Lists.newArrayList();
  private List<ValueVector> allocationVectors;
  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  private int recordCount = 0;
  private boolean schemaAvailable = false;

  public UnionAllRecordBatch(UnionAll config, List<RecordBatch> children, FragmentContext context) throws OutOfMemoryException {
    super(config, context, false);
    assert (children.size() == 2) : "The number of the operands of Union must be 2";
    unionAllInput = new UnionAllInput(this, children.get(0), children.get(1));
  }

  @Override
  public int getRecordCount() {
    return recordCount;
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
      logger.debug("Upstream of Union-All: {}", upstream);
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
    for (ValueVector v : allocationVectors) {
      ValueVector.Mutator m = v.getMutator();
      m.setValueCount(count);
    }
  }

  private boolean doAlloc() {
    for (ValueVector v : allocationVectors) {
      try {
        AllocationHelper.allocateNew(v, current.getRecordCount());
      } catch (OutOfMemoryException ex) {
        return false;
      }
    }
    return true;
  }

  private IterOutcome doWork() throws ClassTransformationException, IOException, SchemaChangeException {
    if (allocationVectors != null) {
      for (ValueVector v : allocationVectors) {
        v.clear();
      }
    }

    allocationVectors = Lists.newArrayList();
    transfers.clear();

    // If both sides of Union-All are empty
    if(unionAllInput.isBothSideEmpty()) {
      for(int i = 0; i < outputFields.size(); ++i) {
        final String colName = outputFields.get(i).getPath();
        final MajorType majorType = MajorType.newBuilder()
            .setMinorType(MinorType.INT)
            .setMode(DataMode.OPTIONAL)
            .build();

        MaterializedField outputField = MaterializedField.create(colName, majorType);
        ValueVector vv = container.addOrGet(outputField, callBack);
        allocationVectors.add(vv);
      }

      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      return IterOutcome.OK_NEW_SCHEMA;
    }


    final ClassGenerator<UnionAller> cg = CodeGenerator.getRoot(UnionAller.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    int index = 0;
    for(VectorWrapper<?> vw : current) {
      ValueVector vvIn = vw.getValueVector();
      // get the original input column names
      SchemaPath inputPath = SchemaPath.getSimplePath(vvIn.getField().getPath());
      // get the renamed column names
      SchemaPath outputPath = SchemaPath.getSimplePath(outputFields.get(index).getPath());

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

          ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
          ValueVector vvOut = container.addOrGet(MaterializedField.create(outputPath.getAsUnescapedPath(), vectorRead.getMajorType()));
          TransferPair tp = vvIn.makeTransferPair(vvOut);
          transfers.add(tp);
        // Copy data in order to rename the column
        } else {
          final LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath, current, collector, context.getFunctionRegistry() );
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }

          MaterializedField outputField = MaterializedField.create(outputPath.getAsUnescapedPath(), expr.getMajorType());
          ValueVector vv = container.addOrGet(outputField, callBack);
          allocationVectors.add(vv);
          TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getPath()));
          ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
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

        final MaterializedField outputField = MaterializedField.create(outputPath.getAsUnescapedPath(), expr.getMajorType());
        ValueVector vector = container.addOrGet(outputField, callBack);
        allocationVectors.add(vector);
        TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getPath()));

        boolean useSetSafe = !(vector instanceof FixedWidthVector);
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
    for(VectorWrapper<?> v: current) {
      v.clear();
    }
  }

  public static class UnionAllInput {
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
    private boolean bothEmpty = false;

    public UnionAllInput(UnionAllRecordBatch unionAllRecordBatch, RecordBatch left, RecordBatch right) {
      this.unionAllRecordBatch = unionAllRecordBatch;
      leftSide = new OneSideInput(left);
      rightSide = new OneSideInput(right);
    }

    private void setBothSideEmpty(boolean bothEmpty) {
      this.bothEmpty = bothEmpty;
    }

    private boolean isBothSideEmpty() {
      return bothEmpty;
    }

    public IterOutcome nextBatch() throws SchemaChangeException {
      if(upstream == RecordBatch.IterOutcome.NOT_YET) {
        IterOutcome iterLeft = leftSide.nextBatch();
        switch(iterLeft) {
          case OK_NEW_SCHEMA:
            /*
             * If the first few record batches are all empty,
             * there is no way to tell whether these empty batches are coming from empty files.
             * It is incorrect to infer output types when either side could be coming from empty.
             *
             * Thus, while-loop is necessary to skip those empty batches.
             */
            whileLoop:
            while(leftSide.getRecordBatch().getRecordCount() == 0) {
              iterLeft = leftSide.nextBatch();

              switch(iterLeft) {
                case STOP:
                case OUT_OF_MEMORY:
                  return iterLeft;

                case NONE:
                  // Special Case: The left side was an empty input.
                  leftIsFinish = true;
                  break whileLoop;

                case NOT_YET:
                case OK_NEW_SCHEMA:
                case OK:
                  continue whileLoop;

                default:
                  throw new IllegalStateException(
                      String.format("Unexpected state %s.", iterLeft));
              }
            }

            break;
          case STOP:
          case OUT_OF_MEMORY:
            return iterLeft;

          default:
            throw new IllegalStateException(
                String.format("Unexpected state %s.", iterLeft));
        }

        IterOutcome iterRight = rightSide.nextBatch();
        switch(iterRight) {
          case OK_NEW_SCHEMA:
            // Unless there is no record batch on the left side of the inputs,
            // always start processing from the left side.
            if(leftIsFinish) {
              unionAllRecordBatch.setCurrentRecordBatch(rightSide.getRecordBatch());
            } else {
              unionAllRecordBatch.setCurrentRecordBatch(leftSide.getRecordBatch());
            }
            // If the record count of the first batch from right input is zero,
            // there are two possibilities:
            // 1. The right side is an empty input (e.g., file).
            // 2. There will be more records carried by later batches.

            /*
             * If the first few record batches are all empty,
             * there is no way to tell whether these empty batches are coming from empty files.
             * It is incorrect to infer output types when either side could be coming from empty.
             *
             * Thus, while-loop is necessary to skip those empty batches.
             */
            whileLoop:
            while(rightSide.getRecordBatch().getRecordCount() == 0) {
              iterRight = rightSide.nextBatch();
              switch(iterRight) {
                case STOP:
                case OUT_OF_MEMORY:
                  return iterRight;

                case NONE:
                  // Special Case: The right side was an empty input.
                  rightIsFinish = true;
                  break whileLoop;

                case NOT_YET:
                case OK_NEW_SCHEMA:
                case OK:
                  continue whileLoop;

                default:
                  throw new IllegalStateException(
                      String.format("Unexpected state %s.", iterRight));
              }
            }

            if(leftIsFinish && rightIsFinish) {
              setBothSideEmpty(true);
            }

            inferOutputFields();
            break;

          case STOP:
          case OUT_OF_MEMORY:
            return iterRight;

          default:
            throw new IllegalStateException(
                String.format("Unexpected state %s.", iterRight));
        }



        upstream = IterOutcome.OK_NEW_SCHEMA;
        return upstream;
      } else {
        if(isBothSideEmpty()) {
          return IterOutcome.NONE;
        }

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
              throw new IllegalStateException(String.format("Unknown state %s.", iterOutcome));
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

    /**
     *
     * Summarize the inference in the four different situations:
     * First of all, the field names are always determined by the left side
     * (Even when the left side is from an empty file, we have the column names.)
     *
     * Cases:
     * 1. Left: non-empty; Right: non-empty
     *      types determined by both sides with implicit casting involved
     * 2. Left: empty; Right: non-empty
     *      type from the right
     * 3. Left: non-empty; Right: empty
     *      types from the left
     * 4. Left: empty; Right: empty
     *      types are nullable integer
     */
    private void inferOutputFields() {
      if(!leftIsFinish && !rightIsFinish) {
        // Both sides are non-empty
        inferOutputFieldsBothSide();
      } else if(!rightIsFinish) {
        // Left side is non-empty
        // While use left side's column names as output column names,
        // use right side's column types as output column types.
        inferOutputFieldsFromSingleSide(
            leftSide.getRecordBatch().getSchema(),
            rightSide.getRecordBatch().getSchema());
      } else {
        // Either right side is empty or both are empty
        // Using left side's schema is sufficient
        inferOutputFieldsFromSingleSide(
            leftSide.getRecordBatch().getSchema(),
            leftSide.getRecordBatch().getSchema());
      }
    }

    // The output table's column names always follow the left table,
    // where the output type is chosen based on DRILL's implicit casting rules
    private void inferOutputFieldsBothSide() {
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

    private void inferOutputFieldsFromSingleSide(final BatchSchema schemaForNames, final BatchSchema schemaForTypes) {
      outputFields = Lists.newArrayList();

      final List<String> outputColumnNames = Lists.newArrayList();
      final Iterator<MaterializedField> iterForNames = schemaForNames.iterator();
      while(iterForNames.hasNext()) {
        outputColumnNames.add(iterForNames.next().getPath());
      }

      final Iterator<MaterializedField> iterForTypes = schemaForTypes.iterator();
      for(int i = 0; iterForTypes.hasNext(); ++i) {
        MaterializedField field = iterForTypes.next();
        outputFields.add(MaterializedField.create(outputColumnNames.get(i), field.getType()));
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
      private RecordBatch recordBatch;

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
