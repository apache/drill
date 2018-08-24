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

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.VarLenReadExpr;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FixedLenExpr;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * ProjectMemoryManager(PMM) is used to estimate the size of rows produced by ProjectRecordBatch.
 * The PMM works as follows:
 *
 * Setup phase: As and when ProjectRecordBatch creates or transfers a field, it registers the field with PMM.
 * If the field is a variable width field, PMM records the expression that produces the variable
 * width field. The expression is a tree of LogicalExpressions. The PMM walks this tree of LogicalExpressions
 * to produce a tree of OutputWidthExpressions. The widths of Fixed width fields are just accumulated into a single
 * total. Note: The PMM, currently, cannot handle new complex fields, it just uses a hard-coded estimate for such fields.
 *
 *
 * Execution phase: Just before a batch is processed by Project, the PMM walks the tree of OutputWidthExpressions
 * and converts them to FixedWidthExpressions. It uses the RecordBatchSizer and the function annotations to do this conversion.
 * See OutputWidthVisitor for details.
 */
public class ProjectMemoryManager extends RecordBatchMemoryManager {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectMemoryManager.class);

    public RecordBatch getIncomingBatch() {
        return incomingBatch;
    }

    RecordBatch incomingBatch = null;
    ProjectRecordBatch outgoingBatch = null;

    int rowWidth = 0;
    Map<String, ColumnWidthInfo> outputColumnSizes;
    // Number of variable width columns in the batch
    int variableWidthColumnCount = 0;
    // Number of fixed width columns in the batch
    int fixedWidthColumnCount = 0;
    // Number of complex columns in the batch
    int complexColumnsCount = 0;


    // Holds sum of all fixed width column widths
    int totalFixedWidthColumnWidth = 0;
    // Holds sum of all complex column widths
    // Currently, this is just a guess
    int totalComplexColumnWidth = 0;

    enum WidthType {
        FIXED,
        VARIABLE
    }

    enum OutputColumnType {
        TRANSFER,
        NEW
    }

    class ColumnWidthInfo {
        OutputWidthExpression outputExpression;
        int width;
        WidthType widthType;
        OutputColumnType outputColumnType;
        ValueVector outputVV; // for transfers, this is the transfer src


        ColumnWidthInfo(OutputWidthExpression outputWidthExpression,
                        OutputColumnType outputColumnType,
                        WidthType widthType,
                        int fieldWidth, ValueVector outputVV) {
            this.outputExpression = outputWidthExpression;
            this.width = fieldWidth;
            this.outputColumnType = outputColumnType;
            this.widthType = widthType;
            this.outputVV = outputVV;
        }

        public OutputWidthExpression getOutputExpression() { return outputExpression; }

        public OutputColumnType getOutputColumnType() { return outputColumnType; }

        boolean isFixedWidth() { return widthType == WidthType.FIXED; }

        public int getWidth() { return width; }

    }

    void ShouldNotReachHere() {
        throw new IllegalStateException();
    }

    private void setIncomingBatch(RecordBatch recordBatch) {
        incomingBatch = recordBatch;
    }

    private void setOutgoingBatch(ProjectRecordBatch outgoingBatch) {
        this.outgoingBatch = outgoingBatch;
    }

    public ProjectMemoryManager(int configuredOutputSize) {
        super(configuredOutputSize);
        outputColumnSizes = new HashMap<>();
    }

    public boolean isComplex(MajorType majorType) {
        MinorType minorType = majorType.getMinorType();
        return minorType == MinorType.MAP || minorType == MinorType.UNION || minorType == MinorType.LIST;
    }

    boolean isFixedWidth(TypedFieldId fieldId) {
        ValueVector vv = getOutgoingValueVector(fieldId);
        return isFixedWidth(vv);
    }

    public ValueVector getOutgoingValueVector(TypedFieldId fieldId) {
        Class<?> clazz = fieldId.getIntermediateClass();
        int[] fieldIds = fieldId.getFieldIds();
        return outgoingBatch.getValueAccessorById(clazz, fieldIds).getValueVector();
    }

    static boolean isFixedWidth(ValueVector vv) {  return (vv instanceof FixedWidthVector); }


    static int getNetWidthOfFixedWidthType(ValueVector vv) {
        assert isFixedWidth(vv);
        return ((FixedWidthVector)vv).getValueWidth();
    }

    public static int getDataWidthOfFixedWidthType(TypeProtos.MajorType majorType) {
        MinorType minorType = majorType.getMinorType();
        final boolean isVariableWidth  = (minorType == MinorType.VARCHAR || minorType == MinorType.VAR16CHAR
                || minorType == MinorType.VARBINARY);

        if (isVariableWidth) {
            throw new IllegalArgumentException("getWidthOfFixedWidthType() cannot handle variable width types");
        }

        if (minorType == MinorType.NULL) {
            return 0;
        }

        return TypeHelper.getSize(majorType);
    }


    void addTransferField(ValueVector vvIn, String inputColumnName, String outputColumnName) {
        addField(vvIn, null, OutputColumnType.TRANSFER, inputColumnName, outputColumnName);
    }

    void addNewField(ValueVector vvOut, LogicalExpression logicalExpression) {
        addField(vvOut, logicalExpression, OutputColumnType.NEW, null, vvOut.getField().getName());
    }

    void addField(ValueVector vv, LogicalExpression logicalExpression, OutputColumnType outputColumnType,
                  String inputColumnName, String outputColumnName) {
        if(isFixedWidth(vv)) {
            addFixedWidthField(vv);
        } else {
            addVariableWidthField(vv, logicalExpression, outputColumnType, inputColumnName, outputColumnName);
        }
    }

    private void addVariableWidthField(ValueVector vv, LogicalExpression logicalExpression,
                                       OutputColumnType outputColumnType, String inputColumnName, String outputColumnName) {
        variableWidthColumnCount++;
        ColumnWidthInfo columnWidthInfo;
        logger.trace("addVariableWidthField(): vv {} totalCount: {} outputColumnType: {}",
                printVV(vv), variableWidthColumnCount, outputColumnType);
        //Variable width transfers
        if(outputColumnType == OutputColumnType.TRANSFER) {
            VarLenReadExpr readExpr = new VarLenReadExpr(inputColumnName);
            columnWidthInfo = new ColumnWidthInfo(readExpr, outputColumnType,
                    WidthType.VARIABLE, -1, vv); //fieldWidth has to be obtained from the RecordBatchSizer
        } else if (isComplex(vv.getField().getType())) {
            addComplexField(vv);
            return;
        } else {
            // Walk the tree of LogicalExpressions to get a tree of OutputWidthExpressions
            OutputWidthVisitorState state = new OutputWidthVisitorState(this);
            OutputWidthExpression outputWidthExpression = logicalExpression.accept(new OutputWidthVisitor(), state);
            columnWidthInfo = new ColumnWidthInfo(outputWidthExpression, outputColumnType,
                    WidthType.VARIABLE, -1, vv); //fieldWidth has to be obtained from the OutputWidthExpression
        }
        ColumnWidthInfo existingInfo = outputColumnSizes.put(outputColumnName, columnWidthInfo);
        Preconditions.checkState(existingInfo == null);
    }

    public static String printVV(ValueVector vv) {
        String str = "null";
        if (vv != null) {
            str = vv.getField().getName() + " " + vv.getField().getType();
        }
        return str;
    }

    void addComplexField(ValueVector vv) {
        //Complex types are not yet supported. Just use a guess for the size
        assert vv == null || isComplex(vv.getField().getType());
        complexColumnsCount++;
        // just a guess
        totalComplexColumnWidth +=  OutputSizeEstimateConstants.COMPLEX_FIELD_ESTIMATE;
        logger.trace("addComplexField(): vv {} totalCount: {} totalComplexColumnWidth: {}",
                printVV(vv), complexColumnsCount, totalComplexColumnWidth);
    }

    void addFixedWidthField(ValueVector vv) {
        assert isFixedWidth(vv);
        fixedWidthColumnCount++;
        int fixedFieldWidth = getNetWidthOfFixedWidthType(vv);
        totalFixedWidthColumnWidth += fixedFieldWidth;
        logger.trace("addFixedWidthField(): vv {} totalCount: {} totalComplexColumnWidth: {}",
                printVV(vv), fixedWidthColumnCount, totalFixedWidthColumnWidth);
    }

    public void init(RecordBatch incomingBatch, ProjectRecordBatch outgoingBatch) {
        setIncomingBatch(incomingBatch);
        setOutgoingBatch(outgoingBatch);
        reset();

        RecordBatchStats.logRecordBatchStats(outgoingBatch.getRecordBatchStatsContext(),
          "configuredOutputSize: %d", getOutputBatchSize());
    }

    private void reset() {
        rowWidth = 0;
        totalFixedWidthColumnWidth = 0;
        totalComplexColumnWidth = 0;

        fixedWidthColumnCount = 0;
        complexColumnsCount = 0;
    }

    @Override
    public void update() {
        long updateStartTime = System.currentTimeMillis();
        RecordBatchSizer batchSizer = new RecordBatchSizer(incomingBatch);
        long batchSizerEndTime = System.currentTimeMillis();

        setRecordBatchSizer(batchSizer);
        rowWidth = 0;
        int totalVariableColumnWidth = 0;
        for (String outputColumnName : outputColumnSizes.keySet()) {
            ColumnWidthInfo columnWidthInfo = outputColumnSizes.get(outputColumnName);
            int width = -1;
            if (columnWidthInfo.isFixedWidth()) {
                // fixed width columns are accumulated in totalFixedWidthColumnWidth
                ShouldNotReachHere();
            } else {
                //Walk the tree of OutputWidthExpressions to get a FixedLenExpr
                //As the tree is walked, the RecordBatchSizer and function annotations
                //are looked-up to come up with the final FixedLenExpr
                OutputWidthExpression savedWidthExpr = columnWidthInfo.getOutputExpression();
                OutputWidthVisitorState state = new OutputWidthVisitorState(this);
                OutputWidthExpression reducedExpr = savedWidthExpr.accept(new OutputWidthVisitor(), state);
                width = ((FixedLenExpr)reducedExpr).getDataWidth();
                Preconditions.checkState(width >= 0);
                int metadataWidth = getMetadataWidth(columnWidthInfo.outputVV);
                logger.trace("update(): fieldName {} width: {} metadataWidth: {}",
                        columnWidthInfo.outputVV.getField().getName(), width, metadataWidth);
                width += metadataWidth;
            }
            totalVariableColumnWidth += width;
        }
        rowWidth += totalFixedWidthColumnWidth;
        rowWidth += totalComplexColumnWidth;
        rowWidth += totalVariableColumnWidth;
        int outPutRowCount;
        if (rowWidth != 0) {
            //if rowWidth is not zero, set the output row count in the sizer
            setOutputRowCount(getOutputBatchSize(), rowWidth);
            // if more rows can be allowed than the incoming row count, then set the
            // output row count to the incoming row count.
            outPutRowCount = Math.min(getOutputRowCount(), batchSizer.rowCount());
        } else {
            // if rowWidth == 0 then the memory manager does
            // not have sufficient information to size the batch
            // let the entire batch pass through.
            // If incoming rc == 0, all RB Sizer look-ups will have
            // 0 width and so total width can be 0
            outPutRowCount = incomingBatch.getRecordCount();
        }
        setOutputRowCount(outPutRowCount);
        long updateEndTime = System.currentTimeMillis();
        logger.trace("update() : Output RC {}, BatchSizer RC {}, incoming RC {}, width {}, total fixed width {}"
                    + ", total variable width {}, total complex width {}, batchSizer time {} ms, update time {}  ms"
                    + ", manager {}, incoming {}",outPutRowCount, batchSizer.rowCount(), incomingBatch.getRecordCount(),
                    rowWidth, totalFixedWidthColumnWidth, totalVariableColumnWidth, totalComplexColumnWidth,
                    (batchSizerEndTime - updateStartTime),(updateEndTime - updateStartTime), this, incomingBatch);

        RecordBatchStats.logRecordBatchStats(RecordBatchIOType.INPUT, getRecordBatchSizer(), outgoingBatch.getRecordBatchStatsContext());
        updateIncomingStats();
    }

    public static int getMetadataWidth(ValueVector vv) {
        int width = 0;
        if (vv instanceof NullableVector) {
            width += ((NullableVector)vv).getBitsVector().getPayloadByteCount(1);
        }

        if (vv instanceof VariableWidthVector) {
            width += ((VariableWidthVector)vv).getOffsetVector().getPayloadByteCount(1);
        }

        if (vv instanceof BaseRepeatedValueVector) {
            width += ((BaseRepeatedValueVector)vv).getOffsetVector().getPayloadByteCount(1);
            width += (getMetadataWidth(((BaseRepeatedValueVector)vv).getDataVector()) * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
        }
        return width;
    }
}
