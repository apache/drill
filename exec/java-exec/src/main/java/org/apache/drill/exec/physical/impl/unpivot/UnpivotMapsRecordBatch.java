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
package org.apache.drill.exec.physical.impl.unpivot;

import java.util.List;
import java.util.Map;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnpivotMaps;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

/**
 * Unpivot maps. Assumptions are:
 *  1) all child vectors in a map are of same type.
 *  2) Each map contains the same number of fields and field names are also same (types could be different).
 *
 * Example input and output:
 * Schema of input:
 *    "schema"        : BIGINT - Schema number. For each schema change this number is incremented.
 *    "computed"      : BIGINT - What time is it computed?
 *    "columns" : MAP - Column names
 *       "region_id"  : VARCHAR
 *       "sales_city" : VARCHAR
 *       "cnt"        : VARCHAR
 *    "statscount" : MAP
 *       "region_id"  : BIGINT - statscount(region_id) - aggregation over all values of region_id
 *                      in incoming batch
 *       "sales_city" : BIGINT - statscount(sales_city)
 *       "cnt"        : BIGINT - statscount(cnt)
 *    "nonnullstatcount" : MAP
 *       "region_id"  : BIGINT - nonnullstatcount(region_id)
 *       "sales_city" : BIGINT - nonnullstatcount(sales_city)
 *       "cnt"        : BIGINT - nonnullstatcount(cnt)
 *   .... another map for next stats function ....
 *
 * Schema of output:
 *  "schema"           : BIGINT - Schema number. For each schema change this number is incremented.
 *  "computed"         : BIGINT - What time is this computed?
 *  "column"           : column name
 *  "statscount"       : BIGINT
 *  "nonnullstatcount" : BIGINT
 *  .... one column for each map type ...
 */
public class UnpivotMapsRecordBatch extends AbstractSingleRecordBatch<UnpivotMaps> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnpivotMapsRecordBatch.class);

  private final List<String> mapFieldsNames;
  private boolean first = true;
  private int keyIndex = 0;
  private List<String> keyList = null;

  private Map<MaterializedField, Map<String, ValueVector>> dataSrcVecMap = null;

  // Map of non-map fields to VV in the incoming schema
  private Map<MaterializedField, ValueVector> copySrcVecMap = null;

  private List<TransferPair> transferList;
  private int recordCount = 0;

  public UnpivotMapsRecordBatch(UnpivotMaps pop, RecordBatch incoming, FragmentContext context)
      throws OutOfMemoryException {
    super(pop, context, incoming);
    this.mapFieldsNames = pop.getMapFieldNames();
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public IterOutcome innerNext() {

    IterOutcome upStream = IterOutcome.OK;

    if (keyIndex == 0) {
      upStream = next(incoming);
    }
    // Process according to upstream outcome
    switch (upStream) {
      case NONE:
      case OUT_OF_MEMORY:
      case NOT_YET:
      case STOP:
        return upStream;
      case OK_NEW_SCHEMA:
        if (first) {
          first = false;
        }
        try {
          if (!setupNewSchema()) {
            upStream = IterOutcome.OK;
          } else {
            return upStream;
          }
        } catch (SchemaChangeException ex) {
          kill(false);
          logger.error("Failure during query", ex);
          context.getExecutorState().fail(ex);
          return IterOutcome.STOP;
        }
        //fall through
      case OK:
        assert first == false : "First batch should be OK_NEW_SCHEMA";
        try {
          container.zeroVectors();
          IterOutcome out = doWork();
          // Preserve OK_NEW_SCHEMA unless doWork() runs into an issue
          if (out != IterOutcome.OK) {
            upStream = out;
          }
        } catch (Exception ex) {
          kill(false);
          logger.error("Failure during query", ex);
          context.getExecutorState().fail(ex);
          return IterOutcome.STOP;
        }
       return upStream;
      default:
        throw new UnsupportedOperationException("Unsupported upstream state " + upStream);
    }
  }

  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  private void doTransfer() {
    final int inputCount = incoming.getRecordCount();
    for (TransferPair tp : transferList) {
      tp.splitAndTransfer(0, inputCount);
    }
  }

  @Override
  protected IterOutcome doWork() {
    int outRecordCount = incoming.getRecordCount();

    prepareTransfers();
    doTransfer();

    keyIndex = (keyIndex + 1) % keyList.size();
    recordCount = outRecordCount;

    if (keyIndex == 0) {
      for (VectorWrapper w : incoming) {
        w.clear();
      }
    }
    return IterOutcome.OK;
  }

  /**
   * Identify the list of fields within a map which are unpivoted as columns in output
   */
  private void buildKeyList() {
    List<String> lastMapKeyList = null;
    for (VectorWrapper<?> vw : incoming) {
      if (vw.getField().getType().getMinorType() != MinorType.MAP) {
        continue;
      }

      keyList = Lists.newArrayList();

      for (ValueVector vv : vw.getValueVector()) {
        keyList.add(SchemaPath.getSimplePath(vv.getField().getName()).toString());
      }

      if (lastMapKeyList == null) {
        lastMapKeyList = keyList;
      } else {
        if (keyList.size() != lastMapKeyList.size() || !lastMapKeyList.containsAll(keyList)) {
          throw new UnsupportedOperationException("Maps have different fields");
        }
      }
    }
  }

  private void buildOutputContainer() {
    dataSrcVecMap = Maps.newHashMap();
    copySrcVecMap = Maps.newHashMap();
    for (VectorWrapper<?> vw : incoming) {
      MaterializedField ds = vw.getField();
      String colName = vw.getField().getName();

      if (!mapFieldsNames.contains(colName)) {
        MajorType mt = vw.getValueVector().getField().getType();
        MaterializedField mf = MaterializedField.create(colName, mt);
        container.add(TypeHelper.getNewVector(mf, oContext.getAllocator()));
        copySrcVecMap.put(mf, vw.getValueVector());
        continue;
      }

      MapVector mapVector = (MapVector) vw.getValueVector();
      assert mapVector.getPrimitiveVectors().size() > 0;

      MajorType mt = mapVector.iterator().next().getField().getType();
      MaterializedField mf = MaterializedField.create(colName, mt);
      assert !dataSrcVecMap.containsKey(mf);
      container.add(TypeHelper.getNewVector(mf, oContext.getAllocator()));

      Map<String, ValueVector> m = Maps.newHashMap();
      dataSrcVecMap.put(mf, m);

      for (ValueVector vv : mapVector) {
        String fieldName = SchemaPath.getSimplePath(vv.getField().getName()).toString();
        if (!keyList.contains(fieldName)) {
          throw new UnsupportedOperationException("Unpivot data vector " +
              ds + " contains key " + fieldName + " not contained in key source!");
        }
        if (vv.getField().getType().getMinorType() == MinorType.MAP) {
          throw new UnsupportedOperationException("Unpivot of nested map is not supported!");
        }
        m.put(fieldName, vv);
      }
    }
    container.buildSchema(incoming.getSchema().getSelectionVectorMode());
  }

  private void prepareTransfers() {
    ValueVector vv;
    TransferPair tp;
    transferList = Lists.newArrayList();

    for (VectorWrapper<?> vw : container) {
      MaterializedField mf = vw.getField();
      if (dataSrcVecMap.containsKey(mf)) {
        String k = keyList.get(keyIndex);
        vv = dataSrcVecMap.get(mf).get(k);
        tp = vv.makeTransferPair(vw.getValueVector());
      } else {
        vv = copySrcVecMap.get(mf);
        tp = vv.makeTransferPair(vw.getValueVector());
      }
      transferList.add(tp);
    }
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    container.clear();
    buildKeyList();
    buildOutputContainer();
    return true;
  }

  @Override
  public void dump() {
    logger.error("UnpivotMapsRecordbatch[recordCount={}, container={}]", recordCount, container);
  }
}
