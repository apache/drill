/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.statistics;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.StatisticsMerge;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.MapVector;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class StatisticsMergeBatch extends AbstractSingleRecordBatch<StatisticsMerge> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsMergeBatch.class);
  private Map<String, String> functions;
  private boolean first = true;
  private int schema = 0;
  private int recordCount = 0;
  private List<String> keyList = null;
  private Map<MaterializedField, ValueVector> dataSrcVecMap = null;
  // Map of non-map fields to VV in the incoming schema
  private Map<MaterializedField, ValueVector> copySrcVecMap = null;
  private Map<String, Map<String, ValueHolder>> aggregationMap = null;
  public StatisticsMergeBatch(StatisticsMerge popConfig, RecordBatch incoming,
                              FragmentContext context) throws OutOfMemoryException {
    super(popConfig, context, incoming);
    this.functions = new HashMap<>();
    this.aggregationMap = new HashMap<>();

    /*for (String key : popConfig.getFunctions()) {
      aggregationMap.put(key, new HashMap<String, ValueHolder>());
      if (key.equalsIgnoreCase("statcount") || key.equalsIgnoreCase("nonnullstatcount")) {
        functions.put(key, "sum");
      } else if (key.equalsIgnoreCase("hll")) {
        functions.put(key, "hll_merge");
      } else if (key.equalsIgnoreCase("sum_width")) {
        functions.put(key, "avg_width");
      }
    }*/
    for (String key : popConfig.getFunctions()) {
      if (key.equalsIgnoreCase("sum_width")) {
        functions.put(key, "avg_width");
      } else if (key.equalsIgnoreCase("hll")) {
        functions.put(key, "hll_merge");
      } else {
        functions.put(key, key);
      }
      aggregationMap.put(functions.get(key), new HashMap<String, ValueHolder>());
    }
  }

  private void createKeyColumn(String name, LogicalExpression expr, Map<MaterializedField, ValueVector> parentMap)
      throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();

    LogicalExpression mle = ExpressionTreeMaterializer.materialize(expr, incoming, collector,
        context.getFunctionRegistry());

    MaterializedField outputField = MaterializedField.create(name, mle.getMajorType());
    ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());
    container.add(vector);

    if (collector.hasErrors()) {
      throw new SchemaChangeException("Failure while materializing expression. "
          + collector.toErrorString());
    }
    parentMap.put(outputField, vector);
  }

  private ValueVector addMapVector(String name, MapVector parent, LogicalExpression expr) throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();

    LogicalExpression mle = ExpressionTreeMaterializer.materialize(expr, incoming, collector,
        context.getFunctionRegistry());

    Class<? extends ValueVector> vvc =
        TypeHelper.getValueVectorClass(mle.getMajorType().getMinorType(), mle.getMajorType().getMode());
    ValueVector vector = parent.addOrGet(name, mle.getMajorType(), vvc);

    if (collector.hasErrors()) {
      throw new SchemaChangeException("Failure while materializing expression. "
          + collector.toErrorString());
    }
    return vector;
  }

  /**
   * Identify the list of fields within a map which are unpivoted as columns in output
   */
  private void buildKeyList() {
    List<String> lastMapKeyList = null;
    for (VectorWrapper<?> vw : incoming) {
      if (vw.getField().getType().getMinorType() != TypeProtos.MinorType.MAP) {
        continue;
      }

      keyList = Lists.newArrayList();

      for (ValueVector vv : vw.getValueVector()) {
        keyList.add(vv.getField().getLastName());
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

  private void buildOutputContainer() throws SchemaChangeException {
    dataSrcVecMap = Maps.newHashMap();
    copySrcVecMap = Maps.newHashMap();
    MajorType mt = null;

    ErrorCollector collector = new ErrorCollectorImpl();
    GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

    calendar.setTimeInMillis(System.currentTimeMillis());
    createKeyColumn("schema", ValueExpressions.getBigInt(schema++), copySrcVecMap);
    createKeyColumn("computed", ValueExpressions.getDate(calendar), copySrcVecMap);

    for (VectorWrapper<?> vw : incoming) {
      addVectorToOutgoingContainer(vw.getField().getLastName(), vw, collector);
      /*MaterializedField ds = vw.getField();
      String field = vw.getField().getLastName();
      // Input map vector
      MapVector mapVector = (MapVector) vw.getValueVector();
      assert mapVector.getPrimitiveVectors().size() > 0;
      // Proceed to create output map vector with same name e.g. statcount etc.
      mt = mapVector.getField().getType();
      MaterializedField mf = MaterializedField.create(functions.get(field), mt);
      assert !dataSrcVecMap.containsKey(mf);
      ValueVector vector = TypeHelper.getNewVector(mf, oContext.getAllocator());
      container.add(vector);
      MapVector outputMapVector = (MapVector) vector;

      for (ValueVector vv : mapVector) {
        String fieldName = vv.getField().getLastName();
        if (!keyList.contains(fieldName)) {
          throw new UnsupportedOperationException("Unpivot data vector " +
              ds + " contains key " + fieldName + " not contained in key source!");
        }
        if (vv.getField().getType().getMinorType() == TypeProtos.MinorType.MAP) {
          throw new UnsupportedOperationException("Unpivot of nested map is not supported!");
        }
        if (field.equals("column")) {
          outputMapVector.addOrGet(fieldName, vv.getField().getType(), vv.getClass());
        } else {
          List<LogicalExpression> args = Lists.newArrayList();
          //TODO: Something else to access value of col such as emp_id?
          args.add(SchemaPath.getSimplePath(vv.getField().getPath()));
          //TODO: Put in the mapVector
          LogicalExpression call = FunctionCallFactory.createExpression(functions.get(field), args);
          //TODO: Is this sufficient to add to new Map?
          ValueVector vector1 = addMapVector(fieldName, outputMapVector, call);
          if (collector.hasErrors()) {
            throw new SchemaChangeException("Failure while materializing expression. "
                + collector.toErrorString());
          }
        }
      }
      dataSrcVecMap.put(ds, outputMapVector);*/
    }
    //Now create NDV in the outgoing container which was not avaliable in the incoming
    for (VectorWrapper<?> vw : incoming) {
      if (vw.getField().getLastName().equalsIgnoreCase("sum_width")) {//NullableFloat8 type
        addVectorToOutgoingContainer("ndv", vw, collector);
        break;
      }
    }
    container.setRecordCount(0);
    recordCount = 0;
    container.buildSchema(incoming.getSchema().getSelectionVectorMode());
  }

  private void addVectorToOutgoingContainer(String field, VectorWrapper vw, ErrorCollector collector)
     throws SchemaChangeException {
    // Input map vector
    MapVector mapVector = (MapVector) vw.getValueVector();
    MaterializedField mf;
    assert mapVector.getPrimitiveVectors().size() > 0;
    // Proceed to create output map vector with same name e.g. statcount etc.
    MajorType mt = mapVector.getField().getType();
    if (functions.get(field) != null) {
      mf = MaterializedField.create(functions.get(field), mt);
    } else {
      mf = MaterializedField.create(field, mt);
    }
    assert !dataSrcVecMap.containsKey(mf);
    ValueVector vector = TypeHelper.getNewVector(mf, oContext.getAllocator());
    container.add(vector);
    MapVector outputMapVector = (MapVector) vector;

    for (ValueVector vv : mapVector) {
      String fieldName = vv.getField().getLastName();
      if (!keyList.contains(fieldName)) {
        throw new UnsupportedOperationException("Unpivot data vector " +
                field + " contains key " + fieldName + " not contained in key source!");
      }
      if (vv.getField().getType().getMinorType() == TypeProtos.MinorType.MAP) {
        throw new UnsupportedOperationException("Unpivot of nested map is not supported!");
      }
      if (field.equals("column")) {
        outputMapVector.addOrGet(fieldName, vv.getField().getType(), vv.getClass());
      } else {
        List<LogicalExpression> args = Lists.newArrayList();
        LogicalExpression call;
        //TODO: Something else to access value of col such as emp_id?
        args.add(SchemaPath.getSimplePath(vv.getField().getPath()));
        //TODO: Put in the mapVector
        if (functions.get(field) != null) {
          call = FunctionCallFactory.createExpression(functions.get(field), args);
        } else {
          call = FunctionCallFactory.createExpression(field, args);
        }
        //TODO: Is this sufficient to add to new Map?
        ValueVector vector1 = addMapVector(fieldName, outputMapVector, call);
        if (collector.hasErrors()) {
          throw new SchemaChangeException("Failure while materializing expression. "
                  + collector.toErrorString());
        }
      }
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
  protected IterOutcome doWork() {
    int outRecordCount = incoming.getRecordCount();
    HashMap<String, Long> nonNullRows = new HashMap<>();

    for (VectorWrapper<?> vw : incoming) {
      ValueVector vv = vw.getValueVector();
      if (vv.getField().getType().getMinorType() != TypeProtos.MinorType.MAP) {
        //We only expect Maps in the incoming. STOP, if this is not the case.
        return IterOutcome.STOP;
      }
      MapVector mapVec = (MapVector) vv;
      Map<String, ValueHolder> statMap = aggregationMap.get(vv.getField().getLastName());

      for (ValueVector mapElt : mapVec) {
        if (vv.getField().getLastName().equalsIgnoreCase("column")) {
          BigIntHolder nameHolder;
          if (statMap.get(mapElt.getField().getLastName()) != null) {
            nameHolder = (BigIntHolder) statMap.get(mapElt.getField().getLastName());
          } else {
            nameHolder = new BigIntHolder();
            statMap.put(mapElt.getField().getLastName(), nameHolder);
          }
          nameHolder.value = 1;
        } else if (vv.getField().getLastName().equalsIgnoreCase("statcount")
            || vv.getField().getLastName().equalsIgnoreCase("nonnullstatcount")) {
          BigIntHolder sumHolder;
          String colName = mapElt.getField().getLastName();
          if (statMap.get(colName) != null) {
            sumHolder = (BigIntHolder) statMap.get(colName);
          } else {
            sumHolder = new BigIntHolder();
            statMap.put(colName, sumHolder);
          }
          //TODO: assert size = 1
          //TODO: logger
          Object val = mapElt.getAccessor().getObject(0);
          if (val != null) {
            sumHolder.value += (long)val;
          }
        } else if (vv.getField().getLastName().equalsIgnoreCase("sum_width")) {
          NullableFloat8Holder sumHolder;
          String colName = mapElt.getField().getLastName();
          if (statMap == null) {
            statMap = aggregationMap.get(functions.get(vv.getField().getLastName()));
          }
          if (statMap.get(colName) != null) {
            sumHolder = (NullableFloat8Holder) statMap.get(colName);
          } else {
            sumHolder = new NullableFloat8Holder();
            statMap.put(colName, sumHolder);
          }
          //TODO: assert size = 1
          //TODO: logger
          Object val = mapElt.getAccessor().getObject(0);
          if (val != null) {
            sumHolder.value += (double) val;
            sumHolder.isSet = 1;
          }
        } else if (vv.getField().getLastName().equalsIgnoreCase("hll")) {
          ObjectHolder hllHolder;
          String colName = mapElt.getField().getLastName();
          if (statMap == null) {
            statMap = aggregationMap.get(functions.get(vv.getField().getLastName()));
          }
          if (statMap.get(colName) != null) {
            hllHolder = (ObjectHolder) statMap.get(colName);
          } else {
            hllHolder = new ObjectHolder();
            hllHolder.obj = new HyperLogLog(context.getContextInformation().getHllMemoryLimit());
            statMap.put(colName, hllHolder);
          }
          NullableVarBinaryVector hllVector = (NullableVarBinaryVector) mapElt;
          try {
            if (hllVector.getAccessor().isSet(0) == 1) {
              ByteArrayInputStream bais = new ByteArrayInputStream(hllVector.getAccessor().getObject(0), 0,
                  mapElt.getBufferSize());
              HyperLogLog other = HyperLogLog.Builder.build(new DataInputStream(bais));
              ((HyperLogLog) hllHolder.obj).addAll(other);
            }
          } catch (Exception ex) {
            //TODO: Catch IOException/CardinalityMergeException
            //TODO: logger
            return IterOutcome.STOP;
          }
        }
      }
      // Add NDV value vector map using HLL map (since the NDV map is directly generated from HLL and not produced by the underlying
      // Statistics Agg)
      Map<String, ValueHolder> hllMap = aggregationMap.get("hll");
      if (hllMap != null) {
        aggregationMap.put("ndv", hllMap);
      }
    }
    return IterOutcome.OK;
  }

  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  @Override
  public IterOutcome innerNext() {
    IterOutcome outcome;
    boolean didSomeWork = false;
    try {
      outer: while (true) {
        outcome = next(incoming);
        switch (outcome) {
          case NONE:
            break outer;
          case OUT_OF_MEMORY:
          case NOT_YET:
          case STOP:
            return outcome;
          case OK_NEW_SCHEMA:
            if (first) {
              first =false;
              if (!setupNewSchema()) {
                outcome = IterOutcome.OK;
              }
              return outcome;
            }
            //fall through
          case OK:
            assert first == false : "First batch should be OK_NEW_SCHEMA";
            IterOutcome out = doWork();
            didSomeWork = true;
            if (out != IterOutcome.OK) {
              return out;
            }
            break;
          default:
            throw new UnsupportedOperationException("Unsupported upstream state " + outcome);
        }
      }
    } catch (SchemaChangeException ex) {
      kill(false);
      logger.error("Failure during query", ex);
      context.fail(ex);
      return IterOutcome.STOP;
    }

    // We can only get here if upstream is NONE i.e. no more batches. If we did some work prior to
    // exhausting all upstream, then return OK. Otherwise, return NONE.
    if (didSomeWork) {
      IterOutcome out = buildOutgoingRecordBatch();
      return out;
    } else {
      return outcome;
    }
  }

  // Prepare the outgoing container
  private IterOutcome buildOutgoingRecordBatch() {
    ErrorCollector collector = new ErrorCollectorImpl();
    int containerElts = 0;
    for (VectorWrapper<?> vw : container) {
      Map<String, ValueHolder> statMap = aggregationMap.get(vw.getField().getLastName());
      if (statMap == null
        && vw.getField().getLastName().equalsIgnoreCase("ndv")) {
        statMap = aggregationMap.get("hll_merge");
      }
      if (vw.getField().getLastName().equalsIgnoreCase("schema")) {
        BigIntVector vv = (BigIntVector) vw.getValueVector();
        vv.allocateNewSafe();
        vv.getMutator().setSafe(0, schema);
      } else if (vw.getField().getLastName().equalsIgnoreCase("computed")) {
        GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        DateVector vv = (DateVector) vw.getValueVector();
        vv.allocateNewSafe();
        vv.getMutator().setSafe(0, cal.getTimeInMillis());
      } else if (statMap != null) {
        MapVector map = (MapVector) vw.getValueVector();
        //map.allocateNewSafe();
        for (int index=0; index<statMap.keySet().size(); index++) {
          String colName = map.getVectorById(index).getField().getLastName();
          if (vw.getField().getLastName().equalsIgnoreCase("column")) {
            VarCharVector vv = (VarCharVector) map.getVectorById(index);
            vv.allocateNewSafe();
            //Set column name in ValueVector
            vv.getMutator().setSafe(0, colName.getBytes(), 0, colName.length());
            ++containerElts;
          } else if (vw.getField().getLastName().equalsIgnoreCase("statcount") ||
                  vw.getField().getLastName().equalsIgnoreCase("nonnullstatcount")) {
            BigIntHolder holder = (BigIntHolder) statMap.get(colName);
            NullableBigIntVector vv = (NullableBigIntVector) map.getVectorById(index);
            vv.allocateNewSafe();
            vv.getMutator().setSafe(0, holder);
            ++containerElts;
          } else if (vw.getField().getLastName().equalsIgnoreCase("avg_width")) {
            Map<String, ValueHolder> nonNullRowsMap = aggregationMap.get("nonnullstatcount");
            NullableFloat8Holder sumWidthHolder = (NullableFloat8Holder) statMap.get(colName);
            BigIntHolder sumNNRowsHolder = (BigIntHolder) nonNullRowsMap.get(colName);
            NullableFloat8Vector vv = (NullableFloat8Vector) map.getVectorById(index);
            vv.allocateNewSafe();
            //Set stat count(rowcount/nonnullrc/width) in ValueVector
            if (sumWidthHolder.isSet == 1 && sumNNRowsHolder.value > 0) {
              vv.getMutator().setSafe(0, (double) (sumWidthHolder.value / sumNNRowsHolder.value));
              ++containerElts;
            }
          } else if (vw.getField().getLastName().equalsIgnoreCase("hll_merge")) {
            ObjectHolder holder = (ObjectHolder) statMap.get(colName);
            NullableVarBinaryVector vv = (NullableVarBinaryVector) map.getVectorById(index);
            vv.allocateNewSafe();
            HyperLogLog hll = (HyperLogLog) holder.obj;
            try {
              vv.getMutator().setSafe(0, hll.getBytes(), 0, hll.getBytes().length);
            } catch (IOException ex) {
              kill(false);
              logger.error("Failure during query", ex);
              context.fail(ex);
              return IterOutcome.STOP;
            }
            ++containerElts;
          } else if (vw.getField().getLastName().equalsIgnoreCase("ndv")) {
            ObjectHolder holder = (ObjectHolder) statMap.get(colName);
            NullableBigIntVector vv = (NullableBigIntVector) map.getVectorById(index);
            vv.allocateNewSafe();
            HyperLogLog hll = (HyperLogLog) holder.obj;
            vv.getMutator().setSafe(0, 1, hll.cardinality());
            ++containerElts;
          }
        }
        map.getMutator().setValueCount(containerElts);
      }
    }
    ++recordCount;
    container.setRecordCount(1);
    return IterOutcome.OK;
  }
  @Override
  public int getRecordCount() {
    return recordCount;
  }
}
