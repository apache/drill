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
package org.apache.drill.exec.record;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.UnionVector;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for dealing with changing schemas
 */
public class SchemaUtil {

  /**
   * Returns the merger of schemas. The merged schema will include the union all columns. If there is a type conflict
   * between columns with the same schemapath but different types, the merged schema will contain a Union type.
   * @param schemas
   * @return
   */
  public static BatchSchema mergeSchemas(BatchSchema... schemas) {
    Map<SchemaPath,Set<MinorType>> typeSetMap = Maps.newLinkedHashMap();

    for (BatchSchema s : schemas) {
      for (MaterializedField field : s) {
        SchemaPath path = SchemaPath.getSimplePath(field.getPath());
        Set<MinorType> currentTypes = typeSetMap.get(path);
        if (currentTypes == null) {
          currentTypes = Sets.newHashSet();
          typeSetMap.put(path, currentTypes);
        }
        MinorType newType = field.getType().getMinorType();
        if (newType == MinorType.MAP || newType == MinorType.LIST) {
          throw new RuntimeException("Schema change not currently supported for schemas with complex types");
        }
        if (newType == MinorType.UNION) {
          for (MinorType subType : field.getType().getSubTypeList()) {
            currentTypes.add(subType);
          }
        } else {
          currentTypes.add(newType);
        }
      }
    }

    List<MaterializedField> fields = Lists.newArrayList();

    for (SchemaPath path : typeSetMap.keySet()) {
      Set<MinorType> types = typeSetMap.get(path);
      if (types.size() > 1) {
        MajorType.Builder builder = MajorType.newBuilder().setMinorType(MinorType.UNION).setMode(DataMode.OPTIONAL);
        for (MinorType t : types) {
          builder.addSubType(t);
        }
        MaterializedField field = MaterializedField.create(path.getAsUnescapedPath(), builder.build());
        fields.add(field);
      } else {
        MaterializedField field = MaterializedField.create(path.getAsUnescapedPath(), Types.optional(types.iterator().next()));
        fields.add(field);
      }
    }

    SchemaBuilder schemaBuilder = new SchemaBuilder();
    BatchSchema s = schemaBuilder.addFields(fields).setSelectionVectorMode(schemas[0].getSelectionVectorMode()).build();
    return s;
  }

  private static  ValueVector coerceVector(ValueVector v, VectorContainer c, MaterializedField field,
                                           int recordCount, OperatorContext context) {
    if (v != null) {
      int valueCount = v.getAccessor().getValueCount();
      TransferPair tp = v.getTransferPair(context.getAllocator());
      tp.transfer();
      if (v.getField().getType().getMinorType().equals(field.getType().getMinorType())) {
        if (field.getType().getMinorType() == MinorType.UNION) {
          UnionVector u = (UnionVector) tp.getTo();
          for (MinorType t : field.getType().getSubTypeList()) {
            if (u.getField().getType().getSubTypeList().contains(t)) {
              continue;
            }
            u.addSubType(t);
          }
        }
        return tp.getTo();
      } else {
        ValueVector newVector = TypeHelper.getNewVector(field, context.getAllocator());
        Preconditions.checkState(field.getType().getMinorType() == MinorType.UNION, "Can only convert vector to Union vector");
        UnionVector u = (UnionVector) newVector;
        final ValueVector vv = u.addVector(tp.getTo());
        MinorType type = v.getField().getType().getMinorType();
        for (int i = 0; i < valueCount; i++) {
          if (!vv.getAccessor().isNull(i)) {
            u.getMutator().setType(i, type);
          } else {
            u.getMutator().setType(i, MinorType.LATE);
          }
        }
        for (MinorType t : field.getType().getSubTypeList()) {
          if (u.getField().getType().getSubTypeList().contains(t)) {
            continue;
          }
          u.addSubType(t);
        }
        u.getMutator().setValueCount(valueCount);
        return u;
      }
    } else {
      v = TypeHelper.getNewVector(field, context.getAllocator());
      v.allocateNew();
      v.getMutator().setValueCount(recordCount);
      return v;
    }
  }

  /**
   * Creates a copy a record batch, converting any fields as necessary to coerce it into the provided schema
   * @param in
   * @param toSchema
   * @param context
   * @return
   */
  public static VectorContainer coerceContainer(VectorAccessible in, BatchSchema toSchema, OperatorContext context) {
    int recordCount = in.getRecordCount();
    boolean isHyper = false;
    Map<String, Object> vectorMap = Maps.newHashMap();
    for (VectorWrapper w : in) {
      if (w.isHyper()) {
        isHyper = true;
        final ValueVector[] vvs = w.getValueVectors();
        vectorMap.put(vvs[0].getField().getPath(), vvs);
      } else {
        assert !isHyper;
        final ValueVector v = w.getValueVector();
        vectorMap.put(v.getField().getPath(), v);
      }
    }

    VectorContainer c = new VectorContainer(context);

    for (MaterializedField field : toSchema) {
      if (isHyper) {
        final ValueVector[] vvs = (ValueVector[]) vectorMap.remove(field.getPath());
        final ValueVector[] vvsOut;
        if (vvs == null) {
          vvsOut = new ValueVector[1];
          vvsOut[0] = coerceVector(null, c, field, recordCount, context);
        } else {
          vvsOut = new ValueVector[vvs.length];
          for (int i = 0; i < vvs.length; ++i) {
            vvsOut[i] = coerceVector(vvs[i], c, field, recordCount, context);
          }
        }
        c.add(vvsOut);
      } else {
        final ValueVector v = (ValueVector) vectorMap.remove(field.getPath());
        c.add(coerceVector(v, c, field, recordCount, context));
      }
    }
    c.buildSchema(in.getSchema().getSelectionVectorMode());
    c.setRecordCount(recordCount);
    Preconditions.checkState(vectorMap.size() == 0, "Leftover vector from incoming batch");
    return c;
  }
}
