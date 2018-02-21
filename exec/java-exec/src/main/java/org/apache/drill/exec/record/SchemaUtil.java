/*
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.UnionVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
        SchemaPath path = SchemaPath.getSimplePath(field.getName());
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
          currentTypes.addAll(field.getType().getSubTypeList());
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
        MaterializedField field = MaterializedField.create(path.getLastSegment().getNameSegment().getPath(), builder.build());
        fields.add(field);
      } else {
        MaterializedField field = MaterializedField.create(path.getLastSegment().getNameSegment().getPath(),
                                                            Types.optional(types.iterator().next()));
        fields.add(field);
      }
    }

    SchemaBuilder schemaBuilder = new SchemaBuilder();
    BatchSchema s = schemaBuilder.addFields(fields).setSelectionVectorMode(schemas[0].getSelectionVectorMode()).build();
    return s;
  }

  @SuppressWarnings("resource")
  private static  ValueVector coerceVector(ValueVector v, VectorContainer c, MaterializedField field,
                                           int recordCount, BufferAllocator allocator) {
    if (v != null) {
      int valueCount = v.getAccessor().getValueCount();
      TransferPair tp = v.getTransferPair(allocator);
      tp.transfer();
      if (v.getField().getType().getMinorType().equals(field.getType().getMinorType())) {
        if (field.getType().getMinorType() == MinorType.UNION) {
          UnionVector u = (UnionVector) tp.getTo();
          for (MinorType t : field.getType().getSubTypeList()) {
            u.addSubType(t);
          }
        }
        return tp.getTo();
      } else {
        ValueVector newVector = TypeHelper.getNewVector(field, allocator);
        Preconditions.checkState(field.getType().getMinorType() == MinorType.UNION, "Can only convert vector to Union vector");
        UnionVector u = (UnionVector) newVector;
        u.setFirstType(tp.getTo(), valueCount);
        return u;
      }
    } else {
      v = TypeHelper.getNewVector(field, allocator);
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
    return coerceContainer(in, toSchema, context.getAllocator());
  }

  public static VectorContainer coerceContainer(VectorAccessible in, BatchSchema toSchema, BufferAllocator allocator) {
    int recordCount = in.getRecordCount();
    boolean isHyper = false;
    Map<String, Object> vectorMap = Maps.newHashMap();
    for (VectorWrapper<?> w : in) {
      if (w.isHyper()) {
        isHyper = true;
        final ValueVector[] vvs = w.getValueVectors();
        vectorMap.put(vvs[0].getField().getName(), vvs);
      } else {
        assert !isHyper;
        @SuppressWarnings("resource")
        final ValueVector v = w.getValueVector();
        vectorMap.put(v.getField().getName(), v);
      }
    }

    VectorContainer c = new VectorContainer(allocator);

    for (MaterializedField field : toSchema) {
      if (isHyper) {
        final ValueVector[] vvs = (ValueVector[]) vectorMap.remove(field.getName());
        final ValueVector[] vvsOut;
        if (vvs == null) {
          vvsOut = new ValueVector[1];
          vvsOut[0] = coerceVector(null, c, field, recordCount, allocator);
        } else {
          vvsOut = new ValueVector[vvs.length];
          for (int i = 0; i < vvs.length; ++i) {
            vvsOut[i] = coerceVector(vvs[i], c, field, recordCount, allocator);
          }
        }
        c.add(vvsOut);
      } else {
        @SuppressWarnings("resource")
        final ValueVector v = (ValueVector) vectorMap.remove(field.getName());
        c.add(coerceVector(v, c, field, recordCount, allocator));
      }
    }
    c.buildSchema(in.getSchema().getSelectionVectorMode());
    c.setRecordCount(recordCount);
    Preconditions.checkState(vectorMap.size() == 0, "Leftover vector from incoming batch");
    return c;
  }

  /**
   * Check if two schemas are the same. The schemas, given as lists, represent the
   * children of the original and new maps (AKA structures.)
   *
   * @param currentChildren current children of a Drill map
   * @param newChildren new children, in an incoming batch, of the same
   * Drill map
   * @return true if the schemas are identical, false if a child is missing
   * or has changed type or cardinality (AKA "mode").
   */
  public static boolean isSameSchema(Collection<MaterializedField> currentChildren,
      List<SerializedField> newChildren) {
    if (currentChildren.size() != newChildren.size()) {
      return false;
    }

    // Column order can permute (see DRILL-5828). So, use a map
    // for matching.

    Map<String, MaterializedField> childMap = CaseInsensitiveMap.newHashMap();
    for (MaterializedField currentChild : currentChildren) {
      childMap.put(currentChild.getName(), currentChild);
    }
    for (SerializedField newChild : newChildren) {
      MaterializedField currentChild = childMap.get(newChild.getNamePart().getName());

      // New map member?

      if (currentChild == null) {
        return false;
      }

      // Changed data type?

      if (! currentChild.getType().equals(newChild.getMajorType())) {
        return false;
      }

      // Perform schema diff for child column(s)
      if (MinorType.MAP.equals(currentChild.getType().getMinorType())) {
        if (currentChild.getChildren().size() != newChild.getChildCount()) {
          return false;
        }

        if (!currentChild.getChildren().isEmpty()) {
          if (!isSameSchema(currentChild.getChildren(), newChild.getChildList())) {
            return false;
          }
        }
      }
    }

    // Everything matches.

    return true;
  }

  /**
   * Check if two schemas are the same including the order of their children. The schemas, given as lists, represent the
   * children of the original and new maps (AKA structures.)
   *
   * @param currentChildren current children of a Drill map
   * @param newChildren new children, in an incoming batch, of the same
   * Drill map
   * @return true if the schemas are identical, false if a child is missing
   * or has changed type or cardinality (AKA "mode").
   */
  public static boolean isSameSchemaIncludingOrder(Collection<MaterializedField> currentChildren,
    Collection<MaterializedField> newChildren) {

    if (currentChildren.size() != newChildren.size()) {
      return false;
    }

    // Insert the two collections in a List data structure to implement ordering logic
    // TODO - MaterializedField should expose a LIST data structure since ordering is key for
    //        batch operators.
    List<MaterializedField> currentChildrenList = new ArrayList<MaterializedField>(currentChildren);
    List<MaterializedField> newChildrenList     = new ArrayList<MaterializedField>(newChildren);

    for (int idx = 0; idx < newChildrenList.size(); idx++) {
      MaterializedField currentChild = currentChildrenList.get(idx);
      MaterializedField newChild     = newChildrenList.get(idx);

      // Same name ?
      if (!currentChild.getName().equalsIgnoreCase(newChild.getName())) {
        return false;
      }

      // Changed data type?
      if (!currentChild.getType().equals(newChild.getType())) {
        return false;
      }

      // Perform schema diff for child column(s)
      if (MinorType.MAP.equals(currentChild.getType().getMinorType())) {
        if (currentChild.getChildren().size() != newChild.getChildren().size()) {
          return false;
        }

        if (!currentChild.getChildren().isEmpty()) {
          if (!isSameSchemaIncludingOrder(currentChild.getChildren(), newChild.getChildren())) {
            return false;
          }
        }
      }
    }

    // Everything matches.

    return true;
  }



}
