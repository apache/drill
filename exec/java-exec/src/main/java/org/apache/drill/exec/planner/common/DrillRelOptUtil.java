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
package org.apache.drill.exec.planner.common;

import java.util.AbstractList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Pair;

/**
 * Utility class that is a subset of the RelOptUtil class and is a placeholder for Drill specific
 * static methods that are needed during either logical or physical planning.
 */
public abstract class DrillRelOptUtil {

  // Similar to RelOptUtil.areRowTypesEqual() with the additional check for allowSubstring
  public static boolean areRowTypesCompatible(
      RelDataType rowType1,
      RelDataType rowType2,
      boolean compareNames,
      boolean allowSubstring) {
    if (rowType1 == rowType2) {
      return true;
    }
    if (compareNames) {
      // if types are not identity-equal, then either the names or
      // the types must be different
      return false;
    }
    if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
      return false;
    }
    final List<RelDataTypeField> f1 = rowType1.getFieldList();
    final List<RelDataTypeField> f2 = rowType2.getFieldList();
    for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
      final RelDataType type1 = pair.left.getType();
      final RelDataType type2 = pair.right.getType();
      // If one of the types is ANY comparison should succeed
      if (type1.getSqlTypeName() == SqlTypeName.ANY
        || type2.getSqlTypeName() == SqlTypeName.ANY) {
        continue;
      }
      if (type1.getSqlTypeName() != type2.getSqlTypeName()) {
        if (allowSubstring
            && (type1.getSqlTypeName() == SqlTypeName.CHAR && type2.getSqlTypeName() == SqlTypeName.CHAR)
            && (type1.getPrecision() <= type2.getPrecision())) {
          return true;
        }

        // Check if Drill implicit casting can resolve the incompatibility
        List<TypeProtos.MinorType> types = Lists.newArrayListWithCapacity(2);
        types.add(Types.getMinorTypeFromName(type1.getSqlTypeName().getName()));
        types.add(Types.getMinorTypeFromName(type2.getSqlTypeName().getName()));
        if(TypeCastRules.getLeastRestrictiveType(types) != null) {
          return true;
        }

        return false;
      }
    }
    return true;
  }

  /**
   * Returns a relational expression which has the same fields as the
   * underlying expression, but the fields have different names.
   *
   * Note: This method is copied from {@link org.eigenbase.rel.CalcRel#createRename(RelNode, List)} because it has a bug
   * which doesn't rename the exprs. This bug is fixed in latest version of Apache Calcite (1.2).
   *
   * @param rel        Relational expression
   * @param fieldNames Field names
   * @return Renamed relational expression
   */
  public static RelNode createRename(
      RelNode rel,
      final List<String> fieldNames) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    assert fieldNames.size() == fields.size();
    final List<Pair<RexNode, String>> refs =
        new AbstractList<Pair<RexNode, String>>() {
          public int size() {
            return fields.size();
          }

          public Pair<RexNode, String> get(int index) {
            return Pair.of(
                (RexNode) new RexInputRef(index, fields.get(index).getType()),
                fieldNames.get(index));
          }
        };
    return CalcRel.createProject(rel, refs, true);
  }
}
