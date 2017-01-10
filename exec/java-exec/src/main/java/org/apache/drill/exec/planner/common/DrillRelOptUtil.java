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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.resolver.TypeCastRules;

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
    final List<RexNode> refs =
        new AbstractList<RexNode>() {
          public int size() {
            return fields.size();
          }

          public RexNode get(int index) {
            return RexInputRef.of(index, fields);
          }
        };

    return RelOptUtil.createProject(rel, refs, fieldNames, false);
  }

  public static boolean isTrivialProject(Project project, boolean useNamesInIdentityProjCalc) {
    if (!useNamesInIdentityProjCalc) {
      return ProjectRemoveRule.isTrivial(project);
    }  else {
      return containIdentity(project.getProjects(), project.getRowType(), project.getInput().getRowType());
    }
  }

  /** Returns a rowType having all unique field name.
   *
   * @param rowType : input rowType
   * @param typeFactory : type factory used to create a new row type.
   * @return
   */
  public static RelDataType uniqifyFieldName(final RelDataType rowType, final RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(RelOptUtil.getFieldTypeList(rowType),
        SqlValidatorUtil.uniquify(rowType.getFieldNames()));
  }

  /**
   * Returns whether the leading edge of a given array of expressions is
   * wholly {@link RexInputRef} objects with types and names corresponding
   * to the underlying row type. */
  private static boolean containIdentity(List<? extends RexNode> exps,
                                        RelDataType rowType, RelDataType childRowType) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<RelDataTypeField> childFields = childRowType.getFieldList();
    int fieldCount = childFields.size();
    if (exps.size() != fieldCount) {
      return false;
    }
    for (int i = 0; i < exps.size(); i++) {
      RexNode exp = exps.get(i);
      if (!(exp instanceof RexInputRef)) {
        return false;
      }
      RexInputRef var = (RexInputRef) exp;
      if (var.getIndex() != i) {
        return false;
      }
      if (!fields.get(i).getName().equals(childFields.get(i).getName())) {
        return false;
      }
      if (!fields.get(i).getType().equals(childFields.get(i).getType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether statistics-based estimates or guesses are used by the optimizer
   * */
  public static boolean guessRows(RelNode rel) {
    final PlannerSettings settings =
        rel.getCluster().getPlanner().getContext().unwrap(PlannerSettings.class);
    if (!settings.useStatistics()) {
      return true;
    }
    if (rel instanceof RelSubset) {
      if (((RelSubset) rel).getBest() != null) {
        return guessRows(((RelSubset) rel).getBest());
      } else if (((RelSubset) rel).getOriginal() != null) {
        return guessRows(((RelSubset) rel).getOriginal());
      }
    } else if (rel instanceof HepRelVertex) {
      if (((HepRelVertex) rel).getCurrentRel() != null) {
        return guessRows(((HepRelVertex) rel).getCurrentRel());
      }
    } else if (rel instanceof TableScan) {
      DrillTable table = rel.getTable().unwrap(DrillTable.class);
      if (table == null) {
        table = rel.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
      }
      if (table != null && table.getStatsTable() != null) {
        return false;
      } else {
        return true;
      }
    } else {
      for (RelNode child : rel.getInputs()) {
        if (guessRows(child)) { // at least one child is a guess
          return true;
        }
      }
    }
    return false;
  }

  public static boolean findComplexPredicate(RelNode rel) {
    if (rel instanceof RelSubset) {
      if (((RelSubset) rel).getBest() != null) {
        return findComplexPredicate(((RelSubset) rel).getBest());
      } else if (((RelSubset) rel).getOriginal() != null) {
        return findComplexPredicate(((RelSubset) rel).getOriginal());
      }
    } else if (rel instanceof HepRelVertex) {
      if (((HepRelVertex) rel).getCurrentRel() != null) {
        return findComplexPredicate(((HepRelVertex) rel).getCurrentRel());
      }
    } else if (rel instanceof Filter) {
      if (findLikeOrRangePredicate(((Filter) rel).getCondition())) {
        return true;
      } else {
        return findComplexPredicate(((Filter) rel).getInput());
      }
    } else if (rel instanceof TableScan) {
      return false;
    } else {
      for (RelNode child : rel.getInputs()) {
        if (findComplexPredicate(child)) { // at least one child is a guess
          return true;
        }
      }
    }
    return false;
  }

  private static boolean findLikeOrRangePredicate(RexNode predicate) {
    if ((predicate == null) || predicate.isAlwaysTrue()) {
      return false;
    }
    for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
      for (RexNode orPred : RelOptUtil.disjunctions(pred)) {
        if (!orPred.isA(SqlKind.EQUALS) ||
             orPred.isA(SqlKind.LIKE)) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean analyzeSimpleEquiJoin(Join join, int[] joinFieldOrdinals) {
    RexNode joinExp = join.getCondition();
    if(joinExp.getKind() != SqlKind.EQUALS) {
      return false;
    } else {
      RexCall binaryExpression = (RexCall)joinExp;
      RexNode leftComparand = (RexNode)binaryExpression.operands.get(0);
      RexNode rightComparand = (RexNode)binaryExpression.operands.get(1);
      if(!(leftComparand instanceof RexInputRef)) {
        return false;
      } else if(!(rightComparand instanceof RexInputRef)) {
        return false;
      } else {
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        int rightFieldCount = join.getRight().getRowType().getFieldCount();
        RexInputRef leftFieldAccess = (RexInputRef)leftComparand;
        RexInputRef rightFieldAccess = (RexInputRef)rightComparand;
        if(leftFieldAccess.getIndex() >= leftFieldCount+rightFieldCount ||
           rightFieldAccess.getIndex() >= leftFieldCount+rightFieldCount) {
          return false;
        }
        /* Both columns reference same table */
        if((leftFieldAccess.getIndex() >= leftFieldCount &&
            rightFieldAccess.getIndex() >= leftFieldCount) ||
           (leftFieldAccess.getIndex() < leftFieldCount &&
            rightFieldAccess.getIndex() < leftFieldCount)) {
          return false;
        } else {
          if (leftFieldAccess.getIndex() < leftFieldCount) {
            joinFieldOrdinals[0] = leftFieldAccess.getIndex();
            joinFieldOrdinals[1] = rightFieldAccess.getIndex() - leftFieldCount;
          } else {
            joinFieldOrdinals[0] = rightFieldAccess.getIndex();
            joinFieldOrdinals[1] = leftFieldAccess.getIndex() - leftFieldCount;
          }
          return true;
        }
      }
    }
  }
}
