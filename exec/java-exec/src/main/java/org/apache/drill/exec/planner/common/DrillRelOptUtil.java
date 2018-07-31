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
package org.apache.drill.exec.planner.common;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
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
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.resolver.TypeCastRules;

/**
 * Utility class that is a subset of the RelOptUtil class and is a placeholder for Drill specific
 * static methods that are needed during either logical or physical planning.
 */
public abstract class DrillRelOptUtil {

  final public static String IMPLICIT_COLUMN = "$drill_implicit_field$";

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
        return TypeCastRules.getLeastRestrictiveType(types) != null;
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

    return DrillRelFactories.LOGICAL_BUILDER
        .create(rel.getCluster(), null)
        .push(rel)
        .projectNamed(refs, fieldNames, true)
        .build();
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
   * @return a rowType having all unique field name.
   */
  public static RelDataType uniqifyFieldName(final RelDataType rowType, final RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(RelOptUtil.getFieldTypeList(rowType),
        SqlValidatorUtil.uniquify(rowType.getFieldNames(), SqlValidatorUtil.EXPR_SUGGESTER, true));
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
   * Travesal RexNode to find at least one operator in the given collection. Continue search if RexNode has a
   * RexInputRef which refers to a RexNode in project expressions.
   *
   * @param node : RexNode to search
   * @param projExprs : the list of project expressions. Empty list means there is No project operator underneath.
   * @param operators collection of operators to find
   * @return : Return null if there is NONE; return the first appearance of item/flatten RexCall.
   */
  public static RexCall findOperators(final RexNode node, final List<RexNode> projExprs, final Collection<String> operators) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {
              if (operators.contains(call.getOperator().getName().toLowerCase())) {
                throw new Util.FoundOne(call); /* throw exception to interrupt tree walk (this is similar to
                                              other utility methods in RexUtil.java */
              }
              return super.visitCall(call);
            }

            public Void visitInputRef(RexInputRef inputRef) {
              if (projExprs.size() == 0 ) {
                return super.visitInputRef(inputRef);
              } else {
                final int index = inputRef.getIndex();
                RexNode n = projExprs.get(index);
                if (n instanceof RexCall) {
                  RexCall r = (RexCall) n;
                  if (operators.contains(r.getOperator().getName().toLowerCase())) {
                    throw new Util.FoundOne(r);
                  }
                }

                return super.visitInputRef(inputRef);
              }
            }
          };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexCall) e.getNode();
    }
  }

  public static boolean isLimit0(RexNode fetch) {
    if (fetch != null && fetch.isA(SqlKind.LITERAL)) {
      RexLiteral l = (RexLiteral) fetch;
      switch (l.getTypeName()) {
        case BIGINT:
        case INTEGER:
        case DECIMAL:
          if (((long) l.getValue2()) == 0) {
            return true;
          }
      }
    }
    return false;
  }

  /**
   * Find whether the given project rel can produce non-scalar output (hence unknown rowcount). This
   * would happen if the project has a flatten
   * @param project : The project rel
   * @return : Return true if the rowcount is unknown. Otherwise, false.
   */
  public static boolean isProjectOutputRowcountUnknown(Project project) {
    for (RexNode rex : project.getProjects()) {
      if (rex instanceof RexCall) {
        if ("flatten".equals(((RexCall) rex).getOperator().getName().toLowerCase())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Find whether the given project rel has unknown output schema. This would happen if the
   * project has CONVERT_FROMJSON which can only derive the schema after evaluation is performed
   * @param project : The project rel
   * @return : Return true if the project output schema is unknown. Otherwise, false.
   */
  public static boolean isProjectOutputSchemaUnknown(Project project) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {
              if ("convert_fromjson".equals(call.getOperator().getName().toLowerCase())) {
                throw new Util.FoundOne(call); /* throw exception to interrupt tree walk (this is similar to
                                              other utility methods in RexUtil.java */
              }
              return super.visitCall(call);
            }
          };
      for (RexNode rex : project.getProjects()) {
        rex.accept(visitor);
      }
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return true;
    }
    return false;
  }

  /**
   * InputRefVisitor is a utility class used to collect all the RexInputRef nodes in a
   * RexNode.
   *
   */
  public static class InputRefVisitor extends RexVisitorImpl<Void> {
    private final List<RexInputRef> inputRefList;

    public InputRefVisitor() {
      super(true);
      inputRefList = new ArrayList<>();
    }

    public Void visitInputRef(RexInputRef ref) {
      inputRefList.add(ref);
      return null;
    }

    public Void visitCall(RexCall call) {
      for (RexNode operand : call.operands) {
        operand.accept(this);
      }
      return null;
    }

    public List<RexInputRef> getInputRefs() {
      return inputRefList;
    }
  }

  /**
   * For a given row type return a map between old field indices and one index right shifted fields.
   * @param rowType : row type to be right shifted.
   * @return map: hash map between old and new indices
   */
  public static Map<Integer, Integer> rightShiftColsInRowType(RelDataType rowType) {
    Map<Integer, Integer> map = new HashMap<>();
    int fieldCount = rowType.getFieldCount();
    for (int i = 0; i< fieldCount; i++) {
      map.put(i, i+1);
    }
    return map;
  }

  /**
   * Given a list of rexnodes it transforms the rexnodes by changing the expr to use new index mapped to the old index.
   * @param builder : RexBuilder from the planner.
   * @param exprs: RexNodes to be transformed.
   * @param corrMap: Mapping between old index to new index.
   * @return
   */
  public static List<RexNode> transformExprs(RexBuilder builder, List<RexNode> exprs, Map<Integer, Integer> corrMap) {
    List<RexNode> outputExprs = new ArrayList<>();
    DrillRelOptUtil.RexFieldsTransformer transformer = new DrillRelOptUtil.RexFieldsTransformer(builder, corrMap);
    for (RexNode expr : exprs) {
      outputExprs.add(transformer.go(expr));
    }
    return outputExprs;
  }

  /**
   * Given a of rexnode it transforms the rexnode by changing the expr to use new index mapped to the old index.
   * @param builder : RexBuilder from the planner.
   * @param expr: RexNode to be transformed.
   * @param corrMap: Mapping between old index to new index.
   * @return
   */
  public static RexNode transformExpr(RexBuilder builder, RexNode expr, Map<Integer, Integer> corrMap) {
    DrillRelOptUtil.RexFieldsTransformer transformer = new DrillRelOptUtil.RexFieldsTransformer(builder, corrMap);
    return transformer.go(expr);
  }

  /**
   * RexFieldsTransformer is a utility class used to convert column refs in a RexNode
   * based on inputRefMap (input to output ref map).
   *
   * This transformer can be used to find and replace the existing inputRef in a RexNode with a new inputRef.
   */
  public static class RexFieldsTransformer {
    private final RexBuilder rexBuilder;
    private final Map<Integer, Integer> inputRefMap;

    public RexFieldsTransformer(
      RexBuilder rexBuilder,
      Map<Integer, Integer> inputRefMap) {
      this.rexBuilder = rexBuilder;
      this.inputRefMap = inputRefMap;
    }

    public RexNode go(RexNode rex) {
      if (rex instanceof RexCall) {
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        final RexCall call = (RexCall) rex;
        for (RexNode operand : call.operands) {
          builder.add(go(operand));
        }
        return call.clone(call.getType(), builder.build());
      } else if (rex instanceof RexInputRef) {
        RexInputRef var = (RexInputRef) rex;
        int index = var.getIndex();
        return rexBuilder.makeInputRef(var.getType(), inputRefMap.get(index));
      } else {
        return rex;
      }
    }
  }
}
