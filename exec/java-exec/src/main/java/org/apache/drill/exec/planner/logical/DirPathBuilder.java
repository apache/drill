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
package org.apache.drill.exec.planner.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.PartitionDescriptor;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlSyntax;

import com.google.common.collect.Lists;

public class DirPathBuilder extends RexVisitorImpl <SchemaPath> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirPathBuilder.class);

  static final String EMPTY_STRING = "";

  final private DrillFilterRel filterRel;
  final private DrillRel inputRel;
  final private RexBuilder builder;
  final private PartitionDescriptor partitionDescriptor;

  private List<String> dirNameList;
  private List<RexNode> conjunctList;
  private List<String> dirPathList = Lists.newArrayList();
  private final static List<String> emptyDirPathList = new ArrayList<>(0);
  private RexNode currentConjunct = null;    // the current conjunct are we evaluating during visitor traversal
  private RexNode finalCondition = null;     // placeholder for the final filter condition
  private boolean dirMatch = false;

  public DirPathBuilder(DrillFilterRel filterRel, DrillRel inputRel, RexBuilder builder, PartitionDescriptor partitionDescriptor) {
    super(true);
    this.filterRel = filterRel;
    this.inputRel = inputRel;
    this.builder = builder;
    this.finalCondition = filterRel.getCondition();
    this.partitionDescriptor = partitionDescriptor;
  }

  private void initPathComponents() {
    int maxHierarchy = partitionDescriptor.getMaxHierarchyLevel();
    dirNameList = Lists.newArrayListWithExpectedSize(maxHierarchy);
    conjunctList = Lists.newArrayListWithExpectedSize(maxHierarchy);
    for (int i=0; i < maxHierarchy; i++) {
      dirNameList.add(EMPTY_STRING);
      conjunctList.add(null);
    }
  }

  /**
   * Build a directory path string for filter conditions containing directory filters.
   * For example, suppose we have directory hierarchy:
   * {orders/2012/Jan...Dec, orders/2013/Jan...Dec, orders/2014/Jan...Dec}
   * path will be built for following types of filters (More types of filters will be added in the future):
   *    1. SELECT * FROM <path>/orders WHERE o_custkey = 5 AND dir0 = '2014' AND dir1 = 'June'
   *    2. SELECT * FROM <path>/orders WHERE (dir0 = '2013' AND dir1 = 'June') OR (dir0 = '2014' AND dir1 = 'June')
   * For (1) dirPath =  <path>/orders/2014/June
   * For (2) there are 2 dirPaths: {<path>/orders/2013/June, <path>/orders/2014/June}
   * @return The list of strings corresponding to directory paths
   */
  public List<String> getDirPath() {
    List<RexNode> disjuncts = RelOptUtil.disjunctions(filterRel.getCondition());
    boolean buildDisjunction = false;
    List<RexNode> newDisjunctList = Lists.newArrayList();

    for (RexNode d : disjuncts) {  // process the top-level disjuncts
      List<RexNode> conjuncts = RelOptUtil.conjunctions(d);
      String dirPath = EMPTY_STRING;
      initPathComponents();

      boolean buildConjunction = false;

      // go through the conjuncts to identify the directory filters
      for (RexNode c : conjuncts) {
        currentConjunct = c;
        SchemaPath expr = c.accept(this);

        if (expr != null) {
          logger.debug("Found directory filter: " + expr.getRootSegment().getPath());
        }
      }

      String prevPath = dirNameList.get(0);

      // compose the final path string
      for (int i = 0; i < dirNameList.size(); i++) {
        String path = dirNameList.get(i);
        if (i > 0) {
          prevPath = dirNameList.get(i-1);
        }
        // Check if both the current path and the previous path are non-empty; currently
        // we will only push a dir<N> filter if dir<N-1> filter is also specified
        if (!path.equals(EMPTY_STRING) && !prevPath.equals(EMPTY_STRING)) {
          dirPath += "/" + path;

          // since we are pushing this directory filter we should remove it from the
          // list of conjuncts
          RexNode thisConjunct = conjunctList.get(i);
          conjuncts.remove(thisConjunct);
          buildConjunction = true;
        }
      }
      if (!dirPath.equals(EMPTY_STRING)) {
        dirPathList.add(dirPath);
      } else {
        // If one of the disjuncts do not satisfy our criteria then we shouldn't apply any optimization
        return emptyDirPathList;
      }

      if (buildConjunction) {
        RexNode newConjunct = RexUtil.composeConjunction(builder, conjuncts, false);
        newDisjunctList.add(newConjunct);
        buildDisjunction = true;
      }

    } // for (disjuncts)

    if (buildDisjunction) {
      this.finalCondition = RexUtil.composeDisjunction(builder, newDisjunctList, false);
    }
    return dirPathList;
  }

  public RexNode getFinalCondition() {
    return finalCondition;
  }

  @Override
  public SchemaPath visitInputRef(RexInputRef inputRef) {
    final int index = inputRef.getIndex();
    final RelDataTypeField field = inputRel.getRowType().getFieldList().get(index);
    if (partitionDescriptor.isPartitionName(field.getName())) {
      dirMatch = true;
    }
    return FieldReference.getWithQuotedRef(field.getName());
  }

  @Override
  public SchemaPath visitCall(RexCall call) {
//    logger.debug("RexCall {}, {}", call);
    final SqlSyntax syntax = call.getOperator().getSyntax();
    switch (syntax) {
    case BINARY:
      if (call.getKind() == SqlKind.EQUALS) {
        dirMatch = false;
        // TODO: an assumption here is that the binary predicate is of the form <column> = <value>.
        // In order to handle predicates of the form '<value> = <column>' we would need to canonicalize
        // the predicate first before calling this function.
        SchemaPath e1 = call.getOperands().get(0).accept(this);

        if (dirMatch && e1 != null) {
          // get the index for the 'dir<N>' filter
          String dirName = e1.getRootSegment().getPath();
          int hierarychyIndex = partitionDescriptor.getPartitionHierarchyIndex(dirName);

          if (hierarychyIndex >= partitionDescriptor.getMaxHierarchyLevel()) {
            return null;
          }

          // SchemaPath e2 = call.getOperands().get(1).accept(this);
          if (call.getOperands().get(1).getKind() == SqlKind.LITERAL) {
            String e2 = ((RexLiteral)call.getOperands().get(1)).getValue2().toString();
            dirNameList.set(hierarychyIndex, e2);
            // dirNameList.set(suffixIndex, e2.getRootSegment().getPath());
            conjunctList.set(hierarychyIndex, currentConjunct);
            return e1;
          }
        }
      }

      return null;

    case SPECIAL:
      switch(call.getKind()) {
      case CAST:
        return getInputFromCast(call);

      default:

      }
      // fall through
    default:
      // throw new AssertionError("Unexpected expression");

    }
    return null;
  }

  private SchemaPath getInputFromCast(RexCall call){
    SchemaPath arg = call.getOperands().get(0).accept(this);
    if (dirMatch) {
      return arg;
    }
    return null;
  }

  @Override
  public SchemaPath visitLocalRef(RexLocalRef localRef) {
    return null;
  }

  @Override
  public SchemaPath visitOver(RexOver over) {
    return null;
  }

  @Override
  public SchemaPath visitCorrelVariable(RexCorrelVariable correlVariable) {
    return null;
  }

  @Override
  public SchemaPath visitDynamicParam(RexDynamicParam dynamicParam) {
    return null;
  }

  @Override
  public SchemaPath visitRangeRef(RexRangeRef rangeRef) {
    return null;
  }

  @Override
  public SchemaPath visitFieldAccess(RexFieldAccess fieldAccess) {
    return super.visitFieldAccess(fieldAccess);
  }

  @Override
  public SchemaPath visitLiteral(RexLiteral literal) {
    return FieldReference.getWithQuotedRef(literal.getValue2().toString());
  }

}
