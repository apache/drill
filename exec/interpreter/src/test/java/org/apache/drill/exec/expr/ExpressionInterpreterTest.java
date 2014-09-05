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
package org.apache.drill.exec.expr;


import com.google.common.collect.Lists;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.mock.MockGroupScanPOP;
import org.apache.drill.exec.store.mock.MockScanBatchCreator;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExpressionInterpreterTest  extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionInterpreterTest.class);

  @Test
  public void interpreterNullableStrExpr() throws Exception {
    String[] colNames = {"col1"};
    TypeProtos.MajorType[] colTypes = {Types.optional(TypeProtos.MinorType.VARCHAR)};
    String expressionStr =  "substr(col1, 1, 3)";
    String[] expectedFirstTwoValues = {"aaa", "null"};

    doTest(expressionStr, colNames, colTypes, expectedFirstTwoValues);
  }


  @Test
  public void interpreterNullableBooleanExpr() throws Exception {
    String[] colNames = {"col1"};
    TypeProtos.MajorType[] colTypes = {Types.optional(TypeProtos.MinorType.VARCHAR)};
    String expressionStr =  "col1 < 'abc' and col1 > 'abc'";
    String[] expectedFirstTwoValues = {"false", "null"};

    doTest(expressionStr, colNames, colTypes, expectedFirstTwoValues);
  }


  @Test
  public void interpreterNullableIntegerExpr() throws Exception {
    String[] colNames = {"col1"};
    TypeProtos.MajorType[] colTypes = {Types.optional(TypeProtos.MinorType.INT)};
    String expressionStr = "col1 + 100 - 1 * 2 + 2";
    String[] expectedFirstTwoValues = {"-2147483548", "null"};

    doTest(expressionStr, colNames, colTypes, expectedFirstTwoValues);
  }

  @Test
  public void interpreterCaseExpr() throws Exception {
    String[] colNames = {"col1"};
    TypeProtos.MajorType[] colTypes = {Types.optional(TypeProtos.MinorType.VARCHAR)};
    String expressionStr =  "case when substr(col1, 1, 3)='aaa' then 'ABC' else 'XYZ' end";
    String[] expectedFirstTwoValues = {"ABC", "XYZ"};

    doTest(expressionStr, colNames, colTypes, expectedFirstTwoValues);
  }


  protected void doTest(String expressionStr, String[] colNames, TypeProtos.MajorType[] colTypes, String[] expectFirstTwoValues) throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    Drillbit bit1 = new Drillbit(CONFIG, serviceSet);

    bit1.run();

    // Create a mock scan batch as input for evaluation.
    assert(colNames.length == colTypes.length);

    MockGroupScanPOP.MockColumn[] columns = new MockGroupScanPOP.MockColumn[colNames.length];

    for (int i = 0; i < colNames.length; i++ ) {
      columns[i] = new MockGroupScanPOP.MockColumn(colNames[i], colTypes[i].getMinorType(), colTypes[i].getMode(),0,0,0);
    }

    MockGroupScanPOP.MockScanEntry entry = new MockGroupScanPOP.MockScanEntry(10, columns);
    MockSubScanPOP scanPOP = new MockSubScanPOP("testTable", java.util.Collections.singletonList(entry));

    RecordBatch batch = createMockScanBatch(bit1, scanPOP);

    batch.next();

    ValueVector vv = evalExprWithInterpreter(expressionStr, batch, bit1);

    // Verify the first 2 values in the output of evaluation.
    assert(expectFirstTwoValues.length == 2);
    assertEquals(expectFirstTwoValues[0], getValueFromVector(vv, 0));
    assertEquals(expectFirstTwoValues[1], getValueFromVector(vv, 1));

    showValueVectorContent(vv);

    vv.clear();
    batch.cleanup();
    batch.getContext().close();
    bit1.close();
  }


  private RecordBatch createMockScanBatch(Drillbit bit, MockSubScanPOP scanPOP) {
    List<RecordBatch> children = Lists.newArrayList();
    MockScanBatchCreator creator = new MockScanBatchCreator();

    try {
      FragmentContext context = new FragmentContext(bit.getContext(), BitControl.PlanFragment.getDefaultInstance(), null, bit.getContext().getFunctionImplementationRegistry());
      return creator.getBatch(context,scanPOP, children);
    } catch (Exception ex) {
      throw new DrillRuntimeException("Error when setup fragment context" + ex);
    }
  }

  private LogicalExpression parseExpr(String expr) throws RecognitionException {
    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    ExprParser.parse_return ret = parser.parse();
    return ret.e;
  }

  private ValueVector evalExprWithInterpreter(String expression, RecordBatch batch, Drillbit bit) throws Exception {
    LogicalExpression expr = parseExpr(expression);
    ErrorCollector error = new ErrorCollectorImpl();
    LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, batch, error, bit.getContext().getFunctionImplementationRegistry());
    if (error.getErrorCount() != 0) {
      logger.error("Failure while materializing expression [{}].  Errors: {}", expression, error);
      assertEquals(0, error.getErrorCount());
    }

    final MaterializedField outputField = MaterializedField.create("outCol", materializedExpr.getMajorType());

    ValueVector vector = TypeHelper.getNewVector(outputField, bit.getContext().getAllocator());

    vector.allocateNewSafe();

    InterpreterEvaluator.evaluate(batch, vector, materializedExpr);

    return vector;
  }

  private void showValueVectorContent(ValueVector vw) {
    for (int row = 0; row < vw.getAccessor().getValueCount(); row ++ ) {
      Object o = vw.getAccessor().getObject(row);
      String cellString;
      if (o instanceof byte[]) {
        cellString = DrillStringUtils.toBinaryString((byte[]) o);
      } else {
        cellString = DrillStringUtils.escapeNewLines(String.valueOf(o));
      }
      System.out.printf(row + "th value: " + cellString + "\n");
    }
  }

  private String getValueFromVector(ValueVector vw, int index) {
    Object o = vw.getAccessor().getObject(index);
    String cellString;
    if (o instanceof byte[]) {
      cellString = DrillStringUtils.toBinaryString((byte[]) o);
    } else {
      cellString = DrillStringUtils.escapeNewLines(String.valueOf(o));
    }
    return cellString;
  }

}
