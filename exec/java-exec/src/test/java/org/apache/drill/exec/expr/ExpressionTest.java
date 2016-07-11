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

import static org.junit.Assert.assertEquals;
import mockit.Expectations;
import mockit.Injectable;
import mockit.NonStrict;
import mockit.NonStrictExpectations;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.expression.parser.ExprParser.parse_return;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.physical.impl.project.Projector;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

public class ExpressionTest extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTest.class);

  private final DrillConfig c = DrillConfig.create();
  private final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);

  @Test
  public void testBasicExpression(@Injectable RecordBatch batch) throws Exception {
    System.out.println(getExpressionCode("if(true) then 1 else 0 end", batch));
  }

  @Test
  public void testExprParseUpperExponent(@Injectable RecordBatch batch) throws Exception {
    getExpressionCode("multiply(`$f0`, 1.0E-4)", batch);
  }

  @Test
  public void testExprParseLowerExponent(@Injectable RecordBatch batch) throws Exception {
    getExpressionCode("multiply(`$f0`, 1.0e-4)", batch);
  }

  @Test
  public void testSpecial(final @Injectable RecordBatch batch, @Injectable ValueVector vector) throws Exception {
    final TypeProtos.MajorType type = Types.optional(MinorType.INT);
    final TypedFieldId tfid = new TypedFieldId(type, false, 0);

    new NonStrictExpectations() {
      @NonStrict VectorWrapper<?> wrapper;
      {
        batch.getValueVectorId(new SchemaPath("alpha", ExpressionPosition.UNKNOWN));
        result = tfid;
        batch.getValueAccessorById(IntVector.class, tfid.getFieldIds());
        result = wrapper;
        wrapper.getValueVector();
        result = new IntVector(MaterializedField.create("result", type), RootAllocatorFactory.newRoot(c));
      }

    };
    System.out.println(getExpressionCode("1 + 1", batch));
  }

  @Test
  public void testSchemaExpression(final @Injectable RecordBatch batch) throws Exception {
    final TypedFieldId tfid = new TypedFieldId(Types.optional(MinorType.BIGINT), false, 0);

    new Expectations() {
      {
        batch.getValueVectorId(new SchemaPath("alpha", ExpressionPosition.UNKNOWN));
        result = tfid;
        // batch.getValueVectorById(tfid); result = new Fixed4(null, null);
      }

    };
    System.out.println(getExpressionCode("1 + alpha", batch));

  }

  @Test(expected = ExpressionParsingException.class)
  public void testExprParseError(@Injectable RecordBatch batch) throws Exception {
    getExpressionCode("less than(1, 2)", batch);
  }

  @Test
  public void testExprParseNoError(@Injectable RecordBatch batch) throws Exception {
    getExpressionCode("equal(1, 2)", batch);
  }

  // HELPER METHODS //

  private LogicalExpression parseExpr(String expr) throws RecognitionException {
    final ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final ExprParser parser = new ExprParser(tokens);
    parse_return ret = parser.parse();
    return ret.e;
  }

  private String getExpressionCode(String expression, RecordBatch batch) throws Exception {
    final LogicalExpression expr = parseExpr(expression);
    final ErrorCollector error = new ErrorCollectorImpl();
    final LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, batch, error, registry);
    if (error.getErrorCount() != 0) {
      logger.error("Failure while materializing expression [{}].  Errors: {}", expression, error);
      assertEquals(0, error.getErrorCount());
    }

    FunctionImplementationRegistry funcReg = new FunctionImplementationRegistry(DrillConfig.create());
    final ClassGenerator<Projector> cg = CodeGenerator.get(Projector.TEMPLATE_DEFINITION, funcReg, null).getRoot();
    cg.addExpr(new ValueVectorWriteExpression(new TypedFieldId(materializedExpr.getMajorType(), -1), materializedExpr));
    return cg.getCodeGenerator().generateAndGet();
  }
}
