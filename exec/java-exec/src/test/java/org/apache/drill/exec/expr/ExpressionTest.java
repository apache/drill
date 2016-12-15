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

import java.io.IOException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.impl.project.Projector;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.test.FileMatcher;
import org.junit.Assert;
import org.junit.Test;

import mockit.Expectations;
import mockit.Injectable;
import mockit.NonStrict;
import mockit.NonStrictExpectations;

/**
 * Light testing of the expression parser. Makes use of
 * <a href="http://jmockit.org">JMockit</a> to for the
 * {@link RecordBatch} needed to resolve references to fields.
 * When the expression is null, the batch is required, but unused,
 * so the mocked batch is used in its "bare" form. When an expression
 * references a field, then the field resolution method is mocked
 * to return the desired type.
 * <p>
 * A number of expressions are further verified by comparing the
 * generated code to a "golden" version.
 */

public class ExpressionTest extends ExecTest {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTest.class);

  private final DrillConfig c = DrillConfig.create();
  private final FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);

  @Test
  public void testBasicExpression(@Injectable RecordBatch batch) throws Exception {
    checkExpressionCode("if(true) then 1 else 0 end", batch, "/code/expr/basicExpr.txt");
  }

  @Test
  public void testExprParseUpperExponent(@Injectable final RecordBatch batch) throws Exception {
    final TypeProtos.MajorType type = Types.optional(MinorType.FLOAT8);
    final TypedFieldId tfid = new TypedFieldId(type, false, 0);

    new NonStrictExpectations() {{
      batch.getValueVectorId(new SchemaPath("$f0", ExpressionPosition.UNKNOWN)); result=tfid;
    }};

    checkExpressionCode("multiply(`$f0`, 1.0E-4)", batch, "/code/expr/upperExponent.txt");
  }

  @Test
  public void testExprParseLowerExponent(@Injectable final RecordBatch batch) throws Exception {
    final TypeProtos.MajorType type = Types.optional(MinorType.FLOAT8);
    final TypedFieldId tfid = new TypedFieldId(type, false, 0);

    new NonStrictExpectations() {{
      batch.getValueVectorId(new SchemaPath("$f0", ExpressionPosition.UNKNOWN)); result=tfid;
    }};

    checkExpressionCode("multiply(`$f0`, 1.0e-4)", batch, "/code/expr/lowerExponent.txt");
  }

  @Test
  public void testSpecial(final @Injectable RecordBatch batch) throws Exception {
    checkExpressionCode("1 + 1", batch, "/code/expr/special.txt");
  }

  @Test
  public void testSchemaExpression(final @Injectable RecordBatch batch) throws Exception {
    final TypedFieldId tfid = new TypedFieldId(Types.optional(MinorType.BIGINT), false, 0);

    new NonStrictExpectations() {{
      batch.getValueVectorId(new SchemaPath("alpha", ExpressionPosition.UNKNOWN));
      result = tfid;
    }};
    checkExpressionCode("1 + alpha", batch, "/code/expr/schemaExpr.txt");
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

  /**
   * Generate code for an expression, then compare with saved "golden" value.
   *
   * @param expression the expression to test
   * @param batch the record batch to use
   * @param expected the name of the resource containing the expected result
   * @throws Exception
   */
  private void checkExpressionCode(String expression, RecordBatch batch,
      String expected) throws Exception {
    String code = getExpressionCode(expression, batch);
    try {
      FileMatcher.Builder builder = new FileMatcher.Builder()
          .actualString(code)

          // Remove class serial number.

          .withFilter(new FileMatcher.Filter() {
            @Override
            public String filter(String line) {
              return line.replaceAll("ProjectorGen\\d+", "ProjectorGen");
            }
          });
      if (expected == null) {
        builder.capture();
      } else {
        builder.expectedResource(expected);
      }
      Assert.assertTrue(builder.matches());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
