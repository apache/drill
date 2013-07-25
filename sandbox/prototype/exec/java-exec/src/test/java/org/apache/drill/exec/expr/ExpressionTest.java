package org.apache.drill.exec.expr;

import static org.junit.Assert.assertEquals;
import mockit.Expectations;
import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.expression.parser.ExprParser.parse_return;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.impl.project.Projector;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;
import org.apache.drill.exec.vector.IntVector;
import org.junit.After;
import org.junit.Test;

public class ExpressionTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTest.class);

  @Test
  public void testBasicExpression(@Injectable RecordBatch batch) throws Exception {
    System.out.println(getExpressionCode("if(true) then 1 else 0 end", batch));
  }

  @Test
  public void testSpecial(final @Injectable RecordBatch batch) throws Exception {
    final TypedFieldId tfid = new TypedFieldId(MajorType.newBuilder().setMode(DataMode.OPTIONAL)
        .setMinorType(MinorType.INT).build(), 0);

    new NonStrictExpectations() {
      {
        batch.getValueVectorId(new SchemaPath("alpha", ExpressionPosition.UNKNOWN));
        result = tfid;
        batch.getValueVectorById(tfid.getFieldId(), IntVector.class);
        result = new IntVector(null, null);
      }

    };
    System.out.println(getExpressionCode("1 + 1", batch));
  }

  @Test
  public void testSchemaExpression(final @Injectable RecordBatch batch) throws Exception {
    final TypedFieldId tfid = new TypedFieldId(MajorType.newBuilder().setMode(DataMode.OPTIONAL)
        .setMinorType(MinorType.BIGINT).build(), 0);

    new Expectations() {
      {
        batch.getValueVectorId(new SchemaPath("alpha", ExpressionPosition.UNKNOWN));
        result = tfid;
        // batch.getValueVectorById(tfid); result = new Fixed4(null, null);
      }

    };
    System.out.println(getExpressionCode("1 + alpha", batch));

  }

  // HELPER METHODS //

  private LogicalExpression parseExpr(String expr) throws RecognitionException {
    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setRegistry(new FunctionRegistry(DrillConfig.create()));
    parse_return ret = parser.parse();
    return ret.e;
  }

  private String getExpressionCode(String expression, RecordBatch batch) throws Exception {
    LogicalExpression expr = parseExpr(expression);
    ErrorCollector error = new ErrorCollectorImpl();
    LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(expr, batch, error);
    if (error.getErrorCount() != 0) {
      logger.error("Failure while materializing expression [{}].  Errors: {}", expression, error);
      assertEquals(0, error.getErrorCount());
    }

    CodeGenerator<Projector> cg = new CodeGenerator<Projector>(Projector.TEMPLATE_DEFINITION, new FunctionImplementationRegistry(DrillConfig.create()));
    cg.addExpr(new ValueVectorWriteExpression(-1, materializedExpr));
    return cg.generate();
  }

  @After
  public void tearDown() throws Exception{
    // pause to get logger to catch up.
    Thread.sleep(1000);
  }
}
