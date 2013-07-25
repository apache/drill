package org.apache.drill.exec.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.expression.ArgumentValidator;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.proto.SchemaDefProtos.NamePart;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;

public class ExpressionTreeMaterializerTest {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTreeMaterializerTest.class);

  final MajorType boolConstant = MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(MinorType.BOOLEAN).build();
  final MajorType bigIntType = MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(MinorType.BIGINT).build();
  final MajorType intType = MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(MinorType.INT).build();

  private MaterializedField getField(int fieldId, String name, MajorType type) {
    return new MaterializedField(FieldDef.newBuilder().setMajorType(type)
        .addName(NamePart.newBuilder().setName(name)).build());
  }


  @Test
  public void testMaterializingConstantTree(@Injectable RecordBatch batch) throws SchemaChangeException {
    
    ErrorCollector ec = new ErrorCollectorImpl();
    LogicalExpression expr = ExpressionTreeMaterializer.materialize(new ValueExpressions.LongExpression(1L, ExpressionPosition.UNKNOWN), batch, ec);
    assertTrue(expr instanceof ValueExpressions.LongExpression);
    assertEquals(1L, ValueExpressions.LongExpression.class.cast(expr).getLong());
    assertFalse(ec.hasErrors());
  }

  @Test
  public void testMaterializingLateboundField(final @Injectable RecordBatch batch) throws SchemaChangeException {
    final SchemaBuilder builder = BatchSchema.newBuilder();
    builder.addField(getField(2, "test", bigIntType));
    final BatchSchema schema = builder.build();
    
    new NonStrictExpectations() {
      {
        batch.getValueVectorId(new FieldReference("test", ExpressionPosition.UNKNOWN)); result = new TypedFieldId(Types.required(MinorType.BIGINT), -5);
      }
    };
    
    ErrorCollector ec = new ErrorCollectorImpl();
    LogicalExpression expr = ExpressionTreeMaterializer.materialize(new FieldReference("test",
        ExpressionPosition.UNKNOWN), batch, ec);
    assertEquals(bigIntType, expr.getMajorType());
    assertFalse(ec.hasErrors());
  }

  @Test
  public void testMaterializingLateboundTree(final @Injectable RecordBatch batch) throws SchemaChangeException {
    new NonStrictExpectations() {
      {
        batch.getValueVectorId(new FieldReference("test", ExpressionPosition.UNKNOWN)); result = new TypedFieldId(Types.required(MinorType.BOOLEAN), -4);
        batch.getValueVectorId(new FieldReference("test1", ExpressionPosition.UNKNOWN)); result = new TypedFieldId(Types.required(MinorType.BIGINT), -5);
      }
    };
    
    ErrorCollector ec = new ErrorCollectorImpl();

    
      LogicalExpression expr = new IfExpression.Builder()
        .addCondition(
            new IfExpression.IfCondition( //
                new FieldReference("test", ExpressionPosition.UNKNOWN), //
                new IfExpression.Builder() //
                    .addCondition( //
                        new IfExpression.IfCondition( //
                            new ValueExpressions.BooleanExpression("true", ExpressionPosition.UNKNOWN), new FieldReference(
                                "test1", ExpressionPosition.UNKNOWN)))
                    .setElse(new ValueExpressions.LongExpression(1L, ExpressionPosition.UNKNOWN)).build()) //
        ) //
        .setElse(new ValueExpressions.LongExpression(1L, ExpressionPosition.UNKNOWN)).build();
    LogicalExpression newExpr = ExpressionTreeMaterializer.materialize(expr, batch, ec);
    assertTrue(newExpr instanceof IfExpression);
    IfExpression newIfExpr = (IfExpression) newExpr;
    assertEquals(1, newIfExpr.conditions.size());
    IfExpression.IfCondition ifCondition = newIfExpr.conditions.get(0);
    assertTrue(ifCondition.expression instanceof IfExpression);
    newIfExpr = (IfExpression) ifCondition.expression;
    assertEquals(1, newIfExpr.conditions.size());
    ifCondition = newIfExpr.conditions.get(0);
    assertEquals(bigIntType, ifCondition.expression.getMajorType());
    assertEquals(true, ((ValueExpressions.BooleanExpression) ifCondition.condition).value);
    if (ec.hasErrors()) System.out.println(ec.toErrorString());
    assertFalse(ec.hasErrors());
  }

  @Test
  public void testMaterializingLateboundTreeValidated(final @Injectable RecordBatch batch) throws SchemaChangeException {
    ErrorCollector ec = new ErrorCollector() {
      int errorCount = 0;

      @Override
      public void addGeneralError(ExpressionPosition expr, String s) {
        errorCount++;
      }

      @Override
      public void addUnexpectedArgumentType(ExpressionPosition expr, String name, MajorType actual, MajorType[] expected,
          int argumentIndex) {
        errorCount++;
      }

      @Override
      public void addUnexpectedArgumentCount(ExpressionPosition expr, int actual, Range<Integer> expected) {
        errorCount++;
      }

      @Override
      public void addUnexpectedArgumentCount(ExpressionPosition expr, int actual, int expected) {
        errorCount++;
      }

      @Override
      public void addNonNumericType(ExpressionPosition expr, MajorType actual) {
        errorCount++;
      }

      @Override
      public void addUnexpectedType(ExpressionPosition expr, int index, MajorType actual) {
        errorCount++;
      }

      @Override
      public void addExpectedConstantValue(ExpressionPosition expr, int actual, String s) {
        errorCount++;
      }

      @Override
      public boolean hasErrors() {
        return errorCount > 0;
      }

      @Override
      public String toErrorString() {
        return String.format("Found %s errors.", errorCount);
      }

      @Override
      public int getErrorCount() {
        return errorCount;
      }
    };

    new NonStrictExpectations() {
      {
        batch.getValueVectorId(new FieldReference("test", ExpressionPosition.UNKNOWN)); result = new TypedFieldId(Types.required(MinorType.BIGINT), -5);
      }
    };
    
    LogicalExpression functionCallExpr = new FunctionCall(FunctionDefinition.simple("testFunc",
        new ArgumentValidator() {
          @Override
          public void validateArguments(ExpressionPosition expr, List<LogicalExpression> expressions, ErrorCollector errors) {
            errors.addGeneralError(expr, "Error!");
          }

          @Override
          public String[] getArgumentNamesByPosition() {
            return new String[0];
          }
        }, OutputTypeDeterminer.FIXED_BOOLEAN), Lists.newArrayList((LogicalExpression) new FieldReference("test",
        ExpressionPosition.UNKNOWN)), ExpressionPosition.UNKNOWN);
    LogicalExpression newExpr = ExpressionTreeMaterializer.materialize(functionCallExpr, batch, ec);
    assertTrue(newExpr instanceof FunctionCall);
    FunctionCall funcExpr = (FunctionCall) newExpr;
    assertEquals(1, funcExpr.args.size());
    assertEquals(bigIntType, funcExpr.args.get(0).getMajorType());
    assertEquals(1, ec.getErrorCount());
    System.out.println(ec.toErrorString());
  }
}
