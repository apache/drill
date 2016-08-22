/**
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
package org.apache.drill.exec.physical.unit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import mockit.Delegate;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.drill.DrillTestWrapper;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.BufferManagerImpl;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.project.Projector;
import org.apache.drill.exec.physical.impl.project.ProjectorTemplate;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.exec.testing.ExecutionControls;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Look! Doesn't extend BaseTestQuery!!
 */
public class PhysicalOpUnitTestBase extends ExecTest {

  @Injectable FragmentContext fragContext;
  @Injectable OperatorContext opContext;
  @Injectable OperatorStats opStats;
  @Injectable PhysicalOperator popConf;
  @Injectable ExecutionControls executionControls;

  private final DrillConfig drillConf = DrillConfig.create();
  private final BufferAllocator allocator = RootAllocatorFactory.newRoot(drillConf);
  private final BufferManagerImpl bufManager = new BufferManagerImpl(allocator);
  private final ScanResult classpathScan = ClassPathScanner.fromPrescan(drillConf);
  private final FunctionImplementationRegistry funcReg = new FunctionImplementationRegistry(drillConf, classpathScan);
  private final TemplateClassDefinition templateClassDefinition = new TemplateClassDefinition<>(Projector.class, ProjectorTemplate.class);
  private final OperatorCreatorRegistry opCreatorReg = new OperatorCreatorRegistry(classpathScan);

  protected LogicalExpression parseExpr(String expr) {
    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    try {
      return parser.parse().e;
    } catch (RecognitionException e) {
      throw new RuntimeException("Error parsing expression: " + expr);
    }
  }

  protected Order.Ordering ordering(String expression, RelFieldCollation.Direction direction, RelFieldCollation.NullDirection nullDirection) {
    return new Order.Ordering(direction, parseExpr(expression), nullDirection);
  }

  protected JoinCondition joinCond(String leftExpr, String relationship, String rightExpr) {
    return new JoinCondition(relationship, parseExpr(leftExpr), parseExpr(rightExpr));
  }

  protected List<NamedExpression> parseExprs(String... expressionsAndOutputNames) {
    Preconditions.checkArgument(expressionsAndOutputNames.length %2 ==0, "List of expressions and output field names" +
        " is not complete, each expression must explicitly give and output name,");
    List<NamedExpression> ret = new ArrayList<>();
    for (int i = 0; i < expressionsAndOutputNames.length; i += 2) {
      ret.add(new NamedExpression(parseExpr(expressionsAndOutputNames[i]),
          new FieldReference(new SchemaPath(new PathSegment.NameSegment(expressionsAndOutputNames[i+1])))));
    }
    return ret;
  }


  void runTest(OperatorTestBuilder testBuilder) {
    BatchCreator<PhysicalOperator> opCreator;
    RecordBatch testOperator;
    try {
      mockFragmentContext(testBuilder.initReservation, testBuilder.maxAllocation);
      opCreator = (BatchCreator<PhysicalOperator>)
          opCreatorReg.getOperatorCreator(testBuilder.popConfig.getClass());
       List<RecordBatch> incomingStreams = Lists.newArrayList();
       for (List<String> batchesJson : testBuilder.inputStreamsJSON) {
         incomingStreams.add(new ScanBatch(null, fragContext,
             getRecordReadersForJsonBatches(batchesJson, fragContext)));
       }
       testOperator = opCreator.getBatch(fragContext, testBuilder.popConfig, incomingStreams);

      Map<String, List<Object>> actualSuperVectors = DrillTestWrapper.addToCombinedVectorResults(new BatchIterator(testOperator));
      Map<String, List<Object>> expectedSuperVectors = DrillTestWrapper.translateRecordListToHeapVectors(testBuilder.baselineRecords);
      DrillTestWrapper.compareMergedVectors(expectedSuperVectors, actualSuperVectors);

    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class BatchIterator implements Iterable<VectorAccessible> {

    private RecordBatch operator;
    public BatchIterator(RecordBatch operator) {
      this.operator = operator;
    }

    @Override
    public Iterator<VectorAccessible> iterator() {
      return new Iterator<VectorAccessible>() {
        boolean needToGrabNext = true;
        RecordBatch.IterOutcome lastResultOutcome;
        @Override
        public boolean hasNext() {
          if (needToGrabNext) {
            lastResultOutcome = operator.next();
            needToGrabNext = false;
          }
          if (lastResultOutcome == RecordBatch.IterOutcome.NONE
            || lastResultOutcome == RecordBatch.IterOutcome.STOP) {
            return false;
          } else if (lastResultOutcome == RecordBatch.IterOutcome.OUT_OF_MEMORY) {
            throw new RuntimeException("Operator ran out of memory");
          } else {
            return true;
          }
        }

        @Override
        public VectorAccessible next() {
          if (needToGrabNext) {
            lastResultOutcome = operator.next();
          }
          needToGrabNext = true;
          return operator;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove is not supported.");
        }
      };
    }
  }

  protected OperatorTestBuilder opTestBuilder() {
    return new OperatorTestBuilder();
  }

  protected class OperatorTestBuilder {

    private PhysicalOperator popConfig;
    private String[] baselineColumns;
    private List<Map<String, Object>> baselineRecords;
    private List<List<String>> inputStreamsJSON;
    private long initReservation = 10000000;
    private long maxAllocation = 15000000;

    public void go() {
      runTest(this);
    }

    public OperatorTestBuilder physicalOperator(PhysicalOperator batch) {
      this.popConfig = batch;
      return this;
    }

    public OperatorTestBuilder initReservation(long initReservation) {
      this.initReservation = initReservation;
      return this;
    }

    public OperatorTestBuilder maxAllocation(long maxAllocation) {
      this.maxAllocation = maxAllocation;
      return this;
    }

    public OperatorTestBuilder inputDataStreamJson(List<String> jsonBatches) {
      this.inputStreamsJSON = new ArrayList<>();
      this.inputStreamsJSON.add(jsonBatches);
      return this;
    }

    public OperatorTestBuilder inputDataStreamsJson(List<List<String>> childStreams) {
      this.inputStreamsJSON = childStreams;
      return this;
    }

    public OperatorTestBuilder baselineColumns(String... columns) {
      for (int i = 0; i < columns.length; i++) {
        LogicalExpression ex = parseExpr(columns[i]);
        if (ex instanceof SchemaPath) {
          columns[i] = ((SchemaPath)ex).toExpr();
        } else {
          throw new IllegalStateException("Schema path is not a valid format.");
        }
      }
      this.baselineColumns = columns;
      return this;
    }

    public OperatorTestBuilder baselineValues(Object ... baselineValues) {
      if (baselineRecords == null) {
        baselineRecords = new ArrayList();
      }
      Map<String, Object> ret = new HashMap();
      int i = 0;
      Preconditions.checkArgument(baselineValues.length == baselineColumns.length,
          "Must supply the same number of baseline values as columns.");
      for (String s : baselineColumns) {
        ret.put(s, baselineValues[i]);
        i++;
      }
      this.baselineRecords.add(ret);
      return this;
    }
  }

  private void mockFragmentContext(long initReservation, long maxAllocation) throws Exception{
    final CodeCompiler compiler = new CodeCompiler(drillConf, optionManager);
    final BufferAllocator allocator = this.allocator.newChildAllocator("allocator_for_operator_test", initReservation, maxAllocation);
    new NonStrictExpectations() {
      {
//        optManager.getOption(withAny(new TypeValidators.BooleanValidator("", false))); result = false;
//        // TODO(DRILL-4450) - Probably want to just create a default option manager, this is a hack to prevent
//        // the code compilation from failing when trying to decide of scalar replacement is turned on
//        // this will cause other code paths to fail because this return value won't be valid for most
//        // string options
//        optManager.getOption(withAny(new TypeValidators.StringValidator("", "try"))); result = "try";
//        optManager.getOption(withAny(new TypeValidators.PositiveLongValidator("", 1l, 1l))); result = 10;
        fragContext.getOptions(); result = optionManager;
        fragContext.getManagedBuffer(); result = bufManager.getManagedBuffer();
        fragContext.shouldContinue(); result = true;
        fragContext.getExecutionControls(); result = executionControls;
        fragContext.getFunctionRegistry(); result = funcReg;
        fragContext.getConfig(); result = drillConf;
        fragContext.getHandle(); result = ExecProtos.FragmentHandle.getDefaultInstance();
        try {
          fragContext.getImplementationClass(withAny(CodeGenerator.get(templateClassDefinition, funcReg)));
          result = new Delegate()
          {
            Object getImplementationClass(CodeGenerator gen) throws IOException, ClassTransformationException {
              return compiler.getImplementationClass(gen);
            }
          };
          fragContext.getImplementationClass(withAny(CodeGenerator.get(templateClassDefinition, funcReg).getRoot()));
          result = new Delegate()
          {
            Object getImplementationClass(ClassGenerator gen) throws IOException, ClassTransformationException {
              return compiler.getImplementationClass(gen.getCodeGenerator());
            }
          };
        } catch (ClassTransformationException e) {
          throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        opContext.getStats();result = opStats;
        opContext.getAllocator(); result = allocator;
        fragContext.newOperatorContext(withAny(popConf));result = opContext;
      }
    };
  }

  private Iterator<RecordReader> getRecordReadersForJsonBatches(List<String> jsonBatches, FragmentContext fragContext) {
    ObjectMapper mapper = new ObjectMapper();
    List<RecordReader> readers = new ArrayList<>();
    for (String batchJason : jsonBatches) {
      JsonNode records;
      try {
        records = mapper.readTree(batchJason);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      readers.add(new JSONRecordReader(fragContext, records, null, Collections.singletonList(SchemaPath.getSimplePath("*"))));
    }
    return readers.iterator();
  }
}
