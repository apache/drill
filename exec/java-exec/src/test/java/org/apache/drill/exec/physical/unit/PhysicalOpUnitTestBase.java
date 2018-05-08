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
package org.apache.drill.exec.physical.unit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.AccountingUserConnection;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentStats;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.logical.DrillLogicalTestutils;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.DrillTestWrapper;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.test.OperatorFixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.TreeMap;

public class PhysicalOpUnitTestBase extends ExecTest {
  protected MockExecutorFragmentContext fragContext;
  protected DrillbitContext drillbitContext;
  protected OperatorFixture.MockOperatorContext opContext;
  protected OperatorFixture operatorFixture;
  protected ExecutorService scanExecutor;
  protected ExecutorService scanDecodeExecutor;

  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  private final DrillConfig drillConf = DrillConfig.create();
  private final ScanResult classpathScan = ClassPathScanner.fromPrescan(drillConf);
  private final OperatorCreatorRegistry opCreatorReg = new OperatorCreatorRegistry(classpathScan);

  @Before
  public void setup() throws Exception {
    scanExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("scan-"));
    scanDecodeExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("scanDecoder-"));

    drillbitContext = Mockito.mock(DrillbitContext.class);
    Mockito.when(drillbitContext.getScanExecutor()).thenReturn(scanExecutor);
    Mockito.when(drillbitContext.getScanDecodeExecutor()).thenReturn(scanDecodeExecutor);

    final OperatorFixture.Builder builder = new OperatorFixture.Builder(dirTestWatcher);
    builder.configBuilder().configProps(drillConf);
    operatorFixture = builder
      .setScanExecutor(scanExecutor)
      .setScanDecoderExecutor(scanDecodeExecutor)
      .build();
    mockFragmentContext();
  }

  @After
  public void teardown() {
    scanExecutor.shutdownNow();
    scanDecodeExecutor.shutdownNow();
  }

  @Override
  protected LogicalExpression parseExpr(String expr) {
    return DrillLogicalTestutils.parseExpr(expr);
  }

  protected Order.Ordering ordering(String expression, RelFieldCollation.Direction direction, RelFieldCollation.NullDirection nullDirection) {
    return DrillLogicalTestutils.ordering(expression, direction, nullDirection);
  }

  protected JoinCondition joinCond(String leftExpr, String relationship, String rightExpr) {
    return DrillLogicalTestutils.joinCond(leftExpr, relationship, rightExpr);
  }

  protected List<NamedExpression> parseExprs(String... expressionsAndOutputNames) {
    return DrillLogicalTestutils.parseExprs(expressionsAndOutputNames);
  }

  protected static class BatchIterator implements Iterable<VectorAccessible> {

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
    private long initReservation = AbstractBase.INIT_ALLOCATION;
    private long maxAllocation = AbstractBase.MAX_ALLOCATION;
    private boolean checkBatchMemory;
    private boolean expectNoRows;
    private Long expectedBatchSize;
    private Integer expectedNumBatches;
    private Integer expectedTotalRows;

    @SuppressWarnings({"unchecked", "resource"})
    public void go() {
      BatchCreator<PhysicalOperator> opCreator;
      RecordBatch testOperator;
      try {
        mockOpContext(popConfig, initReservation, maxAllocation);

        opCreator = (BatchCreator<PhysicalOperator>) opCreatorReg.getOperatorCreator(popConfig.getClass());
        List<RecordBatch> incomingStreams = Lists.newArrayList();
        if (inputStreamsJSON != null) {
          for (List<String> batchesJson : inputStreamsJSON) {
            incomingStreams.add(new ScanBatch(popConfig, fragContext,
                getReaderListForJsonBatches(batchesJson, fragContext)));
          }
        }

        testOperator = opCreator.getBatch(fragContext, popConfig, incomingStreams);

        Map<String, List<Object>> actualSuperVectors = DrillTestWrapper.addToCombinedVectorResults(new BatchIterator(testOperator), expectedBatchSize, expectedNumBatches, expectedTotalRows);
        if ( expectedTotalRows != null ) { return; } // when checking total rows, don't compare actual results

        Map<String, List<Object>> expectedSuperVectors;

        if (expectNoRows) {
          expectedSuperVectors = new TreeMap<>();
          for (String column : baselineColumns) {
            expectedSuperVectors.put(column, new ArrayList<>());
          }
        } else {
          expectedSuperVectors = DrillTestWrapper.translateRecordListToHeapVectors(baselineRecords);
        }

        DrillTestWrapper.compareMergedVectors(expectedSuperVectors, actualSuperVectors);

      } catch (ExecutionSetupException e) {
        throw new RuntimeException(e);
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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

    public OperatorTestBuilder baselineValues(Object... baselineValues) {
      if (baselineRecords == null) {
        baselineRecords = new ArrayList<>();
      }
      Map<String, Object> ret = new HashMap<>();
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

    public OperatorTestBuilder expectZeroRows() {
      this.expectNoRows = true;
      return this;
    }

    public OperatorTestBuilder expectedNumBatches(Integer expectedNumBatches) {
      this.expectedNumBatches = expectedNumBatches;
      return this;
    }

    public OperatorTestBuilder expectedBatchSize(Long batchSize) {
      this.expectedBatchSize = batchSize;
      return this;
    }

    public OperatorTestBuilder expectedTotalRows(Integer expectedTotalRows) {
      this.expectedTotalRows = expectedTotalRows;
      return this;
    }
  }

  /**
   * <h2>Note</h2>
   * <p>
   *   The {@link MockExecutorFragmentContext} should only be used in {@link PhysicalOpUnitTestBase} because {@link PhysicalOpUnitTestBase}
   *   needs a dummy {@link ExecutorFragmentContext} to be passed to batch creators. If you are unit testing operators and need a mock fragment context
   *   please use {@link OperatorFixture.MockFragmentContext}.
   * </p>
   */
  protected static class MockExecutorFragmentContext extends OperatorFixture.MockFragmentContext implements ExecutorFragmentContext {

    public MockExecutorFragmentContext(final FragmentContext fragmentContext) {
      super(fragmentContext.getConfig(), fragmentContext.getOptions(), fragmentContext.getAllocator(),
        fragmentContext.getScanExecutor(), fragmentContext.getScanDecodeExecutor());
    }

    @Override
    public BufferAllocator getRootAllocator() {
      return null;
    }

    @Override
    public PhysicalPlanReader getPlanReader() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ClusterCoordinator getClusterCoordinator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CoordinationProtos.DrillbitEndpoint getForemanEndpoint() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CoordinationProtos.DrillbitEndpoint getEndpoint() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<CoordinationProtos.DrillbitEndpoint> getBits() {
      throw new UnsupportedOperationException();
    }

    @Override
    public OperatorCreatorRegistry getOperatorCreatorRegistry() {
      return null;
    }

    @Override
    public void setBuffers(IncomingBuffers buffers) {
    }

    @Override
    public QueryProfileStoreContext getProfileStoreContext() {
      return null;
    }

    @Override
    public WorkEventBus getWorkEventbus() {
      return null;
    }

    @Override
    public Set<Map.Entry<UserServer.BitToUserConnection, UserServer.BitToUserConnectionConfig>> getUserConnections() {
      return null;
    }

    @Override
    public void waitForSendComplete() {
      throw new UnsupportedOperationException();
    }

    @Override
    public AccountingDataTunnel getDataTunnel(CoordinationProtos.DrillbitEndpoint endpoint) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AccountingUserConnection getUserDataTunnel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Controller getController() {
      return null;
    }

    @Override
    public IncomingBuffers getBuffers() {
      return null;
    }

    @Override
    public FragmentStats getStats() {
      return null;
    }

    @Override
    public void setExecutorState(ExecutorState executorState) {
    }

    @Override
    public boolean isUserAuthenticationEnabled() {
      return false;
    }
  }

  /**
   * <h2>Note</h2>
   * <p>
   *   The {@link MockPhysicalOperator} should only be used in {@link PhysicalOpUnitTestBase} because {@link PhysicalOpUnitTestBase}
   *   needs a dummy {@link MockPhysicalOperator} to be passed to Scanners.
   * </p>
   */
  protected static class MockPhysicalOperator extends AbstractBase
  {
    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return null;
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
      return null;
    }

    @Override
    public int getOperatorType() {
      return 0;
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
      return null;
    }
  }

  protected void mockFragmentContext() throws Exception {
    fragContext = new MockExecutorFragmentContext(operatorFixture.getFragmentContext());
  }

  protected void mockOpContext(final PhysicalOperator popConfig, long initReservation, long maxAllocation) throws Exception {
    opContext = (OperatorFixture.MockOperatorContext)operatorFixture.operatorContext(popConfig);
  }

  protected OperatorCreatorRegistry getOpCreatorReg() {
    return opCreatorReg;
  }

  private Iterator<RecordReader> getRecordReadersForJsonBatches(List<String> jsonBatches, FragmentContext fragContext) {
    return getJsonReadersFromBatchString(jsonBatches, fragContext, Collections.singletonList(SchemaPath.STAR_COLUMN));
  }

  public List<RecordReader> getReaderListForJsonBatches(List<String> jsonBatches, FragmentContext fragContext) {
    Iterator<RecordReader> readers = getRecordReadersForJsonBatches(jsonBatches, fragContext);
    List<RecordReader> readerList = new LinkedList<>();
    while(readers.hasNext()) {
      readerList.add(readers.next());
    }
    return readerList;
  }

  /**
   * Create {@link org.apache.drill.exec.store.easy.json.JSONRecordReader} from input strings.
   * @param jsonBatches : list of input strings, each element represent a batch. Each string could either
   *                    be in the form of "[{...}, {...}, ..., {...}]", or in the form of "{...}".
   * @param fragContext : fragment context
   * @param columnsToRead : list of schema paths to read from JSON reader.
   * @return The {@link org.apache.drill.exec.store.easy.json.JSONRecordReader} corresponding to each given jsonBatch.
   */
  public static Iterator<RecordReader> getJsonReadersFromBatchString(List<String> jsonBatches, FragmentContext fragContext, List<SchemaPath> columnsToRead) {
    ObjectMapper mapper = new ObjectMapper();
    List<RecordReader> readers = new ArrayList<>();
    for (String batchJason : jsonBatches) {
      JsonNode records;
      try {
        records = mapper.readTree(batchJason);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      readers.add(new JSONRecordReader(fragContext, records, null, columnsToRead));
    }
    return readers.iterator();
  }

  /**
   * Create JSONRecordReader from files on a file system.
   * @param fs : file system.
   * @param inputPaths : list of .json file paths.
   * @param fragContext
   * @param columnsToRead
   * @return The {@link org.apache.drill.exec.store.easy.json.JSONRecordReader} corresponding to each given input path.
   */
  public static Iterator<RecordReader> getJsonReadersFromInputFiles(DrillFileSystem fs, List<String> inputPaths, FragmentContext fragContext, List<SchemaPath> columnsToRead) {
    List<RecordReader> readers = new ArrayList<>();
    for (String inputPath : inputPaths) {
      readers.add(new JSONRecordReader(fragContext, inputPath, fs, columnsToRead));
    }
    return readers.iterator();
  }
}
