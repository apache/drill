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
package org.apache.drill.exec.physical.impl;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.AccountingUserConnection;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.RootFragmentContext;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.physical.impl.materialize.RecordMaterializer;
import org.apache.drill.exec.physical.impl.materialize.VectorRecordMaterializer;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class ScreenCreator implements RootCreator<Screen> {
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ScreenCreator.class);

  @Override
  public RootExec getRoot(ExecutorFragmentContext context, Screen config, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkNotNull(children);
    Preconditions.checkArgument(children.size() == 1);
    return new ScreenRoot(context, children.iterator().next(), config);
  }

  public static class ScreenRoot extends BaseRootExec {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScreenRoot.class);
    private final RecordBatch incoming;
    private final RootFragmentContext context;
    private final AccountingUserConnection userConnection;
    private RecordMaterializer materializer;

    private boolean firstBatch = true;

    public enum Metric implements MetricDef {
      BYTES_SENT;

      @Override
      public int metricId() {
        return ordinal();
      }
    }

    public ScreenRoot(RootFragmentContext context, RecordBatch incoming, Screen config) throws OutOfMemoryException {
      super(context, config);
      this.context = context;
      this.incoming = incoming;
      userConnection = context.getUserDataTunnel();
    }

    public RootFragmentContext getContext() {
      return context;
    }

    @Override
    public boolean innerNext() {
      IterOutcome outcome = next(incoming);
      logger.trace("Screen Outcome {}", outcome);
      switch (outcome) {
      case OUT_OF_MEMORY:
        throw new OutOfMemoryException();
      case STOP:
        return false;
      case NONE:
        if (firstBatch) {
          // this is the only data message sent to the client and may contain the schema
          QueryWritableBatch batch;
          QueryData header = QueryData.newBuilder()
            .setQueryId(context.getHandle().getQueryId())
            .setRowCount(0)
            .setDef(RecordBatchDef.getDefaultInstance())
            .build();
          batch = new QueryWritableBatch(header);

          stats.startWait();
          try {
            userConnection.sendData(batch);
          } finally {
            stats.stopWait();
          }
          firstBatch = false; // we don't really need to set this. But who knows!
        }

        return false;
      case OK_NEW_SCHEMA:
        materializer = new VectorRecordMaterializer(context, oContext, incoming);
        //$FALL-THROUGH$
      case OK:
        injector.injectPause(context.getExecutionControls(), "sending-data", logger);
        final QueryWritableBatch batch = materializer.convertNext();
        updateStats(batch);
        stats.startWait();
        try {
          userConnection.sendData(batch);
        } finally {
          stats.stopWait();
        }
        firstBatch = false;

        return true;
      default:
        throw new UnsupportedOperationException();
      }
    }

    public void updateStats(QueryWritableBatch queryBatch) {
      stats.addLongStat(Metric.BYTES_SENT, queryBatch.getByteCount());
    }

    RecordBatch getIncoming() {
      return incoming;
    }

    @Override
    public void close() throws Exception {
      injector.injectPause(context.getExecutionControls(), "send-complete", logger);
      super.close();
    }
  }
}
