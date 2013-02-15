/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.rops;

import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Queue;

/**
 * Sink that writes each record to a queue.
 */
public class QueueSinkROP extends BaseSinkROP<SinkOperator> {
  // Records are written as byte arrays representing JSON documents. The
  // receiver presumably re-hydrates them. A representation using compact maps
  // and lists would probably be better.
  //
  // When processing is finished, an OutcomeType object is written.
  private final Queue<Object> queue;

  public QueueSinkROP(SinkOperator config, Queue<Object> queue) {
    super(config);
    this.queue = queue;
  }

  protected void setupSink() throws IOException {
    // nothing
  }

  @Override
  public long sinkRecord(RecordPointer r) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final JSONDataWriter writer = new JSONDataWriter(baos);
    r.write(writer);
    writer.close();
    queue.add(baos.toByteArray());
    return 0;
  }

  @Override
  public void cleanup(RunOutcome.OutcomeType outcome) {
    assert outcome != null;
    queue.add(outcome);
  }
}

// End QueueSinkROP.java
