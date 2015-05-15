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
package org.apache.drill.exec.work.batch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.Collector;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.record.RawFragmentBatch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Determines when a particular fragment has enough data for each of its receiving exchanges to commence execution.  Also monitors whether we've collected all incoming data.
 */
public class IncomingBuffers implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IncomingBuffers.class);

  private final AtomicInteger streamsRemaining = new AtomicInteger(0);
  private final AtomicInteger remainingRequired;
  private final Map<Integer, DataCollector> fragCounts;
  private final FragmentContext context;

  public IncomingBuffers(PlanFragment fragment, FragmentContext context) {
    this.context = context;
    Map<Integer, DataCollector> collectors = Maps.newHashMap();
    remainingRequired = new AtomicInteger(fragment.getCollectorCount());
    for(int i =0; i < fragment.getCollectorCount(); i++){
      Collector collector = fragment.getCollector(i);
      DataCollector newCollector = collector.getSupportsOutOfOrder() ?
          new MergingCollector(remainingRequired, collector, context) :
          new PartitionedCollector(remainingRequired, collector, context);
      collectors.put(collector.getOppositeMajorFragmentId(), newCollector);
    }

    logger.debug("Came up with a list of {} required fragments.  Fragments {}", remainingRequired.get(), collectors);
    fragCounts = ImmutableMap.copyOf(collectors);

    // Determine the total number of incoming streams that will need to be completed before we are finished.
    int totalStreams = 0;
    for (DataCollector bc : fragCounts.values()) {
      totalStreams += bc.getTotalIncomingFragments();
    }
    assert totalStreams >= remainingRequired.get() : String.format("Total Streams %d should be more than the minimum number of streams to commence (%d).  It isn't.", totalStreams, remainingRequired.get());
    streamsRemaining.set(totalStreams);
  }

  public boolean batchArrived(RawFragmentBatch batch) throws FragmentSetupException, IOException {
    // no need to do anything if we've already enabled running.
    // logger.debug("New Batch Arrived {}", batch);
    if (batch.getHeader().getIsOutOfMemory()) {
      for (DataCollector fSet : fragCounts.values()) {
        fSet.batchArrived(0, batch);
      }
      return false;
    }
    if (batch.getHeader().getIsLastBatch()) {
      streamsRemaining.decrementAndGet();
    }
    int sendMajorFragmentId = batch.getHeader().getSendingMajorFragmentId();
    DataCollector fSet = fragCounts.get(sendMajorFragmentId);
    if (fSet == null) {
      throw new FragmentSetupException(String.format("We received a major fragment id that we were not expecting.  The id was %d. %s", sendMajorFragmentId, Arrays.toString(fragCounts.values().toArray())));
    }
    synchronized (this) {
      boolean decremented = fSet.batchArrived(batch.getHeader().getSendingMinorFragmentId(), batch);
      // we should only return true if remaining required has been decremented and is currently equal to zero.
      return decremented && remainingRequired.get() == 0;
    }
  }

  public int getRemainingRequired() {
    int rem = remainingRequired.get();
    if (rem < 0) {
      return 0;
    }
    return rem;
  }

  public RawBatchBuffer[] getBuffers(int senderMajorFragmentId) {
    return fragCounts.get(senderMajorFragmentId).getBuffers();
  }




  public boolean isDone() {
    return streamsRemaining.get() < 1;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(fragCounts.values().toArray(new AutoCloseable[0]));
  }

}
