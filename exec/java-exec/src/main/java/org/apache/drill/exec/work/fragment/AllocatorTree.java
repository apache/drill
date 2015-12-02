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
package org.apache.drill.exec.work.fragment;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.exec.memory.AllocatorDecorator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.helper.QueryIdHelper;

import com.google.common.base.Preconditions;

/**
 * AllocatorTree and its inner classes creates a tree of automatically maintained allocators. As long as there is at
 * least one outstanding minor fragment allocator, the query query allocator will be maintained. Once all allocators are
 * closed, the QueryAllocator is automatically closed. The tree is currently:
 *
 * QueryAllocator => MinorFragmentAllocator => GenericAllocator
 */
public class AllocatorTree {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AllocatorTree.class);


  /**
   * Simple AllocatorDecorator that informs a close listener when the allocator is closed.
   */
  private abstract static class CloseListeningAllocator extends AllocatorDecorator {

    final CloseListener listener;

    private CloseListeningAllocator(BufferAllocator allocator, CloseListener listener) {
      super(allocator);
      this.listener = listener;
    }

    @Override
    public void close() {
      if (!super.isClosed()) {
        super.close();
        listener.closeEvent();
      }
    }

  }

  public static QueryAllocator newAllocator(
      BufferAllocator parentAllocator, CloseListener listener,
      final QueryId queryId, long queryReservation, long queryMax
      ) {

    final BufferAllocator inner = parentAllocator
        .newChildAllocator("query:" + QueryIdHelper.getQueryId(queryId), queryReservation, queryMax);
    return new QueryAllocator(inner, listener);
  }

  public static class QueryAllocator extends CloseListeningAllocator {

    private final AtomicInteger openAllocators = new AtomicInteger(0);

    private QueryAllocator(BufferAllocator allocator, CloseListener listener) {
      super(allocator, listener);
    }

    public MinorFragmentAllocator newMinorFragmentAllocator(FragmentHandle handle, long initReservation,
        long maxAllocation) {
      openAllocators.incrementAndGet();
      return new MinorFragmentAllocator(
          super.newChildAllocator(
              "minor:" + QueryIdHelper.getFragmentId(handle),
              initReservation,
              maxAllocation),
          new Listener());
    }

    private class Listener implements CloseListener {
      @Override
      public void closeEvent() {
        int outcome = openAllocators.decrementAndGet();
        Preconditions.checkArgument(outcome > -1);
        if (outcome == 0) {
          listener.closeEvent();
        }
      }
    }


    @Override
    public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
      throw new UnsupportedOperationException("Use newFragmentAllocator().");
    }

  }

  public static class MinorFragmentAllocator extends CloseListeningAllocator {

    private MinorFragmentAllocator(BufferAllocator allocator, CloseListener listener) {
      super(allocator, listener);
    }

  }

  /**
   * Interface to listen to an allocator is closed. Called after the allocator has been closed.
   */
  public static interface CloseListener {
    void closeEvent();
  }


}
