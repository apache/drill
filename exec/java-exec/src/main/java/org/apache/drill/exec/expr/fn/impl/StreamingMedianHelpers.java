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

package org.apache.drill.exec.expr.fn.impl;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * This class implements a heap-based streaming median.
 *
 *<p>
 * Reference: <a href="https://www.baeldung.com/java-stream-integers-median-using-heap">Stream Integers Median using Heap</a>
 * </p>
 */

public class StreamingMedianHelpers {

  public interface StreamingMedianHelper {
    void reset();
  }

  public static class StreamingIntMedianHelper implements StreamingMedianHelper {
    private final PriorityQueue<Long> minHeap;
    private final PriorityQueue<Long> maxHeap;

    public StreamingIntMedianHelper() {
      super();
      this.minHeap = new PriorityQueue<>();
      this.maxHeap = new PriorityQueue<>(Comparator.reverseOrder());
    }

    public void addNextNumber(Long n) {
      if (!minHeap.isEmpty() && n < minHeap.peek()) {
        maxHeap.offer(n);
        if (maxHeap.size() > minHeap.size() + 1) {
          minHeap.offer(maxHeap.poll());
        }
      } else {
        minHeap.offer(n);
        if (minHeap.size() > maxHeap.size() + 1) {
          maxHeap.offer(minHeap.poll());
        }
      }
    }

    public void addNextNumber(int n) {
      addNextNumber((long) n);
    }

    public Long getMedian() {
      Long median;
      if (minHeap.size() < maxHeap.size()) {
        median = maxHeap.peek();
      } else if (minHeap.size() > maxHeap.size()) {
        median = minHeap.peek();
      } else if (minHeap.isEmpty() && maxHeap.isEmpty()) {
        median = 0L;
      } else {
        median = (minHeap.peek() + maxHeap.peek()) / 2;
      }
      return median;
    }

    public void reset() {
      minHeap.clear();
      maxHeap.clear();
    }
  }

  public static class StreamingDoubleMedianHelper implements StreamingMedianHelper {
    private final PriorityQueue<Double> minHeap;
    private final PriorityQueue<Double> maxHeap;

    public StreamingDoubleMedianHelper() {
      super();
      this.minHeap = new PriorityQueue<>();
      this.maxHeap = new PriorityQueue<>(Comparator.reverseOrder());
    }

    public void addNextNumber(Double n) {
      if (!minHeap.isEmpty() && n < minHeap.peek()) {
        maxHeap.offer(n);
        if (maxHeap.size() > minHeap.size() + 1) {
          minHeap.offer(maxHeap.poll());
        }
      } else {
        minHeap.offer(n);
        if (minHeap.size() > maxHeap.size() + 1) {
          maxHeap.offer(minHeap.poll());
        }
      }
    }

    public void addNextNumber(float n) {
      addNextNumber((double) n);
    }

    public Double getMedian() {
      Double median;
      if (minHeap.size() < maxHeap.size()) {
        median = maxHeap.peek();
      } else if (minHeap.size() > maxHeap.size()) {
        median = minHeap.peek();
      } else if (minHeap.isEmpty() && maxHeap.isEmpty()) {
        median = 0.0;
      } else {
        median = (minHeap.peek() + maxHeap.peek()) / 2;
      }
      return median;
    }

    public void reset() {
      minHeap.clear();
      maxHeap.clear();
    }
  }
}
