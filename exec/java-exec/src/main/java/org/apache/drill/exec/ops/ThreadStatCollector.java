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
package org.apache.drill.exec.ops;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.procedures.LongObjectProcedure;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.AbstractMap.SimpleEntry;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ThreadStatCollector implements Runnable {
  private static final long ONE_BILLION = 1000000000;
  private static final long RETAIN_INTERVAL = 5 * ONE_BILLION;
  private static final int COLLECTION_INTERVAL = 1;

  private ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
  private ThreadStat cpuStat = new ThreadStat();
  private ThreadStat userStat = new ThreadStat();

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(COLLECTION_INTERVAL * 1000);
        addCpuTime();
        addUserTime();
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  public Integer getCpuTrailingAverage(long id, int seconds) {
    return cpuStat.getTrailingAverage(id, seconds);
  }

  public Integer getUserTrailingAverage(long id, int seconds) {
    return userStat.getTrailingAverage(id, seconds);
  }

  private void addCpuTime() {
    for (long id : mxBean.getAllThreadIds()) {
      cpuStat.add(id, System.nanoTime(), mxBean.getThreadCpuTime(id));
    }
  }

  private void addUserTime() {
    for (long id : mxBean.getAllThreadIds()) {
      userStat.add(id, System.nanoTime(), mxBean.getThreadUserTime(id));
    }
  }

  private static class ThreadStat {
    volatile LongObjectHashMap<Deque<Entry<Long,Long>>> data = new LongObjectHashMap<>();

    public void add(long id, long ts, long value) {
      Entry<Long,Long> entry = new SimpleEntry<>(ts, value);
      Deque<Entry<Long,Long>> list = data.get(id);
      if (list == null) {
        list = new ConcurrentLinkedDeque<>();
      }
      list.add(entry);
      while (ts - list.peekFirst().getKey() > RETAIN_INTERVAL) {
        list.removeFirst();
      }
      data.put(id, list);
    }

    public Integer getTrailingAverage(long id, int seconds) {
      Deque<Entry<Long,Long>> list = data.get(id);
      if (list == null) {
        return null;
      }
      return getTrailingAverage(list, seconds);
    }

    private Integer getTrailingAverage(Deque<Entry<Long, Long>> list, int seconds) {
      Entry<Long,Long> latest = list.peekLast();
      Entry<Long,Long> old = list.peekFirst();
      Iterator<Entry<Long,Long>> iter = list.descendingIterator();
      while (iter.hasNext()) {
        Entry<Long,Long> e = iter.next();
        if (e.getKey() - latest.getKey() > seconds * ONE_BILLION) {
          old = e;
          break;
        }
      }
      try {
        return (int) (100 * (old.getValue() - latest.getValue()) / (old.getKey() - latest.getKey()));
      } catch (Exception e) {
        return null;
      }
    }

    public void print(final int window) {
      data.forEach(new LongObjectProcedure<Deque<Entry<Long,Long>>>() {
        @Override
        public void apply(long l, Deque<Entry<Long,Long>> entries) {
          System.out.println(String.format("%d %d", l, getTrailingAverage(entries, window)));
        }
      });
    }
  }
}
