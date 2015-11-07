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
package io.netty.buffer;

import io.netty.util.internal.StringUtil;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.util.AssertionUtil;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

public class PooledByteBufAllocatorL extends PooledByteBufAllocator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PooledByteBufAllocatorL.class);

  private static final org.slf4j.Logger memoryLogger = org.slf4j.LoggerFactory.getLogger("drill.allocator");
  private static final int MEMORY_LOGGER_FREQUENCY_SECONDS = 60;

  private static final String METRIC_PREFIX = "drill.allocator.";
  public static final PooledByteBufAllocatorL DEFAULT = new PooledByteBufAllocatorL();

  private final MetricRegistry registry = DrillMetrics.getInstance();
  private final AtomicLong hugeBufferSize = new AtomicLong(0);
  private final AtomicLong hugeBufferCount = new AtomicLong(0);
  private final AtomicLong normalBufferSize = new AtomicLong(0);
  private final AtomicLong normalBufferCount = new AtomicLong(0);

  private final PoolArena<ByteBuffer>[] directArenas;
  private final MemoryStatusThread statusThread;
  private final Histogram largeBuffersHist;
  private final Histogram normalBuffersHist;

  private PooledByteBufAllocatorL() {
    super(true);
    try {
      Field f = PooledByteBufAllocator.class.getDeclaredField("directArenas");
      f.setAccessible(true);
      this.directArenas = (PoolArena<ByteBuffer>[]) f.get(this);
    } catch (Exception e) {
      throw new RuntimeException("Failure while initializing allocator.  Unable to retrieve direct arenas field.", e);
    }

    if (memoryLogger.isTraceEnabled()) {
      statusThread = new MemoryStatusThread();
      statusThread.start();
    } else {
      statusThread = null;
    }
    removeOldMetrics();

    registry.register(METRIC_PREFIX + "normal.size", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return normalBufferSize.get();
      }
    });

    registry.register(METRIC_PREFIX + "normal.count", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return normalBufferCount.get();
      }
    });

    registry.register(METRIC_PREFIX + "huge.size", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return hugeBufferSize.get();
      }
    });

    registry.register(METRIC_PREFIX + "huge.count", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return hugeBufferCount.get();
      }
    });

    largeBuffersHist = registry.histogram(METRIC_PREFIX + "huge.hist");
    normalBuffersHist = registry.histogram(METRIC_PREFIX + "normal.hist");

  }

  private synchronized void removeOldMetrics() {
    registry.removeMatching(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.startsWith("drill.allocator.");
      }
    });
  }

  @Override
  protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Drill doesn't support using heap buffers.");
  }

  @Override
  protected UnsafeDirectLittleEndian newDirectBuffer(int initialCapacity, int maxCapacity) {
    PoolThreadCache cache = threadCache.get();
    PoolArena<ByteBuffer> directArena = cache.directArena;

    if (directArena != null) {

      if (initialCapacity > directArena.chunkSize) {
        // This is beyond chunk size so we'll allocate separately.
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(initialCapacity, maxCapacity);

        hugeBufferCount.incrementAndGet();
        hugeBufferSize.addAndGet(buf.capacity());
        largeBuffersHist.update(buf.capacity());
        // logger.debug("Allocating huge buffer of size {}", initialCapacity, new Exception());
        return new UnsafeDirectLittleEndian(new LargeBuffer(buf, hugeBufferSize, hugeBufferCount));

      } else {
        // within chunk, use arena.
        ByteBuf buf = directArena.allocate(cache, initialCapacity, maxCapacity);
        if (!(buf instanceof PooledUnsafeDirectByteBuf)) {
          fail();
        }

        normalBuffersHist.update(buf.capacity());
        if (AssertionUtil.ASSERT_ENABLED) {
          normalBufferSize.addAndGet(buf.capacity());
          normalBufferCount.incrementAndGet();
        }

        return new UnsafeDirectLittleEndian((PooledUnsafeDirectByteBuf) buf, normalBufferCount, normalBufferSize);
      }

    } else {
      throw fail();
    }
  }

  private UnsupportedOperationException fail() {
    return new UnsupportedOperationException(
        "Drill requries that the JVM used supports access sun.misc.Unsafe.  This platform didn't provide that functionality.");
  }


  @Override
  public UnsafeDirectLittleEndian directBuffer(int initialCapacity, int maxCapacity) {
      if (initialCapacity == 0 && maxCapacity == 0) {
          newDirectBuffer(initialCapacity, maxCapacity);
      }
      validate(initialCapacity, maxCapacity);
      return newDirectBuffer(initialCapacity, maxCapacity);
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Drill doesn't support using heap buffers.");
  }


  private static void validate(int initialCapacity, int maxCapacity) {
    if (initialCapacity < 0) {
        throw new IllegalArgumentException("initialCapacity: " + initialCapacity + " (expectd: 0+)");
    }
    if (initialCapacity > maxCapacity) {
        throw new IllegalArgumentException(String.format(
                "initialCapacity: %d (expected: not greater than maxCapacity(%d)",
                initialCapacity, maxCapacity));
    }
  }

  private class MemoryStatusThread extends Thread {

    public MemoryStatusThread() {
      super("memory-status-logger");
      this.setDaemon(true);
      this.setName("allocation.logger");
    }

    @Override
    public void run() {
      while (true) {
        memoryLogger.trace("Memory Usage: \n{}", PooledByteBufAllocatorL.this.toString());
        try {
          Thread.sleep(MEMORY_LOGGER_FREQUENCY_SECONDS * 1000);
        } catch (InterruptedException e) {
          return;
        }

      }
    }

  }

  public void checkAndReset() {
    if (hugeBufferCount.get() != 0 || normalBufferCount.get() != 0) {
      StringBuilder buf = new StringBuilder();
      buf.append("Large buffers outstanding: ");
      buf.append(hugeBufferCount.get());
      buf.append(" totaling ");
      buf.append(hugeBufferSize.get());
      buf.append(" bytes.");
      buf.append('\n');
      buf.append("Normal buffers outstanding: ");
      buf.append(normalBufferCount.get());
      buf.append(" totaling ");
      buf.append(normalBufferSize.get());
      buf.append(" bytes.");
      hugeBufferCount.set(0);
      normalBufferCount.set(0);
      hugeBufferSize.set(0);
      normalBufferSize.set(0);
      throw new DrillRuntimeException(buf.toString());
    }
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(directArenas.length);
    buf.append(" direct arena(s):");
    buf.append(StringUtil.NEWLINE);
    for (PoolArena<ByteBuffer> a : directArenas) {
      buf.append(a);
    }

    buf.append("Large buffers outstanding: ");
    buf.append(this.hugeBufferCount.get());
    buf.append(" totaling ");
    buf.append(this.hugeBufferSize.get());
    buf.append(" bytes.");
    buf.append('\n');
    buf.append("Normal buffers outstanding: ");
    buf.append(this.normalBufferCount.get());
    buf.append(" totaling ");
    buf.append(this.normalBufferSize.get());
    buf.append(" bytes.");
    return buf.toString();
  }
}
