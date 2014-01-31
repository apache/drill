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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class PooledByteBufAllocatorL extends AbstractByteBufAllocator {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PooledByteBufAllocatorL.class);

    private static final int DEFAULT_NUM_HEAP_ARENA;
    private static final int DEFAULT_NUM_DIRECT_ARENA;

    private static final int DEFAULT_PAGE_SIZE;
    private static final int DEFAULT_MAX_ORDER; // 8192 << 11 = 16 MiB per chunk

    private static final int MIN_PAGE_SIZE = 4096;
    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

    static {
        int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
        Throwable pageSizeFallbackCause = null;
        try {
            validateAndCalculatePageShifts(defaultPageSize);
        } catch (Throwable t) {
            pageSizeFallbackCause = t;
            defaultPageSize = 8192;
        }
        DEFAULT_PAGE_SIZE = defaultPageSize;

        int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 11);
        Throwable maxOrderFallbackCause = null;
        try {
            validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
        } catch (Throwable t) {
            maxOrderFallbackCause = t;
            defaultMaxOrder = 11;
        }
        DEFAULT_MAX_ORDER = defaultMaxOrder;

        // Determine reasonable default for nHeapArena and nDirectArena.
        // Assuming each arena has 3 chunks, the pool should not consume more than 50% of max memory.
        final Runtime runtime = Runtime.getRuntime();
        final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
        DEFAULT_NUM_HEAP_ARENA = Math.max(0,
                SystemPropertyUtil.getInt(
                        "io.netty.allocator.numHeapArenas",
                        (int) Math.min(
                                runtime.availableProcessors(),
                                Runtime.getRuntime().maxMemory() / defaultChunkSize / 2 / 3)));
        DEFAULT_NUM_DIRECT_ARENA = Math.max(0,
                SystemPropertyUtil.getInt(
                        "io.netty.allocator.numDirectArenas",
                        (int) Math.min(
                                runtime.availableProcessors(),
                                PlatformDependent.maxDirectMemory() / defaultChunkSize / 2 / 3)));

        if (logger.isDebugEnabled()) {
            logger.debug("-Dio.netty.allocator.numHeapArenas: {}", DEFAULT_NUM_HEAP_ARENA);
            logger.debug("-Dio.netty.allocator.numDirectArenas: {}", DEFAULT_NUM_DIRECT_ARENA);
            if (pageSizeFallbackCause == null) {
                logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE);
            } else {
                logger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE, pageSizeFallbackCause);
            }
            if (maxOrderFallbackCause == null) {
                logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER);
            } else {
                logger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER, maxOrderFallbackCause);
            }
            logger.debug("-Dio.netty.allocator.chunkSize: {}", DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER);
        }
    }

    public static final PooledByteBufAllocatorL DEFAULT =
            new PooledByteBufAllocatorL(PlatformDependent.directBufferPreferred());

    private final PoolArenaL<byte[]>[] heapArenas;
    private final PoolArenaL<ByteBuffer>[] directArenas;

    final ThreadLocal<PoolThreadCacheL> threadCache = new ThreadLocal<PoolThreadCacheL>() {
        private final AtomicInteger index = new AtomicInteger();
        @Override
        protected PoolThreadCacheL initialValue() {
            final int idx = index.getAndIncrement();
            final PoolArenaL<byte[]> heapArena;
            final PoolArenaL<ByteBuffer> directArena;

            if (heapArenas != null) {
                heapArena = heapArenas[Math.abs(idx % heapArenas.length)];
            } else {
                heapArena = null;
            }

            if (directArenas != null) {
                directArena = directArenas[Math.abs(idx % directArenas.length)];
            } else {
                directArena = null;
            }

            return new PoolThreadCacheL(heapArena, directArena);
        }
    };

    public PooledByteBufAllocatorL() {
        this(false);
    }

    public PooledByteBufAllocatorL(boolean preferDirect) {
        this(preferDirect, DEFAULT_NUM_HEAP_ARENA, DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER);
    }

    public PooledByteBufAllocatorL(int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
        this(false, nHeapArena, nDirectArena, pageSize, maxOrder);
    }

    public PooledByteBufAllocatorL(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
        super(preferDirect);

        final int chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

        if (nHeapArena < 0) {
            throw new IllegalArgumentException("nHeapArena: " + nHeapArena + " (expected: >= 0)");
        }
        if (nDirectArena < 0) {
            throw new IllegalArgumentException("nDirectArea: " + nDirectArena + " (expected: >= 0)");
        }

        int pageShifts = validateAndCalculatePageShifts(pageSize);

        if (nHeapArena > 0) {
            heapArenas = newArenaArray(nHeapArena);
            for (int i = 0; i < heapArenas.length; i ++) {
                heapArenas[i] = new PoolArenaL.HeapArena(this, pageSize, maxOrder, pageShifts, chunkSize);
            }
        } else {
            heapArenas = null;
        }

        if (nDirectArena > 0) {
            directArenas = newArenaArray(nDirectArena);
            for (int i = 0; i < directArenas.length; i ++) {
                directArenas[i] = new PoolArenaL.DirectArena(this, pageSize, maxOrder, pageShifts, chunkSize);
            }
        } else {
            directArenas = null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> PoolArenaL<T>[] newArenaArray(int size) {
        return new PoolArenaL[size];
    }

    private static int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: 4096+)");
        }

        // Ensure pageSize is power of 2.
        boolean found1 = false;
        int pageShifts = 0;
        for (int i = pageSize; i != 0 ; i >>= 1) {
            if ((i & 1) != 0) {
                if (!found1) {
                    found1 = true;
                } else {
                    throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2");
                }
            } else {
                if (!found1) {
                    pageShifts ++;
                }
            }
        }
        return pageShifts;
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i --) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        PoolThreadCacheL cache = threadCache.get();
        PoolArenaL<byte[]> heapArena = cache.heapArena;
        if (heapArena != null) {
            return heapArena.allocate(cache, initialCapacity, maxCapacity);
        } else {
          throw new UnsupportedOperationException();
        }
    }

    
    /**
     * Override the abstract allocator. Normally, the abstract allocator
     * defaults the second parameter to MAXINT as a "sanity check", but we
     * have reinterpreted the second parameter as "max requested".
     */
    @Override
    public ByteBuf directBuffer(int request) {
    	return directBuffer(request, request);
    }
    
    
    
    /**
     * Allocate a buffer from the current thread's direct arena.
     */
    @Override
    protected ByteBuf newDirectBuffer(int minRequested, int maxRequested) {
        PoolThreadCacheL cache = threadCache.get();
        PoolArenaL<ByteBuffer> directArena = cache.directArena;
        if (directArena != null) {
            return directArena.allocate(cache, minRequested, maxRequested);
        } else {
            if (PlatformDependent.hasUnsafe()) {
              throw new UnsupportedOperationException();
//                return new UnpooledUnsafeDirectByteBuf(this, initialCapacity, maxCapacity);
            } else {
              throw new UnsupportedOperationException();
//                return new UnpooledDirectByteBuf(this, initialCapacity, maxCapacity);
            }
        }
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(heapArenas.length);
        buf.append(" heap arena(s):");
        buf.append(StringUtil.NEWLINE);
        for (PoolArenaL<byte[]> a: heapArenas) {
            buf.append(a);
        }
        buf.append(directArenas.length);
        buf.append(" direct arena(s):");
        buf.append(StringUtil.NEWLINE);
        for (PoolArenaL<ByteBuffer> a: directArenas) {
            buf.append(a);
        }
        return buf.toString();
    }
}
