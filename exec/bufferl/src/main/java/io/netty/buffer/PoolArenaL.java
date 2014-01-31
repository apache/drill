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

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;

import java.nio.ByteBuffer;


/**
 * A PoolArenaL is a single arena for managing memory. 
 * It represents a composite memory manager which consists of other 
 * memory allocators, where each of the allocators
 * works best for different allocation sizes.
 * 
 * More specifically, a PoolArenaL includes the following allocators:
 *   tiny: (16-256) - multiple lists of pages of items, where each list has items of a fixed size 16*N.
 *   small: (512-pageSize/2)  - multiple lists of pages of items, where each list has items of a fixed size 2^N.
 *   normal: (pageSize-chunkSize) - a skiplist of chunks ordered to minimize fragmentation, where
 *                                  each chunk is divided into pages using the buddy system.
 *   huge: (>chunkSize)  - memory is allocated directly from the operating system.
 *   
 * Note: In a multi-threaded environment, the "parent" creates multiple "arenas",
 * distributing threads among them to minimize contention.
 *  
 * @param <T>
 */
abstract class PoolArenaL<T> {

    final PooledByteBufAllocatorL parent;

    private final int pageSize;
    private final int maxOrder;
    private final int pageShifts;
    private final int chunkSize;
    private final int subpageOverflowMask;

    private final PoolSubpageL<T>[] tinySubpagePools;
    private final PoolSubpageL<T>[] smallSubpagePools;

    private final PoolChunkListL<T> q050;
    private final PoolChunkListL<T> q025;
    private final PoolChunkListL<T> q000;
    private final PoolChunkListL<T> qInit;
    private final PoolChunkListL<T> q075;
    private final PoolChunkListL<T> q100;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * Create a new arena.
     * @param parent - The global memory manager (where we go to allocate a completely new buffer).
     * @param pageSize - The minimum size of memory for using the "normal" allocation (buddy system).
     * @param maxOrder - The size of a "chunk", where chunkSize = pageSize * 2^maxOrder.
     * @param pageShifts - pageSize expressed as a power of 2.  (redundant?)
     * @param chunkSize - chunkSize in bytes. (redundant?)
     */
    protected PoolArenaL(PooledByteBufAllocatorL parent, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        this.parent = parent;
        this.pageSize = pageSize;
        this.maxOrder = maxOrder;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        subpageOverflowMask = ~(pageSize - 1);

        // Create the tiny pools, ranging in size from 16 to 256 in steps of 16.
        tinySubpagePools = newSubpagePoolArray(512 >>> 4);
        for (int i = 0; i < tinySubpagePools.length; i ++) {
            tinySubpagePools[i] = newSubpagePoolHead(pageSize);
        }

        // Create the small pools, ranging in size from 512 to pagesize/2 as powers of 2.
        smallSubpagePools = newSubpagePoolArray(pageShifts - 9);
        for (int i = 0; i < smallSubpagePools.length; i ++) {
            smallSubpagePools[i] = newSubpagePoolHead(pageSize);
        }

        // Create a skip list of chunks consisting of separate lists connected together.
        //   Chunks migrate between the lists depending on how much free space they have.
        q100 = new PoolChunkListL<T>(this, null, 100, Integer.MAX_VALUE);
        q075 = new PoolChunkListL<T>(this, q100, 75, 100);
        q050 = new PoolChunkListL<T>(this, q075, 50, 100);
        q025 = new PoolChunkListL<T>(this, q050, 25, 75);
        q000 = new PoolChunkListL<T>(this, q025, 1, 50);
        qInit = new PoolChunkListL<T>(this, q000, Integer.MIN_VALUE, 25);
        q100.prevList = q075;
        q075.prevList = q050;
        q050.prevList = q025;
        q025.prevList = q000;
        q000.prevList = null;
        qInit.prevList = qInit;
    }

    /** Initialize a subpage list */
    private PoolSubpageL<T> newSubpagePoolHead(int pageSize) {
        PoolSubpageL<T> head = new PoolSubpageL<T>(pageSize);
        head.prev = head;
        head.next = head;
        return head;
    }

    /** Create an array (uninitialized) of subpage lists.*/
    @SuppressWarnings("unchecked")
    private PoolSubpageL<T>[] newSubpagePoolArray(int size) {
        return new PoolSubpageL[size];
    }

    
    /**
     * Allocate a buffer from the current arena.
     * Unlike netty.io buffers, this buffer can grow without bounds,
     * but it will throw an exception if growth involves copying a page 
     * or more of data. Instead of being an upper bounds sanity check,
     * the "max" capacity is used to opportunistically allocate extra memory.
     * Later, the capacity can be reduced very efficiently.
     * To avoid excessive copying, a buffer cannot grow if it must copy
     * more than a single page of data.
     * @param cache   TODO: not sure
     * @param minRequested  The smallest capacity buffer we want
     * @param maxRequested  If convenient, allocate up to this capacity
     * @return A buffer with capacity between min and max capacity
     */
    PooledByteBufL<T> allocate(PoolThreadCacheL cache, int minRequested, int maxRequested) {
    	
    	// Create a buffer header, limiting growth to minimize copying
        PooledByteBufL<T> buf = newByteBuf(Integer.MAX_VALUE);
        allocate(cache, buf, minRequested, maxRequested);
        return buf;
    }

    /**
     * Allocate memory to a buffer container.
     * @param cache TODO: not sure
     * @param buf - A buffer which will contain the allocated memory
     * @param minRequested - The smallest amount of memory.
     * @param maxRequested - The maximum memory to allocate if convenient.
     */
    private void allocate(PoolThreadCacheL cache, PooledByteBufL<T> buf, final int minRequested, int maxRequested) {
    	//   This code should be reorganized.
    	//        case: <= maxTiny:   allocateTiny{select which tiny list, allocate subpage from list.}
    	//        case: <= maxSmall:  allocateSmall{select which small list, allocate subpage from list.}
    	//        case: <= maxNormal: allocateNormal
    	//        otherwise:          allocateHuge
    	//   where maxTiny=256, maxSmall=pageSize/2, maxNormal=chunkSize.
    	
    	// CASE: minCapacity is a subpage
    	final int normCapacity = normalizeCapacity(minRequested);
        if ((normCapacity & subpageOverflowMask) == 0) { // minCapacity <= pageSize/2
            int tableIdx;
            PoolSubpageL<T>[] table;
            
            // if "tiny", pick list based on multiple of 16
            if ((normCapacity & 0xFFFFFE00) == 0) { // minCapacity < 512
                tableIdx = normCapacity >>> 4;
                table = tinySubpagePools;
                
            // else "small", pick list based on power of 2.    
            } else {
                tableIdx = 0;
                int i = normCapacity >>> 10;
                while (i != 0) {
                    i >>>= 1;
                    tableIdx ++;
                }
                table = smallSubpagePools;
            }

            // Whether tiny or small, allocate an item from the first page in the corresponding list
            synchronized (this) {
                final PoolSubpageL<T> head = table[tableIdx];
                final PoolSubpageL<T> s = head.next;
                if (s != head) {
                    assert s.doNotDestroy && s.elemSize == normCapacity;
                    long handle = s.allocate();
                    assert handle >= 0;
                    s.chunk.initBufWithSubpage(buf, handle, minRequested);
                    return;
                }
            }
            
            // If the list was empty, allocate a new page, allocate an item, and add page to the list.
            //   This is awkward. "allocateNormal" does subpage allocation internally,
            //   and it really shouldn't know anything at all about subpages.
            //   Instead, we should allocate a complete page and 
            //   add it to the desired list ourselves.
            allocateNormal(buf, minRequested, normCapacity);
            return;
            
        // CASE:  HUGE allocation.     
        } else if (normCapacity > chunkSize) {
            allocateHuge(buf, minRequested);
            return;
        }

        // OTHERWISE: Normal allocation of pages from a chunk.
        allocateNormal(buf, minRequested, maxRequested);
    }

    
    /**
     * Allocate a "normal" (page .. chunk) sized buffer from a chunk in the skiplist.
     * @param buf - the buffer header which will receive the memory.
     * @param minRequested - the minimum requested capacity in bytes.
     * @param maxRequested - the maximum requested capacity.
     */
    private synchronized void allocateNormal(PooledByteBufL<T> buf, int minRequested, int maxRequested) {
    	
    	// If the buffer can be allocated from the skip list, then allocate it.
        if (q050.allocate(buf, minRequested, maxRequested) || q025.allocate(buf, minRequested, maxRequested) ||
            q000.allocate(buf, minRequested, maxRequested) || qInit.allocate(buf, minRequested, maxRequested) ||
            q075.allocate(buf, minRequested, maxRequested) || q100.allocate(buf, minRequested, maxRequested)) {
            return;
        }

        // Create a new chunk.
        PoolChunkL<T> c = newChunk(pageSize, maxOrder, pageShifts, chunkSize);
        
        // Allocate a buffer from the chunk.
        long handle = c.allocate(minRequested, maxRequested);
        assert handle > 0;
        c.initBuf(buf, handle, minRequested, maxRequested);
        
        // Add the new chunk to the skip list at the "newly initialized" location.
        qInit.add(c);
    }

    
    /**
     * Allocate a huge (>chunksize) buffer.
     * @param buf
     * @param reqCapacity
     */
    private void allocateHuge(PooledByteBufL<T> buf, int reqCapacity) {
        buf.initUnpooled(newUnpooledChunk(reqCapacity), reqCapacity);
    }

    
    /**
     * Free a piece of memory.
     * @param chunk
     * @param handle
     */
    synchronized void free(PoolChunkL<T> chunk, long handle) {
        if (chunk.unpooled) {
            destroyChunk(chunk);
        } else {
            chunk.parent.free(chunk, handle);
        }
    }
    
    
    
    /**
     * Find which list holds subpage buffers of the given size.
     * @param elemSize
     * @return
     */
    PoolSubpageL<T> findSubpagePoolHead(int elemSize) {
        int tableIdx;
        PoolSubpageL<T>[] table;
        if ((elemSize & 0xFFFFFE00) == 0) { // < 512
            tableIdx = elemSize >>> 4;
            table = tinySubpagePools;    
        } else {
            tableIdx = 0;
            elemSize >>>= 10;
            while (elemSize != 0) {
                elemSize >>>= 1;
                tableIdx ++;
            }
            table = smallSubpagePools;
        }

        return table[tableIdx];
    }

    
    
    /**
     * Bump the requested size up to the size which will actually be allocated
     * @param reqCapacity - the requested size
     * @return the large, normalized size
     */
    private int normalizeCapacity(int reqCapacity) {
        if (reqCapacity < 0) {
            throw new IllegalArgumentException("capacity: " + reqCapacity + " (expected: 0+)");
        }
        
        // CASE: HUGE allocation, don't change it.
        if (reqCapacity >= chunkSize) {
            return reqCapacity;
        }

        // CASE: normal or small allocation, then round up to 2^n
        if ((reqCapacity & 0xFFFFFE00) != 0) { // >= 512
            // Doubled

            int normalizedCapacity = reqCapacity;
            normalizedCapacity |= normalizedCapacity >>>  1;
            normalizedCapacity |= normalizedCapacity >>>  2;
            normalizedCapacity |= normalizedCapacity >>>  4;
            normalizedCapacity |= normalizedCapacity >>>  8;
            normalizedCapacity |= normalizedCapacity >>> 16;
            normalizedCapacity ++;

            if (normalizedCapacity < 0) {
                normalizedCapacity >>>= 1;
            }

            return normalizedCapacity;
        }

        // OTHERWISE: tiny allocations. Round up to the next multiple of 16
        if ((reqCapacity & 15) == 0) {
            return reqCapacity;
        }

        return (reqCapacity & ~15) + 16;
    }

    
    /**
     * Change the size of a buffer by allocating new memory and copying the old data to it.
     * @param buf  - the buffer containing the memory
     * @param newCapacity - the desired capacity
     * @param freeOldMemory - whether to release the old memory or not.
     */
    void reallocate(PooledByteBufL<T> buf, int newCapacity, boolean freeOldMemory) {
    	
    	// Sanity check to not grow beyond the maxCapacity.
    	//   This check may not be relevant any more since we have reinterpreted maxCapacity.
        if (newCapacity < 0 || newCapacity > buf.maxCapacity()) {
            throw new IllegalArgumentException("newCapacity: " + newCapacity);
        }

        // Do nothing if capacity doesn't actually change.
        int oldCapacity = buf.length;
        if (oldCapacity == newCapacity) {
            return;
        }

        // Cache some local values to make them more accessible.
        PoolChunkL<T> oldChunk = buf.chunk;
        long oldHandle = buf.handle;
        T oldMemory = buf.memory;
        int oldOffset = buf.offset;
        int readerIndex = buf.readerIndex();
        int writerIndex = buf.writerIndex();

        // Allocate new memory for the buffer
        allocate(parent.threadCache.get(), buf, newCapacity, newCapacity);
        
        // CASE: buffer has grown. Copy data from old buffer to new.
        if (newCapacity > oldCapacity) {
            memoryCopy(
                    oldMemory, oldOffset + readerIndex,
                    buf.memory, buf.offset + readerIndex, writerIndex - readerIndex);
            
        // CASE: buffer has shrunk. Copy data, but also reset the reader/writer positions.    
        } else if (newCapacity < oldCapacity) {
            if (readerIndex < newCapacity) {
                if (writerIndex > newCapacity) {
                    writerIndex = newCapacity;
                }
                memoryCopy(
                        oldMemory, oldOffset + readerIndex,
                        buf.memory, buf.offset + readerIndex, writerIndex - readerIndex);
            } else {
                readerIndex = writerIndex = newCapacity;
            }
        }

        buf.setIndex(readerIndex, writerIndex); // move to buffer has shrunk case?

        // If requested, release the old memory.
        if (freeOldMemory) {
            free(oldChunk, oldHandle);
        }
    }

    /** Create a chunkSize chunk of memory which will be part of the pool */
    protected abstract PoolChunkL<T> newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize);
    
    /** Create an arbitrary size chunk of memory which is not part of the pool. */
    protected abstract PoolChunkL<T> newUnpooledChunk(int capacity);
    
    /** Create a new buffer, but don't allocate memory yet. */
    protected abstract PooledByteBufL<T> newByteBuf(int maxCapacity);
    
    /** Copy memory from one allocation to another. */
    protected abstract void memoryCopy(T src, int srcOffset, T dst, int dstOffset, int length);
    
    /** Release or recycle a chunk of memory */
    protected abstract void destroyChunk(PoolChunkL<T> chunk);

    /** Display the contents of a PoolArena */
    public synchronized String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Chunk(s) at 0~25%:");
        buf.append(StringUtil.NEWLINE);
        buf.append(qInit);
        buf.append(StringUtil.NEWLINE);
        buf.append("Chunk(s) at 0~50%:");
        buf.append(StringUtil.NEWLINE);
        buf.append(q000);
        buf.append(StringUtil.NEWLINE);
        buf.append("Chunk(s) at 25~75%:");
        buf.append(StringUtil.NEWLINE);
        buf.append(q025);
        buf.append(StringUtil.NEWLINE);
        buf.append("Chunk(s) at 50~100%:");
        buf.append(StringUtil.NEWLINE);
        buf.append(q050);
        buf.append(StringUtil.NEWLINE);
        buf.append("Chunk(s) at 75~100%:");
        buf.append(StringUtil.NEWLINE);
        buf.append(q075);
        buf.append(StringUtil.NEWLINE);
        buf.append("Chunk(s) at 100%:");
        buf.append(StringUtil.NEWLINE);
        buf.append(q100);
        buf.append(StringUtil.NEWLINE);
        buf.append("tiny subpages:");
        for (int i = 1; i < tinySubpagePools.length; i ++) {
            PoolSubpageL<T> head = tinySubpagePools[i];
            if (head.next == head) {
                continue;
            }

            buf.append(StringUtil.NEWLINE);
            buf.append(i);
            buf.append(": ");
            PoolSubpageL<T> s = head.next;
            for (;;) {
                buf.append(s);
                s = s.next;
                if (s == head) {
                    break;
                }
            }
        }
        buf.append(StringUtil.NEWLINE);
        buf.append("small subpages:");
        for (int i = 0; i < smallSubpagePools.length; i ++) {
            PoolSubpageL<T> head = smallSubpagePools[i];
            if (head.next == head) {
                continue;
            }

            buf.append(StringUtil.NEWLINE);
            buf.append(i);
            buf.append(": ");
            PoolSubpageL<T> s = head.next;
            for (;;) {
                buf.append(s);
                s = s.next;
                if (s == head) {
                    break;
                }
            }
        }
        buf.append(StringUtil.NEWLINE);

        return buf.toString();
    }

    
    /** HeapArena is an arena which allocates memory from the Java Heap */
    static final class HeapArena extends PoolArenaL<byte[]> {

        HeapArena(PooledByteBufAllocatorL parent, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
            super(parent, pageSize, maxOrder, pageShifts, chunkSize);
        }

        @Override
        protected PoolChunkL<byte[]> newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize) {
            return new PoolChunkL<byte[]>(this, new byte[chunkSize], pageSize, maxOrder, pageShifts, chunkSize);
        }

        @Override
        protected PoolChunkL<byte[]> newUnpooledChunk(int capacity) {
            return new PoolChunkL<byte[]>(this, new byte[capacity], capacity);
        }

        @Override
        protected void destroyChunk(PoolChunkL<byte[]> chunk) {
            // Rely on GC.
        }

        @Override
        protected PooledByteBufL<byte[]> newByteBuf(int maxCapacity) {
            return PooledHeapByteBufL.newInstance(maxCapacity);
        }

        @Override
        protected void memoryCopy(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
            if (length == 0) {
                return;
            }

            System.arraycopy(src, srcOffset, dst, dstOffset, length);
        }
    }

    
    /** DirectArena is an arena which allocates memory from off-heap (direct allocation).*/
    static final class DirectArena extends PoolArenaL<ByteBuffer> {

        private static final boolean HAS_UNSAFE = PlatformDependent.hasUnsafe();

        DirectArena(PooledByteBufAllocatorL parent, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
            super(parent, pageSize, maxOrder, pageShifts, chunkSize);
        }

        @Override
        protected PoolChunkL<ByteBuffer> newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize) {
            return new PoolChunkL<ByteBuffer>(
                    this, ByteBuffer.allocateDirect(chunkSize), pageSize, maxOrder, pageShifts, chunkSize);
        }

        @Override
        protected PoolChunkL<ByteBuffer> newUnpooledChunk(int capacity) {
            return new PoolChunkL<ByteBuffer>(this, ByteBuffer.allocateDirect(capacity), capacity);
        }

        @Override
        protected void destroyChunk(PoolChunkL<ByteBuffer> chunk) {
            PlatformDependent.freeDirectBuffer(chunk.memory);
        }

        @Override
        protected PooledByteBufL<ByteBuffer> newByteBuf(int maxCapacity) {
            if (HAS_UNSAFE) {
                return PooledUnsafeDirectByteBufL.newInstance(maxCapacity);
            } else {
              throw new UnsupportedOperationException();
//                return PooledDirectByteBufL.newInstance(maxCapacity);
            }
        }

        @Override
        protected void memoryCopy(ByteBuffer src, int srcOffset, ByteBuffer dst, int dstOffset, int length) {
            if (length == 0) {
                return;
            }

            if (HAS_UNSAFE) {
                PlatformDependent.copyMemory(
                        PlatformDependent.directBufferAddress(src) + srcOffset,
                        PlatformDependent.directBufferAddress(dst) + dstOffset, length);
            } else {
                // We must duplicate the NIO buffers because they may be accessed by other Netty buffers.
                src = src.duplicate();
                dst = dst.duplicate();
                src.position(srcOffset).limit(srcOffset + length);
                dst.position(dstOffset);
                dst.put(src);
            }
        }
    }
}
