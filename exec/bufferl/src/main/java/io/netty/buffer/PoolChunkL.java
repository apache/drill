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


/** 
 * A Chunk is a large, fixed size piece of memory allocated from the operating system. 
 * This PoolChunk allocator divides a chunk into a run of pages using the buddy system.
 * The actual allocation will be the size requested, rounded up to the next 
 * power-of-2.
 * 
 * This allocator does "normal" allocations, where the requested size varies
 *    from page size to chunk size.
 * 
 * The allocator is based on buddy system. It views a chunk as a binary tree with
 *     1 run of chunksize, or
 *     2 runs of chunksize/2, or
 *     4 runs of chunksize/4, or
 *     ...
 *     2^maxOrder runs of pagesize
 *     
 * Each node in the binary tree is labeled:
 *     - unused.  The node and all its children are unallocated
 *     - allocated. The node (and all its children) are allocated
 *                  as a single request. The children remain marked "unused".
 *     - branch. At least one descendent is allocated.
 *     
 * The binary tree is represented in the "memoryMap",
 *   which saves the node status in a simple array without using links.
 *   The array indices indicate the position within the tree as follows:
 *     0 - unused
 *     1 - root
 *     2,3 - children of 1
 *     4,5  6,7  - children of 2 and 3
 *     8,9 10,11   12,13 14,15     - next level of children.
 *     
 * Thus, i/2 points to the parent of i,  
 *       i*2 points to left child, i*2+1 points to right child.    
 *
 * Note the current code also deals with smaller subpage allocations.
 *    The overall memory manager only comes here when it wants a new page,
 *    not every time it allocates a subpage piece of memory.
 */
final class PoolChunkL<T> {
    private static final int ST_UNUSED = 0;
    private static final int ST_BRANCH = 1;
    private static final int ST_ALLOCATED = 2;
    private static final int ST_ALLOCATED_SUBPAGE = ST_ALLOCATED | 1;

    private static final long multiplier = 0x5DEECE66DL;
    private static final long addend = 0xBL;
    private static final long mask = (1L << 48) - 1;

    final PoolArenaL<T> arena;
    final T memory;
    final boolean unpooled;

    private final int[] memoryMap;
    private final PoolSubpageL<T>[] subpages;
    /** Used to determine if the requested capacity is equal to or greater than pageSize. */
    private final int subpageOverflowMask;
    private final int pageSize;
    private final int pageShifts;

    private final int chunkSize;
    private final int maxSubpageAllocs;

    private long random = (System.nanoTime() ^ multiplier) & mask;

    private int freeBytes;

    PoolChunkListL<T> parent;
    PoolChunkL<T> prev;
    PoolChunkL<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Create a new "chunk" of memory to be used in the given arena.
     *
     * @param arena - the arena this chunk belongs to
     * @param memory
     * @param pageSize - the size of a page (known to arena, why here?)
     * @param maxOrder - how many pages to a chunk  (2^^maxOrder pages)
     * @param pageShifts - page size as number of shifts  (2^^pageShifts)
     * @param chunkSize - the size of a chunk   (pagesize*2^^maxOrder(known to arena)
     */
    PoolChunkL(PoolArenaL<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        unpooled = false;
        this.arena = arena;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        subpageOverflowMask = ~(pageSize - 1);
        freeBytes = chunkSize;

        int chunkSizeInPages = chunkSize >>> pageShifts;
        maxSubpageAllocs = 1 << maxOrder;

        // Generate the memory map.
        memoryMap = new int[maxSubpageAllocs << 1];
        int memoryMapIndex = 1;
        for (int i = 0; i <= maxOrder; i ++) {
            int runSizeInPages = chunkSizeInPages >>> i;
            for (int j = 0; j < chunkSizeInPages; j += runSizeInPages) {
                //noinspection PointlessBitwiseExpression
                memoryMap[memoryMapIndex ++] = j << 17 | runSizeInPages << 2 | ST_UNUSED;
            }
        }

        subpages = newSubpageArray(maxSubpageAllocs);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunkL(PoolArenaL<T> arena, T memory, int size) {
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        memoryMap = null;
        subpages = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        chunkSize = size;
        maxSubpageAllocs = 0;
    }

    /** Creates an uninitialized array of subpage lists */
    @SuppressWarnings("unchecked")
    private PoolSubpageL<T>[] newSubpageArray(int size) {
        return new PoolSubpageL[size];
    }

    
    /** returns the percentage of the chunk which has been allocated */
    int usage() {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    
    /** 
     * Allocates a buffer of the given size from current chunk
     * @param capacity - requested capacity of the buffer
     * @return - handle to the buffer, -1 if failed
     */
    long allocate(int capacity) {
    	return allocate(capacity, capacity);
    }
    
    
    /**
     * Allocates a buffer with size between minRequested and maxRequested
     * @param minRequested
     * @param maxRequested
     * @return
     */
    long allocate(int minRequested, int maxRequested) {
    	
    	// CASE: allocating runs of pages, make use of maxCapacity since we can trim it later
    	if (maxRequested > pageSize/2)   {
    		return allocateRun(minRequested, maxRequested, 1, chunkSize);
    	}
    	
    	// OTHERWISE: allocating subpage buffer. Special case: maxCapacity is normCapacity.
    	//   Note: this case should be moved to PoolArena.
    	else {
    		return allocateSubpage(maxRequested, 1, memoryMap[1]);
    	}
    }

    
    
    /**
     * Allocate a run of pages where the run size is within minCapacity thru maxCapacity.
     * @param minRequested - the minimum size of the buffer
     * @param maxRequested - the maximum size of the buffer to be allocated if convenient
     * @param node - the subtree of this chunk to search
     * @return handle to the allocated memory
     * 
     * More specifically, this routine finds an unused node in the binary tree,
     *   s.t.  the node is big enough to contain minCapacity, and is not
     *         bigger than the size to contain maxCapacity.
     *         
     * A node is the correct size to contain x bytes, if
     *      runlength(node) == roundup-power-of-2(x)
     *  equivalently, runlength(node) >= x  and   runlength(node.child) < x
     *  equivalently, runlength(node) >= x  and   runlength(node)/2 < x  
     *  
     * Similarly, a node is the correct size to contain min...max bytes, if
     *    runlength(node) >= roundup-power-of-2(min)  &&  runlength(node) <= roundup-power-of-2(max)
     *    or equivalently,    runlength(node) >= min  &&  runlength(node)/2 < max   
     */
    long allocateRun(int minRequested, int maxRequested, int node, int runLength) {
    	
    	// Search through the subtrees until finding an unused node s.t. runlength >= min
    	for (; runLength >= minRequested; runLength /= 2) {
    		if ((memoryMap[node]&3) != ST_BRANCH) break;
    		
            // Search one random subtree (recursively)
    		int child = node*2 + nextRandom();
    		long handle = allocateRun(minRequested, maxRequested, child, runLength/2);
    		if (handle != -1) return handle;
    			
    		// If not found, search the other subtree (tail recursion) 
    		node = child ^ 1;
    	}
    		
    	// if we failed to find an unused node which is big enough, then failure.
    	if (runLength < minRequested || (memoryMap[node]&3) != ST_UNUSED) {
    		return -1;
    	}
    	
    	// At this point, we have an unused node s.t.  runlength >= min.
    	//   In other words, it is larger than the minimum, but it may also be larger
    	//   than the maximum. 
    	
    	// Continue descending subtree looking for a node s.t. runlength/2 < max
        for (; runLength/2 >= maxRequested; runLength/=2) {
        	
        	// We are about to allocate from one of our children, so we become BRANCH
        	memoryMap[node] = (memoryMap[node]&~3) | ST_BRANCH;
        	
        	// Pick one of the children and continue descending its subtree.
        	node = node * 2 + nextRandom();
        }
    	
    	// We are at an unused node which satisfies both conditions. Allocate it.
        memoryMap[node] = (memoryMap[node]&~3) | ST_ALLOCATED;
        freeBytes -= runLength;
        return node;
    }
    
    
    
    
    
    /**
     * Allocate a new page for splitting into subpage items.
     *   Note: this routine doesn't belong here. Instead, we should have a "subpage" allocator 
     *   which is invoked instead of us and is responsible for the entire subpage allocation sizes.
     * @param normCapacity - the actual size of the buffer we will allocate
     * @param curIdxn - the node where our search stargs
     * @param val - contents of the current node
     * @return a handle to the allocated buffer.
     */
    private long allocateSubpage(int normCapacity, int curIdx, int val) {
        int state = val & 3;
        if (state == ST_BRANCH) {
            int nextIdx = curIdx << 1 ^ nextRandom();
            long res = branchSubpage(normCapacity, nextIdx);
            if (res > 0) {
                return res;
            }

            return branchSubpage(normCapacity, nextIdx ^ 1);
        }

        if (state == ST_UNUSED) {
            return allocateSubpageSimple(normCapacity, curIdx, val);
        }

        if (state == ST_ALLOCATED_SUBPAGE) {
            PoolSubpageL<T> subpage = subpages[subpageIdx(curIdx)];
            int elemSize = subpage.elemSize;
            if (normCapacity != elemSize) {
                return -1;
            }

            return subpage.allocate();
        }

        return -1;
    }

    
    
    /**
     * Allocate a page to be used for subpage buffers, knowing the subtree is UNUSED
     * @param normCapacity
     * @param curIdx
     * @param val
     * @return
     */
    private long allocateSubpageSimple(int normCapacity, int curIdx, int val) {
        int runLength = runLength(val);
        for (;;) {
            if (runLength == pageSize) {
                memoryMap[curIdx] = val & ~3 | ST_ALLOCATED_SUBPAGE;
                freeBytes -= runLength;

                int subpageIdx = subpageIdx(curIdx);
                PoolSubpageL<T> subpage = subpages[subpageIdx];
                if (subpage == null) {
                    subpage = new PoolSubpageL<T>(this, curIdx, runOffset(val), pageSize, normCapacity);
                    subpages[subpageIdx] = subpage;
                } else {
                    subpage.init(normCapacity);
                }
                return subpage.allocate();
            }

            int nextIdx = curIdx << 1 ^ nextRandom();
            int unusedIdx = nextIdx ^ 1;

            memoryMap[curIdx] = val & ~3 | ST_BRANCH;
            //noinspection PointlessBitwiseExpression
            memoryMap[unusedIdx] = memoryMap[unusedIdx] & ~3 | ST_UNUSED;

            runLength >>>= 1;
            curIdx = nextIdx;
            val = memoryMap[curIdx];
        }
    }

    private long branchSubpage(int normCapacity, int nextIdx) {
        int nextVal = memoryMap[nextIdx];
        if ((nextVal & 3) != ST_ALLOCATED) {
            return allocateSubpage(normCapacity, nextIdx, nextVal);
        }
        return -1;
    }
    
    /**
     * Free up memory to reduce the size of a run of pages.
     *     The resulting run starts on the same page, and
     *     the trailing pages are returned to the memory manager.
     *     Trim is intended to be an efficient way to reduce the size of a buffer.
     *     No new memory is allocated, nor is any data copied.
     * @param handle - which run of pages was allocated from the current chunk
     * @param smallerSize - the new desired size the new run of pages
     * @return a new handle to the smaller run of pages, or -1 if can't trim.
     */
    long trim(long handle, int smallerSize) {
    	int memoryMapIdx = (int) handle;
    	int bitmapIx = (int)(handle >>> 32);
    	int originalRunLength = runLength(memoryMap[memoryMapIdx]);
    	
    	// If the buffer was a HUGE allocation, then we leave it alone
    	if (this.unpooled || memoryMapIdx == 0 || handle < 0) {
    		return -1;
    	}
    	
    	// If the buffer was a subpage, then we also leave it alone.
    	if (bitmapIx != 0 || (memoryMap[memoryMapIdx] & 3) == ST_ALLOCATED_SUBPAGE) {
    		return -1;
    	}
 
    	// We can't trim if the result will become a subpage
    	if (smallerSize <= pageSize/2) {
    		return -1;
    	}
       	
    	// If the buffer is growing, then we aren't really trimming.
    	if (smallerSize >= originalRunLength) {
    		return -1;
    	}
    	
    	// Starting at current node, follow left hand children, until reaching node of desired size.
    	//   Note that runLength and memoryMapIdx move in unison. 
    	int runLength;
    	for (runLength = originalRunLength;  smallerSize*2 <= runLength;  runLength /= 2, memoryMapIdx = memoryMapIdx<<1) {
    		
    		// Current node is a parent of the desired node. It now becomes a "BRANCH".
    		memoryMap[memoryMapIdx] = (memoryMap[memoryMapIdx] & ~3) | ST_BRANCH;
    		
    		// Right hand child is now an unused buddy. Mark it "UNUSED".
    		//  (done - should already be marked "UNUSED")
    	}
    	
    	// We are now at the desired node. Mark it allocated.
    	memoryMap[memoryMapIdx] = (memoryMap[memoryMapIdx] & ~3) | ST_ALLOCATED;
    	freeBytes += (originalRunLength - runLength);
    	
    	// return new handle to the reduced size buffer.
    	return memoryMapIdx;
    }

    /**
     * Return a buffer back to the memory pool.
     * @param handle
     */
    void free(long handle) {
        int memoryMapIdx = (int) handle;
        int bitmapIdx = (int) (handle >>> 32);

        // If buffer was allocated from a subpage, then free it.
        int val = memoryMap[memoryMapIdx];
        int state = val & 3;
        if (state == ST_ALLOCATED_SUBPAGE) {
            assert bitmapIdx != 0;
            PoolSubpageL<T> subpage = subpages[subpageIdx(memoryMapIdx)];
            assert subpage != null && subpage.doNotDestroy;
            if (subpage.free(bitmapIdx & 0x3FFFFFFF)) {
                return;
            }
            
        // Otherwise, it should have been allocated from the chunk    
        } else {
            assert state == ST_ALLOCATED : "state: " + state;
            assert bitmapIdx == 0;
        }

        // Update the nr of bytes free
        freeBytes += runLength(val);

        // start at current node and work up the tree
        for (;;) {
        	
            // Mark the node as "unused"
            memoryMap[memoryMapIdx] = val & ~3 | ST_UNUSED;
            
            // If at top of tree, done
            if (memoryMapIdx == 1) {
                assert freeBytes == chunkSize;
                return;
            }

            // If the buddy is allocated, we can stop since no more merging can occur
            if ((memoryMap[siblingIdx(memoryMapIdx)] & 3) != ST_UNUSED) {
                break;
            }

            // move to current node's parent, effectively merging with UNUSED buddy
            memoryMapIdx = parentIdx(memoryMapIdx);
            val = memoryMap[memoryMapIdx];
        }
    }

    void initBuf(PooledByteBufL<T> buf, long handle, int reqCapacity) {
    	initBuf(buf, handle, reqCapacity, reqCapacity);
    }
    
    /**
     * Initialize a buffer given a handle that was allocated from this chunk.
     * @param buf       The buffer to be initialized.
     * @param handle    The handle representing memory allocated from this chunk.
     * @param minRequested  The minimum requested capacity.
     * @param maxRequested   The maximum requested capacity.
     */
    void initBuf(PooledByteBufL<T> buf, long handle, int minRequested, int maxRequested) {
        int memoryMapIdx = (int) handle;
        int bitmapIdx = (int) (handle >>> 32);
        
        // If this is a normal allocation
        if (bitmapIdx == 0) {
        	
        	// Verify the memory is allocated
            int val = memoryMap[memoryMapIdx];
            assert (val & 3) == ST_ALLOCATED : String.valueOf(val & 3);
            
            // Initialize buffer with as large as possible capacity within the requested range
            //  (assert: we know the buffer is >= minCapacity)
            int capacity = Math.min(runLength(val), maxRequested);         
            buf.init(this, handle, runOffset(val), capacity, runLength(val));
            
        // Otherwise initialize buffer as a subpage allocation.  
        } else {
            initBufWithSubpage(buf, handle, bitmapIdx, minRequested);
        }
    }

    void initBufWithSubpage(PooledByteBufL<T> buf, long handle, int reqCapacity) {
        initBufWithSubpage(buf, handle, (int) (handle >>> 32), reqCapacity);
    }

    private void initBufWithSubpage(PooledByteBufL<T> buf, long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;

        int memoryMapIdx = (int) handle;
        int val = memoryMap[memoryMapIdx];
        assert (val & 3) == ST_ALLOCATED_SUBPAGE;

        PoolSubpageL<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;

        buf.init(
                this, handle,
                runOffset(val) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize, reqCapacity, subpage.elemSize);
    }

    private static int parentIdx(int memoryMapIdx) {
        return memoryMapIdx >>> 1;
    }

    private static int siblingIdx(int memoryMapIdx) {
        return memoryMapIdx ^ 1;
    }

    private int runLength(int val) {
        return (val >>> 2 & 0x7FFF) << pageShifts;
    }

    private int runOffset(int val) {
        return val >>> 17 << pageShifts;
    }

    private int subpageIdx(int memoryMapIdx) {
        return memoryMapIdx - maxSubpageAllocs;
    }

    private int nextRandom() {
        random = random * multiplier + addend & mask;
        return (int) (random >>> 47) & 1;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Chunk(");
        buf.append(Integer.toHexString(System.identityHashCode(this)));
        buf.append(": ");
        buf.append(usage());
        buf.append("%, ");
        buf.append(chunkSize - freeBytes);
        buf.append('/');
        buf.append(chunkSize);
        buf.append(')');
        return buf.toString();
    }

	public int getPageSize() {
		return pageSize;
	}
}
