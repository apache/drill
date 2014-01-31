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


/**
 * A list of chunks with similar "usage".  If a chunk is added to the wrong list,
 *    it will migrate to the next list which hopefully will be the correct one.
 *
 * @param <T>
 */
final class PoolChunkListL<T> {
    private final PoolArenaL<T> arena;
    private final PoolChunkListL<T> nextList;
    PoolChunkListL<T> prevList;

    private final int minUsage;
    private final int maxUsage;

    private PoolChunkL<T> head;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * Create a new, empty pool of chunks.
     * @param arena - the bigger arena this pool belongs to
     * @param nextList - the next list to consider (in the same pool)
     * @param minUsage - contains chunks with the specified usage (min ... max)
     * @param maxUsage
     */
    PoolChunkListL(PoolArenaL<T> arena, PoolChunkListL<T> nextList, int minUsage, int maxUsage) {
        this.arena = arena;
        this.nextList = nextList;
        this.minUsage = minUsage;
        this.maxUsage = maxUsage;
    }

    /**
     * Allocate a buffer with the requested size
     * @param buf - the container to hold the buffer
     * @param minRequested - the minumum requested capacity
     * @param maxRequested - the maximum capacity
     * @return
     */
    boolean allocate(PooledByteBufL<T> buf, int minRequested, int maxRequested) {
    	
    	// If list is empty, then allocation fails
        if (head == null) {
            return false;
        }

        // Do for each chunk in the list
        for (PoolChunkL<T> cur = head;;) {
        	
        	// If we successfully allocated from the chunk ...
            long handle = cur.allocate(minRequested, maxRequested);
            if (handle < 0) {
                cur = cur.next;
                if (cur == null) {
                    return false;
                }
                
            // ... then add the memory to the buffer container
            } else {
                cur.initBuf(buf, handle, minRequested, maxRequested);
                
                // If usage changed, then move to next list
                if (cur.usage() >= maxUsage) {
                    remove(cur);
                    nextList.add(cur);
                }
                return true;
            }
        }
    }

    
    /**
     * Release a buffer back to the original chunk.
     * @param chunk
     * @param handle
     */
    void free(PoolChunkL<T> chunk, long handle) {
    	
    	// Release buffer back to the original chunk
        chunk.free(handle);
        
        // If usage changed, then move to different list
        if (chunk.usage() < minUsage) {
            remove(chunk);
            if (prevList == null) {
                assert chunk.usage() == 0;
                arena.destroyChunk(chunk);
            } else {
                prevList.add(chunk);
            }
        }
    }
    
    /**
     * Shrink the buffer down to the specified size, freeing up unused memory.
     * @param chunk - chunk the buffer resides in
     * @param handle - the handle to the buffer
     * @param size - the new desired "size"
     * @return a new handle to the smaller buffer
     */
    long trim(PoolChunkL<T> chunk, long handle, int size) {
    	
    	// Trim the buffer, possibly getting a new handle.
    	handle = chunk.trim(handle,  size);
    	if (handle == -1) return handle;
    	
    	// Move the chunk to a different list if usage changed significantly
    	if (chunk.usage() < minUsage) {
    		assert chunk.usage() > 0 && prevList != null;
    		remove(chunk);
    		prevList.add(chunk);
    	}
    	
    	// return new handle for the smaller buffer
    	return handle;
    }

    
    /**
     * Add a chunk to the current chunklist
     * @param chunk
     */
    void add(PoolChunkL<T> chunk) {
    	
    	// If usage has change, then add to the neighboring list instead
    	int usage = chunk.usage();
        if (usage >= maxUsage) {
            nextList.add(chunk);
            return;
        //} else if (usage < minUsage) {   // TODO: Could this result in a recursive loop?
        //	prevList.add(chunk);
        //	return;
        }

        // Add chunk to linked list.
        chunk.parent = this;
        if (head == null) {
            head = chunk;
            chunk.prev = null;
            chunk.next = null;
        } else {
            chunk.prev = null;
            chunk.next = head;
            head.prev = chunk;
            head = chunk;
        }
    }

    
    /**
     * Remove a chunk from the current linked list of chunks
     * @param cur
     */
    private void remove(PoolChunkL<T> cur) {
        if (cur == head) {
            head = cur.next;
            if (head != null) {
                head.prev = null;
            }
        } else {
            PoolChunkL<T> next = cur.next;
            cur.prev.next = next;
            if (next != null) {
                next.prev = cur.prev;
            }
        }
    }

    @Override
    public String toString() {
        if (head == null) {
            return "none";
        }

        StringBuilder buf = new StringBuilder();
        for (PoolChunkL<T> cur = head;;) {
            buf.append(cur);
            cur = cur.next;
            if (cur == null) {
                break;
            }
            buf.append(StringUtil.NEWLINE);
        }

        return buf.toString();
    }
}
