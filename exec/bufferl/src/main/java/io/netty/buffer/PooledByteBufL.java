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

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.ResourceLeak;
import io.netty.util.ResourceLeakDetector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A ByteBuf optimized to work with little endian direct buffers
 * and the netty pool allocator. 
 * The buffer can be of any size - tiny, small, normal, or huge.
 * The class contains all information needed to free the memory
 *   (which chunk, which pages, which elements).
 *
 * @param <T>
 */

abstract class PooledByteBufL<T> extends AbstractReferenceCountedByteBuf {

    private final ResourceLeak leak;
    private final Recycler.Handle recyclerHandle;

    protected PoolChunkL<T> chunk;
    protected long handle;
    protected T memory;
    protected int offset;
    protected int length;
    private int maxLength;

    private ByteBuffer tmpNioBuf;

    protected PooledByteBufL(Recycler.Handle recyclerHandle, int maxCapacity) {
        super(maxCapacity);
        leak = leakDetector.open(this);
        this.recyclerHandle = recyclerHandle;
    }

    /**
     * Initialize a new buffer for "normal" allocations.
     * @param chunk - which chunk the buffer came from
     * @param handle - which pages within the chunk
     * @param offset - byte offset to the first page
     * @param length - the requested length
     * @param maxLength - the max limit for resizing.
     */
    void init(PoolChunkL<T> chunk, long handle, int offset, int length, int maxLength) {
        assert handle >= 0;
        assert chunk != null;

        this.chunk = chunk;
        this.handle = handle;
        memory = chunk.memory;
        this.offset = offset;
        this.length = length;
        this.maxLength = maxLength;
        setIndex(0, 0);
        tmpNioBuf = null;
    }

    void initUnpooled(PoolChunkL<T> chunk, int length) {
        assert chunk != null;

        this.chunk = chunk;
        handle = 0;
        memory = chunk.memory;
        offset = 0;
        this.length = maxLength = length;
        setIndex(0, 0);
        tmpNioBuf = null;
    }

    @Override
    public final int capacity() {
        return length;
    }

    
    /**
     * Change the size of an allocated buffer, reallocating if appropriate.
     * @param newCapacity
     * @return
     */
    @Override
    public final ByteBuf capacity(int newCapacity) {
        ensureAccessible();

        // Check for the easy resizing cases, and return if successfully resized.
        if (chunk.unpooled) {
            if (newCapacity == length) {
                return this;
            }
        } else {
            if (newCapacity > length) {
                if (newCapacity <= maxLength) {
                    length = newCapacity;
                    return this;
                }
            } else if (newCapacity < length) {
                if (newCapacity > maxLength >>> 1) {
                    if (maxLength <= 512) {
                        if (newCapacity > maxLength - 16) {
                            length = newCapacity;
                            setIndex(Math.min(readerIndex(), newCapacity), Math.min(writerIndex(), newCapacity));
                            return this;
                        }        
                    } else { // > 512 (i.e. >= 1024)
                        length = newCapacity;
                        setIndex(Math.min(readerIndex(), newCapacity), Math.min(writerIndex(), newCapacity));
                        return this;
                    }
                } 
            } else {
                return this;
            }
        }

        // Trim down the size of the current buffer, if able to.
        long newHandle = chunk.parent.trim(chunk, handle,  newCapacity);
        if (newHandle != -1) {
        	chunk.initBuf(this, newHandle, newCapacity);
        	return this;
        }
        
        // Reallocate the data.
        chunk.arena.reallocate(this, newCapacity, true);
        
        return this;
    }

    
    
    @Override
    public final ByteBufAllocator alloc() {
        return chunk.arena.parent;
    }

    @Override
    public final ByteOrder order() {
        return ByteOrder.BIG_ENDIAN; // TODO:  Is this correct?
    }

    @Override
    public final ByteBuf unwrap() {
        return null;
    }

    protected final ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        if (tmpNioBuf == null) {
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
        }
        return tmpNioBuf;
    }

    protected abstract ByteBuffer newInternalNioBuffer(T memory);

    
    /**
     * Free the memory and recycle the header.
     */
    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            chunk.arena.free(chunk, handle);
            if (leak != null) {
                leak.close();
            } else {
                recycle();
            }
        }
    }
    
    
    
    @SuppressWarnings("unchecked")
    private void recycle() {
        Recycler.Handle recyclerHandle = this.recyclerHandle;
        if (recyclerHandle != null) {
            ((Recycler<Object>) recycler()).recycle(this, recyclerHandle);
        }
    }

    protected abstract Recycler<?> recycler();

    protected final int idx(int index) {
        return offset + index;
    }
}


/**
 * Exception thrown after resizing a buffer results in excessive copying.
 *   The buffer has been properly resized, so It is possible to ignore the exception and continue.
 */
class TooMuchCopyingException extends IllegalArgumentException {
	private static final long serialVersionUID = -8184712014469036532L;
	TooMuchCopyingException(String s) {
		super(s);
	}
}
