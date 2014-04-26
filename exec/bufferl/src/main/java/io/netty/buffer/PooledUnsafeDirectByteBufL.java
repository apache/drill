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

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public final class PooledUnsafeDirectByteBufL extends PooledByteBufL<ByteBuffer> {
  
    private static final boolean NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private static final Recycler<PooledUnsafeDirectByteBufL> RECYCLER = new Recycler<PooledUnsafeDirectByteBufL>() {
        @Override
        protected PooledUnsafeDirectByteBufL newObject(Handle handle) {
            return new PooledUnsafeDirectByteBufL(handle, 0);
        }
    };

    static PooledUnsafeDirectByteBufL newInstance(int maxCapacity) {
        PooledUnsafeDirectByteBufL buf = RECYCLER.get();
        buf.setRefCnt(1);
        buf.maxCapacity(maxCapacity);
        return buf;
    }

    private long memoryAddress;

    private PooledUnsafeDirectByteBufL(Recycler.Handle recyclerHandle, int maxCapacity) {
        super(recyclerHandle, maxCapacity);
    }

    @Override
    void init(PoolChunkL<ByteBuffer> chunk, long handle, int offset, int length, int maxLength) {
        super.init(chunk, handle, offset, length, maxLength);
        initMemoryAddress();
    }

    @Override
    void initUnpooled(PoolChunkL<ByteBuffer> chunk, int length) {
        super.initUnpooled(chunk, length);
        initMemoryAddress();
    }

    private void initMemoryAddress() {
        memoryAddress = PlatformDependent.directBufferAddress(memory) + offset;
    }

    @Override
    protected ByteBuffer newInternalNioBuffer(ByteBuffer memory) {
        return memory.duplicate();
    }

    @Override
    public boolean isDirect() {
        return true;
    }

    @Override
    protected byte _getByte(int index) {
        return PlatformDependent.getByte(addr(index));
    }

    @Override
    protected short _getShort(int index) {
        short v = PlatformDependent.getShort(addr(index));
        return NATIVE_ORDER? v : Short.reverseBytes(v);
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        long addr = addr(index);
        return (PlatformDependent.getByte(addr) & 0xff) << 16 |
                (PlatformDependent.getByte(addr + 1) & 0xff) << 8 |
                PlatformDependent.getByte(addr + 2) & 0xff;
    }

    @Override
    protected int _getInt(int index) {
        int v = PlatformDependent.getInt(addr(index));
        return NATIVE_ORDER? v : Integer.reverseBytes(v);
    }

    @Override
    protected long _getLong(int index) {
        long v = PlatformDependent.getLong(addr(index));
        return NATIVE_ORDER? v : Long.reverseBytes(v);
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        checkIndex(index, length);
        if (dst == null) {
            throw new NullPointerException("dst");
        }
        if (dstIndex < 0 || dstIndex > dst.capacity() - length) {
            throw new IndexOutOfBoundsException("dstIndex: " + dstIndex);
        }

        if (length != 0) {
            if (dst.hasMemoryAddress()) {
                PlatformDependent.copyMemory(addr(index), dst.memoryAddress() + dstIndex, length);
            } else if (dst.hasArray()) {
                PlatformDependent.copyMemory(addr(index), dst.array(), dst.arrayOffset() + dstIndex, length);
            } else {
                dst.setBytes(dstIndex, this, index, length);
            }
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        checkIndex(index, length);
        if (dst == null) {
            throw new NullPointerException("dst");
        }
        if (dstIndex < 0 || dstIndex > dst.length - length) {
            throw new IndexOutOfBoundsException("dstIndex: " + dstIndex);
        }
        if (length != 0) {
            PlatformDependent.copyMemory(addr(index), dst, dstIndex, length);
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        checkIndex(index);
        int bytesToCopy = Math.min(capacity() - index, dst.remaining());
        ByteBuffer tmpBuf = internalNioBuffer();
        index = idx(index);
        tmpBuf.clear().position(index).limit(index + bytesToCopy);
        dst.put(tmpBuf);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        checkIndex(index, length);
        if (length != 0) {
            byte[] tmp = new byte[length];
            PlatformDependent.copyMemory(addr(index), tmp, 0, length);
            out.write(tmp);
        }
        return this;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        checkIndex(index, length);
        if (length == 0) {
            return 0;
        }

        ByteBuffer tmpBuf = internalNioBuffer();
        index = idx(index);
        tmpBuf.clear().position(index).limit(index + length);
        return out.write(tmpBuf);
    }

    @Override
    protected void _setByte(int index, int value) {
        PlatformDependent.putByte(addr(index), (byte) value);
    }

    @Override
    protected void _setShort(int index, int value) {
        PlatformDependent.putShort(addr(index), NATIVE_ORDER ? (short) value : Short.reverseBytes((short) value));
    }

    @Override
    protected void _setMedium(int index, int value) {
        long addr = addr(index);
        PlatformDependent.putByte(addr, (byte) (value >>> 16));
        PlatformDependent.putByte(addr + 1, (byte) (value >>> 8));
        PlatformDependent.putByte(addr + 2, (byte) value);
    }

    @Override
    protected void _setInt(int index, int value) {
        PlatformDependent.putInt(addr(index), NATIVE_ORDER ? value : Integer.reverseBytes(value));
    }

    @Override
    protected void _setLong(int index, long value) {
        PlatformDependent.putLong(addr(index), NATIVE_ORDER ? value : Long.reverseBytes(value));
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        checkIndex(index, length);
        if (src == null) {
            throw new NullPointerException("src");
        }
        if (srcIndex < 0 || srcIndex > src.capacity() - length) {
            throw new IndexOutOfBoundsException("srcIndex: " + srcIndex);
        }

        if (length != 0) {
            if (src.hasMemoryAddress()) {
                PlatformDependent.copyMemory(src.memoryAddress() + srcIndex, addr(index), length);
            } else if (src.hasArray()) {
                PlatformDependent.copyMemory(src.array(), src.arrayOffset() + srcIndex, addr(index), length);
            } else {
                src.getBytes(srcIndex, this, index, length);
            }
        }
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        checkIndex(index, length);
        if (length != 0) {
            PlatformDependent.copyMemory(src, srcIndex, addr(index), length);
        }
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        checkIndex(index);
        ByteBuffer tmpBuf = internalNioBuffer();
        if (src == tmpBuf) {
            src = src.duplicate();
        }

        index = idx(index);
        tmpBuf.clear().position(index).limit(index + src.remaining());
        tmpBuf.put(src);
        return this;
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        checkIndex(index, length);
        byte[] tmp = new byte[length];
        int readBytes = in.read(tmp);
        if (readBytes > 0) {
            PlatformDependent.copyMemory(tmp, 0, addr(index), readBytes);
        }
        return readBytes;
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        checkIndex(index, length);
        ByteBuffer tmpNioBuf = internalNioBuffer();
        index = idx(index);
        tmpNioBuf.clear().position(index).limit(index + length);
        try {
            return in.read(tmpNioBuf);
        } catch (ClosedChannelException e) {
            return -1;
        }
    }

    @Override
    public ByteBuf copy(int index, int length) {
        checkIndex(index, length);
        PooledUnsafeDirectByteBufL copy = (PooledUnsafeDirectByteBufL) alloc().directBuffer(length, maxCapacity());
        if (length != 0) {
            PlatformDependent.copyMemory(addr(index), copy.addr(0), length);
            copy.setIndex(0, length);
        }
        return copy;
    }

    @Override
    public int nioBufferCount() {
        return 1;
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        return new ByteBuffer[] { nioBuffer(index, length) };
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        checkIndex(index, length);
        index = idx(index);
        return (ByteBuffer) internalNioBuffer().clear().position(index).limit(index + length);
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException("direct buffer");
    }

    @Override
    public int arrayOffset() {
        throw new UnsupportedOperationException("direct buffer");
    }

    @Override
    public boolean hasMemoryAddress() {
        return true;
    }

    @Override
    public long memoryAddress() {
        return memoryAddress;
    }

    private long addr(int index) {
        return memoryAddress + index;
    }

    @Override
    protected Recycler<?> recycler() {
        return RECYCLER;
    }
}
