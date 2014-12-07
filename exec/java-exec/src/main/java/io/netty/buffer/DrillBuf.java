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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import org.apache.drill.exec.memory.Accountor;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.util.AssertionUtil;

public final class DrillBuf extends AbstractByteBuf {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillBuf.class);

  private static final boolean BOUNDS_CHECKING_ENABLED = AssertionUtil.BOUNDS_CHECKING_ENABLED;

  private final ByteBuf b;
  private final long addr;
  private final int offset;
  private final boolean rootBuffer;

  private volatile BufferAllocator allocator;
  private volatile Accountor acct;
  private volatile int length;

  private OperatorContext context;
  private FragmentContext fContext;


  public DrillBuf(BufferAllocator allocator, Accountor a, UnsafeDirectLittleEndian b) {
    super(b.maxCapacity());
    this.b = b;
    this.addr = b.memoryAddress();
    this.acct = a;
    this.length = b.capacity();
    this.offset = 0;
    this.rootBuffer = true;
    this.allocator = allocator;
  }

  private DrillBuf(ByteBuffer bb) {
    super(bb.remaining());
    UnpooledUnsafeDirectByteBuf bytebuf = new UnpooledUnsafeDirectByteBuf(UnpooledByteBufAllocator.DEFAULT, bb, bb.remaining());
    this.acct = FakeAllocator.FAKE_ACCOUNTOR;
    this.addr = bytebuf.memoryAddress();
    this.allocator = FakeAllocator.FAKE_ALLOCATOR;
    this.b = bytebuf;
    this.length = bytebuf.capacity();
    this.offset = 0;
    this.rootBuffer = true;
    this.writerIndex(bb.remaining());
  }

  private DrillBuf(BufferAllocator allocator, Accountor a) {
    super(0);
    this.b = new EmptyByteBuf(allocator.getUnderlyingAllocator()).order(ByteOrder.LITTLE_ENDIAN);
    this.allocator = allocator;
    this.acct = a;
    this.length = 0;
    this.addr = 0;
    this.rootBuffer = true;
    this.offset = 0;
  }

  private DrillBuf(DrillBuf buffer, int index, int length) {
    super(length);
    if (index < 0 || index > buffer.capacity() - length) {
      throw new IndexOutOfBoundsException(buffer.toString() + ".slice(" + index + ", " + length + ')');
    }

    this.length = length;
    writerIndex(length);

    this.b = buffer;
    this.addr = buffer.memoryAddress() + index;
    this.offset = index;
    this.acct = null;
    this.length = length;
    this.rootBuffer = false;
    this.allocator = buffer.allocator;
  }

  public void setOperatorContext(OperatorContext c) {
    this.context = c;
  }
  public void setFragmentContext(FragmentContext c) {
    this.fContext = c;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public DrillBuf reallocIfNeeded(int size) {
    if (this.capacity() >= size) {
      return this;
    }
    if (context != null) {
      return context.replace(this, size);
    } else if(fContext != null) {
      return fContext.replace(this, size);
    } else {
      throw new UnsupportedOperationException("Realloc is only available in the context of an operator's UDFs");
    }

  }

  @Override
  public int refCnt() {
    return b.refCnt();
  }

  private long addr(int index) {
    return addr + index;
  }

  private final void checkIndexD(int index) {
    ensureAccessible();
    if (index < 0 || index >= capacity()) {
      throw new IndexOutOfBoundsException(String.format(
              "index: %d (expected: range(0, %d))", index, capacity()));
    }
  }

  private final void checkIndexD(int index, int fieldLength) {
    ensureAccessible();
    if (fieldLength < 0) {
      throw new IllegalArgumentException("length: " + fieldLength + " (expected: >= 0)");
    }
    if (index < 0 || index > capacity() - fieldLength) {
      throw new IndexOutOfBoundsException(String.format(
              "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
    }
  }

  /**
   * Allows a function to determine whether not reading a particular string of bytes is valid.
   *
   * Will throw an exception if the memory is not readable for some reason.  Only doesn't something in the
   * case that AssertionUtil.BOUNDS_CHECKING_ENABLED is true.
   *
   * @param start The starting position of the bytes to be read.
   * @param end The exclusive endpoint of the bytes to be read.
   */
  public void checkBytes(int start, int end){
    if (BOUNDS_CHECKING_ENABLED) {
      checkIndexD(start, end - start);
    }
  }

  private void chk(int index, int width) {
    if (BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index, width);
    }
  }

  private void chk(int index) {
    if (BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index);
    }
  }

  private void ensure(int width) {
    if (BOUNDS_CHECKING_ENABLED) {
      ensureWritable(width);
    }
  }

  public boolean transferAccounting(Accountor target) {
    if (rootBuffer) {
      boolean outcome = acct.transferTo(target, this, length);
      acct = target;
      return outcome;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public synchronized boolean release() {
    if (b.release() && rootBuffer) {
      acct.release(this, length);
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean release(int decrement) {
    if (b.release(decrement) && rootBuffer) {
      acct.release(this, length);
      return true;
    }
    return false;
  }

  @Override
  public int capacity() {
    return length;
  }

  @Override
  public synchronized ByteBuf capacity(int newCapacity) {
    if (rootBuffer) {
      if (newCapacity == length) {
        return this;
      } else if (newCapacity < length) {
        b.capacity(newCapacity);
        int diff = length - b.capacity();
        acct.releasePartial(this, diff);
        this.length = length - diff;
        return this;
      } else {
        throw new UnsupportedOperationException("Accounting byte buf doesn't support increasing allocations.");
      }
    } else {
      throw new UnsupportedOperationException("Non root bufs doen't support changing allocations.");
    }
  }

  @Override
  public int maxCapacity() {
    return length;
  }

  @Override
  public ByteBufAllocator alloc() {
    return b.alloc();
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    // if(endianness != ByteOrder.LITTLE_ENDIAN) throw new
    // UnsupportedOperationException("Drill buffers only support little endian.");
    return this;
  }

  @Override
  public ByteBuf unwrap() {
    return b;
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public ByteBuf readBytes(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readSlice(int length) {
    ByteBuf slice = slice(readerIndex(), length);
    readerIndex(readerIndex() + length);
    return slice;
  }

  @Override
  public ByteBuf copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf copy(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf slice() {
    return slice(readerIndex(), readableBytes());
  }

  @Override
  public DrillBuf slice(int index, int length) {
    DrillBuf buf = new DrillBuf(this, index, length);
    buf.writerIndex = length;
    return buf;
  }

  @Override
  public DrillBuf duplicate() {
    return new DrillBuf(this, 0, length);
  }

  @Override
  public int nioBufferCount() {
    return 1;
  }

  @Override
  public ByteBuffer nioBuffer() {
    return nioBuffer(readerIndex(), readableBytes());
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return b.nioBuffer(offset + index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return b.internalNioBuffer(offset + index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    return new ByteBuffer[]{nioBuffer()};
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return new ByteBuffer[]{nioBuffer(index, length)};
  }

  @Override
  public boolean hasArray() {
    return b.hasArray();
  }

  @Override
  public byte[] array() {
    return b.array();
  }

  @Override
  public int arrayOffset() {
    return b.arrayOffset();
  }

  @Override
  public boolean hasMemoryAddress() {
    return true;
  }

  @Override
  public long memoryAddress() {
    return this.addr;
  }

  @Override
  public String toString(Charset charset) {
      return toString(readerIndex, readableBytes(), charset);
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    if (length == 0) {
      return "";
    }

    ByteBuffer nioBuffer;
    if (nioBufferCount() == 1) {
      nioBuffer = nioBuffer(index, length);
    } else {
      nioBuffer = ByteBuffer.allocate(length);
      getBytes(index, nioBuffer);
      nioBuffer.flip();
    }

    return ByteBufUtil.decodeString(nioBuffer, charset);
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    // identity equals only.
    return this == obj;
  }

  @Override
  public ByteBuf retain(int increment) {
    b.retain(increment);
    return this;
  }

  @Override
  public ByteBuf retain() {
    b.retain();
    return this;
  }

  @Override
  public long getLong(int index) {
    chk(index, 8);
    long v = PlatformDependent.getLong(addr(index));
    return v;
  }

  @Override
  public float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  @Override
  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  @Override
  public char getChar(int index) {
    return (char) getShort(index);
  }

  @Override
  public long getUnsignedInt(int index) {
    return getInt(index) & 0xFFFFFFFFL;
  }

  @Override
  public int getInt(int index) {
    chk(index, 4);
    int v = PlatformDependent.getInt(addr(index));
    return v;
  }

  @Override
  public int getUnsignedShort(int index) {
    return getShort(index) & 0xFFFF;
  }

  @Override
  public short getShort(int index) {
    chk(index, 2);
    short v = PlatformDependent.getShort(addr(index));
    return v;
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    chk(index, 2);
    PlatformDependent.putShort(addr(index), (short) value);
    return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    chk(index, 4);
    PlatformDependent.putInt(addr(index), value);
    return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    chk(index, 8);
    PlatformDependent.putLong(addr(index), value);
    return this;
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    chk(index, 2);
    PlatformDependent.putShort(addr(index), (short) value);
    return this;
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    chk(index, 4);
    PlatformDependent.putInt(addr(index), Float.floatToRawIntBits(value));
    return this;
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    chk(index, 8);
    PlatformDependent.putLong(addr(index), Double.doubleToRawLongBits(value));
    return this;
  }

  @Override
  public ByteBuf writeShort(int value) {
    ensure(2);
    PlatformDependent.putShort(addr(writerIndex), (short) value);
    writerIndex += 2;
    return this;
  }

  @Override
  public ByteBuf writeInt(int value) {
    ensure(4);
    PlatformDependent.putInt(addr(writerIndex), value);
    writerIndex += 4;
    return this;
  }

  @Override
  public ByteBuf writeLong(long value) {
    ensure(8);
    PlatformDependent.putLong(addr(writerIndex), value);
    writerIndex += 8;
    return this;
  }

  @Override
  public ByteBuf writeChar(int value) {
    ensure(2);
    PlatformDependent.putShort(addr(writerIndex), (short) value);
    writerIndex += 2;
    return this;
  }

  @Override
  public ByteBuf writeFloat(float value) {
    ensure(4);
    PlatformDependent.putInt(addr(writerIndex), Float.floatToRawIntBits(value));
    writerIndex += 4;
    return this;
  }

  @Override
  public ByteBuf writeDouble(double value) {
    ensure(8);
    PlatformDependent.putLong(addr(writerIndex), Double.doubleToRawLongBits(value));
    writerIndex += 8;
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    b.getBytes(index + offset,  dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    b.getBytes(index + offset, dst);
    return this;
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    chk(index, 1);
    PlatformDependent.putByte(addr(index), (byte) value);
    return this;
  }

  @Override
  protected byte _getByte(int index) {
    return getByte(index);
  }

  @Override
  protected short _getShort(int index) {
    return getShort(index);
  }

  @Override
  protected int _getInt(int index) {
    return getInt(index);
  }

  @Override
  protected long _getLong(int index) {
    return getLong(index);
  }

  @Override
  protected void _setByte(int index, int value) {
    setByte(index, value);
  }

  @Override
  protected void _setShort(int index, int value) {
    setShort(index, value);
  }

  @Override
  protected void _setMedium(int index, int value) {
    setMedium(index, value);
  }

  @Override
  protected void _setInt(int index, int value) {
    setInt(index, value);
  }

  @Override
  protected void _setLong(int index, long value) {
    setLong(index, value);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    b.getBytes(index + offset, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    b.getBytes(index + offset, out, length);
    return this;
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    long addr = addr(index);
    return (PlatformDependent.getByte(addr) & 0xff) << 16 |
            (PlatformDependent.getByte(addr + 1) & 0xff) << 8 |
            PlatformDependent.getByte(addr + 2) & 0xff;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return b.getBytes(index + offset, out, length);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    b.setBytes(index + offset, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    b.setBytes(index + offset, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    b.setBytes(index + offset, src);
    return this;
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return b.setBytes(index + offset, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return b.setBytes(index + offset, in, length);
  }

  @Override
  public byte getByte(int index) {
    chk(index, 1);
    return PlatformDependent.getByte(addr(index));
  }

  public static DrillBuf getEmpty(BufferAllocator allocator, Accountor a) {
    return new DrillBuf(allocator, a);
  }

  public boolean isRootBuffer() {
    return rootBuffer;
  }

  public static DrillBuf wrapByteBuffer(ByteBuffer b) {
    if (!b.isDirect()) {
      throw new IllegalStateException("DrillBufs can only refer to direct memory.");
    } else {
      return new DrillBuf(b);
    }

  }

}
