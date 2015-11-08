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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.exec.memory.Accountor;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.util.AssertionUtil;

import com.google.common.base.Preconditions;

public final class DrillBuf extends AbstractByteBuf implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillBuf.class);

  private static final boolean BOUNDS_CHECKING_ENABLED = AssertionUtil.BOUNDS_CHECKING_ENABLED;

  private final ByteBuf b;
  private final long addr;
  private final int offset;
  private final boolean rootBuffer;
  private final AtomicLong rootRefCnt = new AtomicLong(1);
  private volatile BufferAllocator allocator;
  private volatile Accountor acct;
  private volatile int length;

  // TODO - cleanup
  // The code is partly shared and partly copy-pasted between
  // these three types. They should be unified under one interface
  // to share code and to remove the hacky code here to use only
  // one of these types at a time and use null checks to find out
  // which.
  private BufferManager bufManager;

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

  private DrillBuf(BufferAllocator allocator, Accountor a) {
    super(0);
    this.b = new EmptyByteBuf(allocator.getUnderlyingAllocator()).order(ByteOrder.LITTLE_ENDIAN);
    this.allocator = allocator;
    this.acct = a;
    this.length = 0;
    this.addr = 0;
    this.rootBuffer = false;
    this.offset = 0;
  }

  /**
   * Special constructor used for RPC ownership transfer.  Takes a snapshot slice of the current buf
   *  but points directly to the underlying UnsafeLittleEndian buffer.  Does this by calling unwrap()
   *  twice on the provided DrillBuf and expecting an UnsafeDirectLittleEndian buffer. This operation
   *  includes taking a new reference count on the underlying buffer and maintaining returning with a
   *  current reference count for itself (masking the underlying reference count).
   * @param allocator
   * @param a Allocator used when users try to receive allocator from buffer.
   * @param b Accountor used for accounting purposes.
   */
  public DrillBuf(BufferAllocator allocator, Accountor a, DrillBuf b) {
    this(allocator, a, getUnderlying(b), b, 0, b.length, true);
    assert b.unwrap().unwrap() instanceof UnsafeDirectLittleEndian;
    b.unwrap().unwrap().retain();
  }


  private DrillBuf(DrillBuf buffer, int index, int length) {
    this(buffer.allocator, null, buffer, buffer, index, length, false);
  }

  private static ByteBuf getUnderlying(DrillBuf b){
    ByteBuf underlying = b.unwrap().unwrap();
    return underlying.slice((int) (b.memoryAddress() - underlying.memoryAddress()), b.length);
  }
  private DrillBuf(BufferAllocator allocator, Accountor a, ByteBuf replacement, DrillBuf buffer, int index, int length, boolean root) {
    super(length);
    if (index < 0 || index > buffer.capacity() - length) {
      throw new IndexOutOfBoundsException(buffer.toString() + ".slice(" + index + ", " + length + ')');
    }

    this.length = length;
    writerIndex(length);

    this.b = replacement;
    this.addr = buffer.memoryAddress() + index;
    this.offset = index;
    this.acct = a;
    this.length = length;
    this.rootBuffer = root;
    this.allocator = allocator;
  }

  public void setBufferManager(BufferManager bufManager) {
    this.bufManager = bufManager;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public DrillBuf reallocIfNeeded(int size) {
    if (this.capacity() >= size) {
      return this;
    }

    if (bufManager != null) {
      return bufManager.replace(this, size);
    } else {
      throw new UnsupportedOperationException("Realloc is only available in the context of an operator's UDFs");
    }

  }

  @Override
  public int refCnt() {
    if(rootBuffer){
      return (int) this.rootRefCnt.get();
    }else{
      return b.refCnt();
    }

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
    return release(1);
  }

  /**
   * Release the provided number of reference counts.  If this is a root buffer, will decrease accounting if the local reference count returns to zero.
   */
  @Override
  public synchronized boolean release(int decrement) {

    if(rootBuffer){
      final long newRefCnt = this.rootRefCnt.addAndGet(-decrement);
      Preconditions.checkArgument(newRefCnt > -1, "Buffer has negative reference count.");
      if (newRefCnt == 0) {
        b.release(decrement);
        acct.release(this, length);
        return true;
      }else{
        return false;
      }
    }else{
      return b.release(decrement);
    }
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
  public synchronized ByteBuf retain(int increment) {
    if(rootBuffer){
      this.rootRefCnt.addAndGet(increment);
    }else{
      b.retain(increment);
    }
    return this;
  }

  @Override
  public ByteBuf retain() {
    return retain(1);
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

  public void setByte(int index, byte b){
    chk(index, 1);
    PlatformDependent.putByte(addr(index), b);
  }

  public void writeByteUnsafe(byte b){
    PlatformDependent.putByte(addr(readerIndex), b);
    readerIndex++;
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

  public ByteBuf setBytes(int index, ByteBuffer src, int srcIndex, int length) {
    if (src.isDirect()) {
      checkIndex(index, length);
      PlatformDependent.copyMemory(PlatformDependent.directBufferAddress(src) + srcIndex, this.memoryAddress() + index,
          length);
    } else {
      if (srcIndex == 0 && src.capacity() == length) {
        b.setBytes(index + offset, src);
      } else {
        ByteBuffer newBuf = src.duplicate();
        newBuf.position(srcIndex);
        newBuf.limit(srcIndex + length);
        b.setBytes(index + offset, src);
      }
    }

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

  @Override
  public void close() {
    release();
  }

}
