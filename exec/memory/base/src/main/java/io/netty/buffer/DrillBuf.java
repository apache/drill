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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.HistoricalLog;
import org.apache.drill.exec.memory.AllocatorManager.BufferLedger;
import org.apache.drill.exec.memory.BoundsChecking;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.BufferManager;
import org.slf4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

public final class DrillBuf extends AbstractByteBuf implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillBuf.class);

  private static final boolean DEBUG = false;
  private static final AtomicLong idGenerator = new AtomicLong(0);

  private final long id = idGenerator.incrementAndGet();
  private final AtomicInteger refCnt;
  private final UnsafeDirectLittleEndian byteBuf;
  private final long addr;
  private final int offset;
  private final int flags;
  private final BufferLedger ledger;
  private final BufferManager bufManager;
  private final ByteBufAllocator alloc;
  private final boolean isEmpty;
  private volatile int length;

  private final HistoricalLog historicalLog = DEBUG ? new HistoricalLog(4, "DrillBuf[%d]", id) : null;

  public DrillBuf(
      final AtomicInteger refCnt,
      final BufferLedger ledger,
      final UnsafeDirectLittleEndian byteBuf,
      final BufferManager manager,
      final ByteBufAllocator alloc,
      final int offset,
      final int length,
      boolean isEmpty) {
    super(byteBuf.maxCapacity());
    this.refCnt = refCnt;
    this.byteBuf = byteBuf;
    this.isEmpty = isEmpty;
    this.bufManager = manager;
    this.alloc = alloc;
    this.addr = byteBuf.memoryAddress() + offset;
    this.ledger = ledger;
    this.length = length;
    this.offset = offset;
    this.flags = 0;
  }

  public DrillBuf reallocIfNeeded(final int size) {
    Preconditions.checkArgument(size >= 0, "reallocation size must be non-negative");

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
    if (isEmpty) {
      return 1;
    } else {
      return refCnt.get();
    }
  }

  private long addr(int index) {
    return addr + index;
  }

  private final void checkIndexD(int index, int fieldLength) {
    ensureAccessible();
    if (fieldLength < 0) {
      throw new IllegalArgumentException("length: " + fieldLength + " (expected: >= 0)");
    }
    if (index < 0 || index > capacity() - fieldLength) {
      if (DEBUG) {
        historicalLog.logHistory(logger);
      }
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
  public void checkBytes(int start, int end) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(start, end - start);
    }
  }

  private void chk(int index, int width) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index, width);
    }
  }

  private void ensure(int width) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      ensureWritable(width);
    }
  }

  /**
   * Create a new DrillBuf that is associated with an alternative allocator for the purposes of memory ownership and
   * accounting. This has no impact on the reference counting for this allocator.
   *
   * This operation has no impact on the reference count of this DrillBuf. The newly created DrillBuf with either have a
   * reference count of 1 (in the case that this is the first time this memory is being associated with the new
   * allocator) or the current value of the reference count for the other AllocatorManager/BufferLedger combination in
   * the case that the provided allocator already had an association to this underlying memory.
   *
   * @param allocator
   *          The target allocator to create an association with.
   * @return A new DrillBuf which shares the same underlying memory as this DrillBuf.
   */
  public DrillBuf retain(BufferAllocator allocator) {

    if (isEmpty) {
      return this;
    }

    BufferLedger otherLedger = this.ledger.getLedgerForAllocator(allocator);
    return otherLedger.newDrillBuf(offset, length);
  }

  /**
   * Transfer the memory accounting ownership of this DrillBuf to another allocator. This will generate a new DrillBuf
   * that carries an association with the underlying memory of this DrillBuf. If this DrillBuf is connected to the
   * owning BufferLedger of this memory, that memory ownership/accounting will be transferred to the taret allocator. If
   * this DrillBuf does not currently own the memory underlying it (and is only associated with it), this does not
   * transfer any ownership to the newly created DrillBuf.
   *
   * This operation has no impact on the reference count of this DrillBuf. The newly created DrillBuf with either have a
   * reference count of 1 (in the case that this is the first time this memory is being associated with the new
   * allocator) or the current value of the reference count for the other AllocatorManager/BufferLedger combination in
   * the case that the provided allocator already had an association to this underlying memory.
   *
   * Transfers will always succeed, even if that puts the other allocator into an overlimit situation. This is possible
   * due to the fact that the original owning allocator may have allocated this memory out of a local reservation
   * whereas the target allocator may need to allocate new memory from a parent or RootAllocator. This operation is done
   * in a mostly-lockless but consistent manner. As such, the overlimit==true situation could occur slightly prematurely
   * to an actual overlimit==true condition. This is simply conservative behavior which means we may return overlimit
   * slightly sooner than is necessary.
   *
   * @param target
   *          The allocator to transfer ownership to.
   * @return A new transfer result with the impact of the transfer (whether it was overlimit) as well as the newly
   *         created DrillBuf.
   */
  public TransferResult transferOwnership(BufferAllocator target) {

    if (isEmpty) {
      return new TransferResult(true, this);
    }

    final BufferLedger otherLedger = this.ledger.getLedgerForAllocator(target);
    final DrillBuf newBuf = otherLedger.newDrillBuf(offset, length);
    final boolean allocationFit = this.ledger.transferBalance(otherLedger);
    return new TransferResult(allocationFit, newBuf);
  }

  /**
   * The outcome of a Transfer.
   */
  public class TransferResult {

    /**
     * Whether this transfer fit within the target allocator's capacity.
     */
    public final boolean allocationFit;

    /**
     * The newly created buffer associated with the target allocator.
     */
    public final DrillBuf buffer;

    private TransferResult(boolean allocationFit, DrillBuf buffer) {
      this.allocationFit = allocationFit;
      this.buffer = buffer;
    }

  }

  @Override
  public boolean release() {
    return release(1);
  }

  /**
   * Release the provided number of reference counts.  If this is a root buffer, will decrease accounting if the local reference count returns to zero.
   */
  @Override
  public synchronized boolean release(int decrement) {
    if (isEmpty) {
      return false;
    }

    Preconditions.checkArgument(decrement > 0, String.format("release(%d) argument is not positive", decrement));
    if (DEBUG) {
      historicalLog.recordEvent("release(%d)", decrement);
    }

    final int refCnt = this.refCnt.addAndGet(-decrement);
    Preconditions.checkState(refCnt >= 0, String.format("DrillBuf[%d] refCnt has gone negative", id));
    if (refCnt == 0) {
      ledger.release();
      return true;
    }

    return false;
  }

  @Override
  public int capacity() {
    return length;
  }

  @Override
  public synchronized DrillBuf capacity(int newCapacity) {

    if (newCapacity == length) {
      return this;
    }

    Preconditions.checkArgument(newCapacity >= 0);

    if (newCapacity < length) {
      length = newCapacity;
      return this;
    }

    throw new UnsupportedOperationException("Buffers don't support resizing that increases the size.");
  }

  @Override
  public ByteBufAllocator alloc() {
    return byteBuf.alloc();
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    return this;
  }

  @Override
  public ByteBuf unwrap() {
    return byteBuf;
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
    final ByteBuf slice = slice(readerIndex(), length);
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

  public static String bufferState(final ByteBuf buf) {
    final int cap = buf.capacity();
    final int mcap = buf.maxCapacity();
    final int ri = buf.readerIndex();
    final int rb = buf.readableBytes();
    final int wi = buf.writerIndex();
    final int wb = buf.writableBytes();
    return String.format("cap/max: %d/%d, ri: %d, rb: %d, wi: %d, wb: %d",
        cap, mcap, ri, rb, wi, wb);
  }

  @Override
  public DrillBuf slice(int index, int length) {

    if (isEmpty) {
      return this;
    }

    /*
     * Re the behavior of reference counting,
     * see http://netty.io/wiki/reference-counted-objects.html#wiki-h3-5, which explains
     * that derived buffers share their reference count with their parent
     */
    final DrillBuf newBuf = ledger.newDrillBuf(offset + index, length);
    newBuf.writerIndex(length);
    return newBuf;
  }

  @Override
  public DrillBuf duplicate() {
    return slice(0, length);
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
    return byteBuf.nioBuffer(offset + index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return byteBuf.internalNioBuffer(offset + index, length);
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
    return byteBuf.hasArray();
  }

  @Override
  public byte[] array() {
    return byteBuf.array();
  }

  @Override
  public int arrayOffset() {
    return byteBuf.arrayOffset();
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
  public String toString() {
    return toString(0, 0, Charsets.UTF_8);
  }

  @Override
  public String toString(Charset charset) {
    return toString(readerIndex, readableBytes(), charset);
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    final String basics =
        String.format("{DrillBuf[%d], udle identityHashCode == %d, identityHashCode == %d}",
            id, System.identityHashCode(byteBuf), System.identityHashCode(refCnt));

    if (length == 0) {
      return basics;
    }

    final ByteBuffer nioBuffer;
    if (nioBufferCount() == 1) {
      nioBuffer = nioBuffer(index, length);
    } else {
      nioBuffer = ByteBuffer.allocate(length);
      getBytes(index, nioBuffer);
      nioBuffer.flip();
    }

    return basics + '\n' + ByteBufUtil.decodeString(nioBuffer, charset);
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
    Preconditions.checkArgument(increment > 0, "retain(%d) argument is not positive", increment);

    if (isEmpty) {
      return this;
    }

    if (DEBUG) {
      historicalLog.recordEvent("retain(%d)", increment);
    }

    refCnt.addAndGet(increment);
    return this;
  }

  @Override
  public ByteBuf retain() {
    return retain(1);
  }

  @Override
  public long getLong(int index) {
    chk(index, 8);
    final long v = PlatformDependent.getLong(addr(index));
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
    final int v = PlatformDependent.getInt(addr(index));
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
    byteBuf.getBytes(index + offset, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    byteBuf.getBytes(index + offset, dst);
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
    byteBuf.getBytes(index + offset, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    byteBuf.getBytes(index + offset, out, length);
    return this;
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    final long addr = addr(index);
    return (PlatformDependent.getByte(addr) & 0xff) << 16 |
            (PlatformDependent.getByte(addr + 1) & 0xff) << 8 |
            PlatformDependent.getByte(addr + 2) & 0xff;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return byteBuf.getBytes(index + offset, out, length);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    byteBuf.setBytes(index + offset, src, srcIndex, length);
    return this;
  }

  public ByteBuf setBytes(int index, ByteBuffer src, int srcIndex, int length) {
    if (src.isDirect()) {
      checkIndex(index, length);
      PlatformDependent.copyMemory(PlatformDependent.directBufferAddress(src) + srcIndex, this.memoryAddress() + index,
          length);
    } else {
      if (srcIndex == 0 && src.capacity() == length) {
        byteBuf.setBytes(index + offset, src);
      } else {
        ByteBuffer newBuf = src.duplicate();
        newBuf.position(srcIndex);
        newBuf.limit(srcIndex + length);
        byteBuf.setBytes(index + offset, src);
      }
    }

    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    byteBuf.setBytes(index + offset, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    byteBuf.setBytes(index + offset, src);
    return this;
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return byteBuf.setBytes(index + offset, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return byteBuf.setBytes(index + offset, in, length);
  }

  @Override
  public byte getByte(int index) {
    chk(index, 1);
    return PlatformDependent.getByte(addr(index));
  }

  @Override
  public void close() {
    release();
  }

  /**
   * Returns the possible memory consumed by this DrillBuf in the worse case scenario. (not shared, connected to larger
   * underlying buffer of allocated memory)
   *
   * @return Size in bytes.
   */
  public int getPossibleMemoryConsumed() {
    return ledger.getSize();
  }

  /**
   * Return that is Accounted for by this buffer (and its potentially shared siblings within the context of the
   * associated allocator).
   *
   * @return Size in bytes.
   */
  public int getActualMemoryConsumed() {
    return ledger.getAccountedSize();
  }

  private final static int LOG_BYTES_PER_ROW = 10;
  /**
   * Log this buffer's byte contents in the form of a hex dump.
   *
   * @param logger where to log to
   * @param start the starting byte index
   * @param length how many bytes to log
   */
  public void logBytes(final Logger logger, final int start, final int length) {
    final int roundedStart = (start / LOG_BYTES_PER_ROW) * LOG_BYTES_PER_ROW;

    final StringBuilder sb = new StringBuilder("buffer byte dump\n");
    int index = roundedStart;
    for(int nLogged = 0; nLogged < length; nLogged += LOG_BYTES_PER_ROW) {
      sb.append(String.format(" [%05d-%05d]", index, index + LOG_BYTES_PER_ROW - 1));
      for(int i = 0; i < LOG_BYTES_PER_ROW; ++i) {
        try {
          final byte b = getByte(index++);
          sb.append(String.format(" 0x%02x", b));
        } catch(IndexOutOfBoundsException ioob) {
          sb.append(" <ioob>");
        }
      }
      sb.append('\n');
    }
    logger.trace(sb.toString());
  }

  /**
   * Get the integer id assigned to this DrillBuf for debugging purposes.
   *
   * @return integer id
   */
  public long getId() {
    return id;
  }

  /**
   * Log this buffer's history.
   *
   * @param logger the logger to use
   */
  public void logHistory(final Logger logger) {
    if (historicalLog == null) {
      logger.warn("DrillBuf[{}] historicalLog not available", id);
    } else {
      historicalLog.logHistory(logger);
    }
  }


}
