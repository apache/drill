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
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.HistoricalLog;
import org.apache.drill.exec.memory.Accountor;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.memory.BufferLedger;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.util.AssertionUtil;
import org.apache.drill.exec.util.Pointer;
import org.slf4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

public final class DrillBuf extends AbstractByteBuf implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillBuf.class);

  private static final boolean BOUNDS_CHECKING_ENABLED = AssertionUtil.BOUNDS_CHECKING_ENABLED;
  private static final boolean DEBUG = BaseAllocator.isDebug();
  private static final AtomicInteger idGenerator = new AtomicInteger(0);

  private final ByteBuf byteBuf;
  private final long addr;
  private final int offset;
  private final int flags;
  private final AtomicInteger rootRefCnt;
  private volatile BufferAllocator allocator;

  // TODO - cleanup
  // The code is partly shared and partly copy-pasted between
  // these three types. They should be unified under one interface
  // to share code and to remove the hacky code here to use only
  // one of these types at a time and use null checks to find out
  // which.
  private final boolean oldWorld; // Indicates that we're operating with TopLevelAllocator.
  private final boolean rootBuffer;
  private volatile Accountor acct;
  private BufferManager bufManager;
  @Deprecated private OperatorContext operatorContext;
  @Deprecated private FragmentContext fragmentContext;

  private volatile BufferLedger bufferLedger;
  private volatile int length; // TODO this just seems to duplicate .capacity()

  // members used purely for debugging
  // TODO once we have a reduced number of constructors, move these to DEBUG clauses in them
  private final int id = idGenerator.incrementAndGet();
  private final HistoricalLog historicalLog = DEBUG ? new HistoricalLog(4, "DrillBuf[%d]", id) : null;
  private final static IdentityHashMap<UnsafeDirectLittleEndian, Collection<DrillBuf>> unwrappedMap =
      DEBUG ? new IdentityHashMap<UnsafeDirectLittleEndian, Collection<DrillBuf>>() : null;

  // TODO(cwestin) javadoc
  private void unwrappedPut() {
    final UnsafeDirectLittleEndian udle = (UnsafeDirectLittleEndian) byteBuf;
    synchronized(unwrappedMap) {
      Collection<DrillBuf> drillBufs = unwrappedMap.get(udle);
      if (drillBufs == null) {
        drillBufs = new LinkedList<DrillBuf>();
        unwrappedMap.put(udle, drillBufs);
      }

      drillBufs.add(this);
    }
  }

  // TODO(cwestin) javadoc
  public static Collection<DrillBuf> unwrappedGet(final UnsafeDirectLittleEndian udle) {
    synchronized(unwrappedMap) {
      final Collection<DrillBuf> drillBufs = unwrappedMap.get(udle);
      if (drillBufs == null) {
        return Collections.emptyList();
      }
      return new LinkedList<DrillBuf>(drillBufs);
    }
  }

  // TODO(cwestin) javadoc
  private static boolean unwrappedRemove(final DrillBuf drillBuf) {
    final ByteBuf byteBuf = drillBuf.unwrap();
    if (!(byteBuf instanceof UnsafeDirectLittleEndian)) {
      return false;
    }

    final UnsafeDirectLittleEndian udle = (UnsafeDirectLittleEndian) byteBuf;
    synchronized(unwrappedMap) {
      Collection<DrillBuf> drillBufs = unwrappedMap.get(udle);
      if (drillBufs == null) {
        return false;
      }
      final Object object = drillBufs.remove(drillBuf);
      if (drillBufs.isEmpty()) {
        unwrappedMap.remove(udle);
      }
      return object != null;
    }
  }

  public DrillBuf(BufferAllocator allocator, Accountor a, UnsafeDirectLittleEndian b) {
    super(b.maxCapacity());
    this.byteBuf = b;
    this.addr = b.memoryAddress();
    this.acct = a;
    this.length = b.capacity();
    this.offset = 0;
    this.rootBuffer = true;
    this.allocator = allocator;

    // members from the new world order
    flags = 0;
    rootRefCnt = null;
    oldWorld = true;
  }

  // TODO(cwestin) javadoc
  public DrillBuf(final BufferLedger bufferLedger, final BufferAllocator bufferAllocator,
      final UnsafeDirectLittleEndian byteBuf) {
    super(byteBuf.maxCapacity());
    this.byteBuf = byteBuf;
    byteBuf.retain(1);
    this.bufferLedger = bufferLedger;
    addr = byteBuf.memoryAddress();
    allocator = bufferAllocator;
    length = byteBuf.capacity();
    offset = 0;
    flags = 0;
    rootRefCnt = new AtomicInteger(1);
    oldWorld = false;

    // members from the old world order
    rootBuffer = false;
    acct = null;

    if (DEBUG) {
      unwrappedPut();
      historicalLog.recordEvent(
          "DrillBuf(BufferLedger, BufferAllocator[%d], UnsafeDirectLittleEndian[identityHashCode == "
              + "%d](%s)) => rootRefCnt identityHashCode == %d",
              bufferAllocator.getId(), System.identityHashCode(byteBuf), byteBuf.toString(),
              System.identityHashCode(rootRefCnt));
    }
  }

  private DrillBuf(BufferAllocator allocator, Accountor a) {
    super(0);
    this.byteBuf = new EmptyByteBuf(allocator.getUnderlyingAllocator()).order(ByteOrder.LITTLE_ENDIAN);
    this.allocator = allocator;
    this.acct = a;
    this.length = 0;
    this.addr = 0;
    this.rootBuffer = false;
    this.offset = 0;

    // members from the new world order
    flags = 0;
    rootRefCnt = null;
    oldWorld = true;
  }

  private DrillBuf(final BufferLedger bufferLedger, final BufferAllocator bufferAllocator) {
    super(0);
    this.bufferLedger = bufferLedger;
    allocator = bufferAllocator;

    byteBuf = new EmptyByteBuf(bufferLedger.getUnderlyingAllocator()).order(ByteOrder.LITTLE_ENDIAN);
    length = 0;
    addr = 0;
    flags = 0;
    rootRefCnt = new AtomicInteger(1);
    offset = 0;

    // members from the old world order
    rootBuffer = false;
    acct = null;
    oldWorld = false;

    if (DEBUG) {
      // We don't put the empty buffers in the unwrappedMap.
      historicalLog.recordEvent(
          "DrillBuf(BufferLedger, BufferAllocator[%d])  => rootRefCnt identityHashCode == %d",
          bufferAllocator.getId(), System.identityHashCode(rootRefCnt));
    }
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

    this.byteBuf = replacement;
    this.addr = buffer.memoryAddress() + index;
    this.offset = index;
    this.acct = a;
    this.length = length;
    this.rootBuffer = root;
    this.allocator = allocator;

    // members from the new world order
    flags = 0;
    rootRefCnt = null;
    oldWorld = true;
  }

  /**
   * Indicate a shared refcount, as per http://netty.io/wiki/reference-counted-objects.html#wiki-h3-5
   */
  private final static int F_DERIVED = 0x0002;

  // TODO(cwestin) javadoc
  /**
   * Used for sharing.
   *
   * @param bufferLedger
   * @param bufferAllocator
   * @param originalBuf
   * @param index
   * @param length
   * @param flags
   */
  public DrillBuf(final BufferLedger bufferLedger, final BufferAllocator bufferAllocator,
      final DrillBuf originalBuf, final int index, final int length, final int flags) {
    this(bufferAllocator, bufferLedger, getUnderlyingUdle(originalBuf),
        originalBuf, index + originalBuf.offset, length, flags);
  }

  /**
   * Unwraps a DrillBuf until the underlying UnsafeDirectLittleEndian buffer is
   * found.
   *
   * @param originalBuf the original DrillBuf
   * @return the underlying UnsafeDirectLittleEndian ByteBuf
   */
  private static ByteBuf getUnderlyingUdle(final DrillBuf originalBuf) {
    int count = 1;
    ByteBuf unwrapped = originalBuf.unwrap();
    while(!(unwrapped instanceof UnsafeDirectLittleEndian)
        && (!(unwrapped instanceof EmptyByteBuf))) {
      unwrapped = unwrapped.unwrap();
      ++count;
    }

    if (DEBUG) {
      if (count > 1) {
        throw new IllegalStateException("UnsafeDirectLittleEndian is wrapped more than one level");
      }
    }

    return unwrapped;
  }

  // TODO(cwestin) javadoc
  /*
   * TODO the replacement argument becomes an UnsafeDirectLittleEndian;
   * buffer argument may go away if it is determined to be unnecessary after all
   * the deprecated stuff is removed (I suspect only the replacement argument is
   * necessary then).
   */
  private DrillBuf(BufferAllocator allocator, BufferLedger bufferLedger,
      ByteBuf replacement, DrillBuf buffer, int index, int length, int flags) {
    super(replacement.maxCapacity());

    // members from the old world order
    rootBuffer = false;
    acct = null;
    oldWorld = false;

    if (index < 0 || index > (replacement.maxCapacity() - length)) {
      throw new IndexOutOfBoundsException(replacement.toString() + ".slice(" + index + ", " + length + ')');
    }

    this.flags = flags;

    this.length = length; // capacity()
    writerIndex(length);

    byteBuf = replacement;
    if ((flags & F_DERIVED) == 0) {
      replacement.retain(1);
    }

    addr = replacement.memoryAddress() + index;
    offset = index;
    this.bufferLedger = bufferLedger;
    if (!(buffer instanceof DrillBuf)) {
      throw new IllegalArgumentException("DrillBuf slicing can only be performed on other DrillBufs");
    }

    if ((flags & F_DERIVED) != 0) {
      final DrillBuf rootBuf = (DrillBuf) buffer;
      rootRefCnt = rootBuf.rootRefCnt;
    } else {
      rootRefCnt = new AtomicInteger(1);
    }

    this.allocator = allocator;

    if (DEBUG) {
      unwrappedPut();
      historicalLog.recordEvent(
          "DrillBuf(BufferAllocator[%d], BufferLedger, ByteBuf[identityHashCode == "
              + "%d](%s), DrillBuf[%d], index = %d, length = %d, flags = 0x%08x)"
              + " => rootRefCnt identityHashCode == %d",
          allocator.getId(), System.identityHashCode(replacement), replacement.toString(),
          buffer.id, index, length, flags, System.identityHashCode(rootRefCnt));
    }
  }

  @Deprecated
  public void setOperatorContext(OperatorContext c) {
    this.operatorContext = c;
  }

  @Deprecated
  public void setFragmentContext(FragmentContext c) {
    this.fragmentContext = c;
  }

  // TODO(DRILL-3331)
  public void setBufferManager(BufferManager bufManager) {
    Preconditions.checkState(this.bufManager == null,
        "the BufferManager for a buffer can only be set once");
    this.bufManager = bufManager;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public DrillBuf reallocIfNeeded(final int size) {
    Preconditions.checkArgument(size >= 0, "reallocation size must be non-negative");

    if (this.capacity() >= size) {
      return this;
    }

    if (operatorContext != null) {
      return operatorContext.replace(this, size);
    } else if(fragmentContext != null) {
      return fragmentContext.replace(this, size);
    } else if (bufManager != null) {
      return bufManager.replace(this, size);
    } else {
      throw new UnsupportedOperationException("Realloc is only available in the context of an operator's UDFs");
    }
  }

  @Override
  public int refCnt() {
    if (oldWorld) {
      if(rootBuffer){
        return (int) this.rootRefCnt.get();
      }else{
        return byteBuf.refCnt();
      }
    }

    return rootRefCnt.get();
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
    if (BOUNDS_CHECKING_ENABLED) {
      checkIndexD(start, end - start);
    }
  }

  private void chk(int index, int width) {
    if (BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index, width);
    }
  }

  private void ensure(int width) {
    if (BOUNDS_CHECKING_ENABLED) {
      ensureWritable(width);
    }
  }

  /**
   * Used by allocators to transfer ownership from one allocator to another.
   *
   * @param newLedger the new ledger the buffer should use going forward
   * @param newAllocator the new allocator
   * @return whether or not the buffer fits the receiving allocator's allocation limit
   */
  public boolean transferTo(final BufferAllocator newAllocator, final BufferLedger newLedger) {
    final Pointer<BufferLedger> pNewLedger = new Pointer<>(newLedger);
    final boolean fitsAllocation = bufferLedger.transferTo(newAllocator, pNewLedger, this);
    allocator = newAllocator;
    bufferLedger = pNewLedger.value;
    return fitsAllocation;
  }

  /**
   * DrillBuf's implementation of sharing buffer functionality, to be accessed from
   * {@link BufferAllocator#shareOwnership(DrillBuf, Pointer)}. See that function
   * for more information.
   *
   * @param otherLedger the ledger belonging to the other allocator to share with
   * @param otherAllocator the other allocator to be shared with
   * @param index the starting index (for slicing capability)
   * @param length the length (for slicing capability)
   * @return the new DrillBuf (wrapper)
   */
  public DrillBuf shareWith(final BufferLedger otherLedger, final BufferAllocator otherAllocator,
      final int index, final int length) {
    return shareWith(otherLedger, otherAllocator, index, length, 0);
  }

  // TODO(cwestin) javadoc
  private DrillBuf shareWith(final BufferLedger otherLedger, final BufferAllocator otherAllocator,
      final int index, final int length, final int flags) {
    final Pointer<DrillBuf> pDrillBuf = new Pointer<>();
    bufferLedger = bufferLedger.shareWith(pDrillBuf, otherLedger, otherAllocator, this, index, length, flags);
    return pDrillBuf.value;
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
  public boolean release() {
    return release(1);
  }

  /**
   * Release the provided number of reference counts.  If this is a root buffer, will decrease accounting if the local reference count returns to zero.
   */
  @Override
  public synchronized boolean release(int decrement) {
    Preconditions.checkArgument(decrement > 0,
        "release(%d) argument is not positive", decrement);
    if (DEBUG) {
      historicalLog.recordEvent("release(%d)", decrement);
    }

    if (oldWorld) {
      if(rootBuffer){
        final long newRefCnt = this.rootRefCnt.addAndGet(-decrement);
        Preconditions.checkArgument(newRefCnt > -1, "Buffer has negative reference count.");
        if (newRefCnt == 0) {
          byteBuf.release(decrement);
          acct.release(this, length);
          return true;
        }else{
          return false;
        }
      }else{
        return byteBuf.release(decrement);
      }
    }

    final int refCnt = rootRefCnt.addAndGet(-decrement);
    Preconditions.checkState(refCnt >= 0, "DrillBuf[%d] refCnt has gone negative", id);
    if (refCnt == 0) {
      bufferLedger.release(this);

      if (DEBUG) {
        unwrappedRemove(this);
      }

      // release the underlying buffer
      byteBuf.release(1);

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
    if (oldWorld) {
      if (rootBuffer) {
        if (newCapacity == length) {
          return this;
        } else if (newCapacity < length) {
          byteBuf.capacity(newCapacity);
          int diff = length - byteBuf.capacity();
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

    if ((flags & F_DERIVED) != 0) {
      throw new UnsupportedOperationException("Derived buffers don't support resizing.");
    }

    if (newCapacity == length) {
      return this;
    }

    if (newCapacity < length) {
      byteBuf.capacity(newCapacity);
      final int diff = length - byteBuf.capacity();
      length -= diff;
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
    if (oldWorld) {
      DrillBuf buf = new DrillBuf(this, index, length);
      buf.writerIndex = length;
      return buf;
    }

    /*
     * Re the behavior of reference counting,
     * see http://netty.io/wiki/reference-counted-objects.html#wiki-h3-5, which explains
     * that derived buffers share their reference count with their parent
     */
    final DrillBuf buf = shareWith(bufferLedger, allocator, index, length, F_DERIVED);
    buf.writerIndex(length);
    return buf;
  }

  @Override
  public DrillBuf duplicate() {
    if (oldWorld) {
      return new DrillBuf(this, 0, length);
    }

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
        String.format("{DrillBuf[%d], udle identityHashCode == %d, rootRefCnt identityHashCode == %d}",
            id, System.identityHashCode(byteBuf), System.identityHashCode(rootRefCnt));

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
    if (DEBUG) {
      historicalLog.recordEvent("retain(%d)", increment);
    }

    if (oldWorld) {
      if(rootBuffer){
        this.rootRefCnt.addAndGet(increment);
      }else{
        byteBuf.retain(increment);
      }
      return this;
    }

    rootRefCnt.addAndGet(increment);
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

  public static DrillBuf getEmpty(BufferAllocator allocator, Accountor a) {
    return new DrillBuf(allocator, a);
  }

  public static DrillBuf getEmpty(final BufferLedger bufferLedger, final BufferAllocator bufferAllocator) {
    return new DrillBuf(bufferLedger, bufferAllocator);
  }

  /**
   * Find out if this is a "root buffer." This is obsolete terminology
   * based on the original implementation of DrillBuf, which would layer
   * DrillBufs on top of other DrillBufs when slicing (or duplicating).
   * The buffer at the bottom of the layer was the "root buffer." However,
   * the current implementation flattens such references to always make
   * DrillBufs that are wrap a single buffer underneath, and slices and
   * their original source have a shared fate as per
   * http://netty.io/wiki/reference-counted-objects.html#wiki-h3-5, so
   * this concept isn't really meaningful anymore. But there are callers
   * that want to know a buffer's original size, and whether or not it
   * is "primal" in some sense. Perhaps this just needs a new name that
   * indicates that the buffer was an "original" and not a slice.
   *
   * @return whether or not the buffer is an original
   */
  @Deprecated
  public boolean isRootBuffer() {
    if (oldWorld) {
      return rootBuffer;
    }

    return (flags & F_DERIVED) == 0;
  }

  @Override
  public void close() {
    release();
  }

  /**
   * Indicates whether this DrillBuf and the supplied one have a "shared fate."
   * Having a "shared fate" indicates that the two DrillBufs share a reference
   * count, and will both be released at the same time if either of them is
   * released.
   * @param otherBuf the other buffer to check against
   * @return true if the two buffers have a shared fate, false otherwise
   */
  public boolean hasSharedFate(final DrillBuf otherBuf) {
    return rootRefCnt == otherBuf.rootRefCnt;
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
  public int getId() {
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

  public void logHistoryForUdle(final Logger logger, final UnsafeDirectLittleEndian udle) {
    final Collection<DrillBuf> drillBufs = unwrappedGet(udle);
    for(final DrillBuf drillBuf : drillBufs) {
      drillBuf.logHistory(logger);
    }
  }
}
