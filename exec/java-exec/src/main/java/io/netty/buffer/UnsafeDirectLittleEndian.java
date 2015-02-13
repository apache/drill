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

import java.nio.ByteOrder;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.StackTrace;
import org.apache.drill.exec.util.AssertionUtil;
import org.slf4j.Logger;

public final class UnsafeDirectLittleEndian extends WrappedByteBuf {
  private static final boolean NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
  private final AbstractByteBuf wrapped;
  private final long memoryAddress;

  private static final boolean TRACK_BUFFERS = false;
  private AtomicLong bufferCount;
  private AtomicLong bufferSize;
  private long initCap = -1;

  private final static IdentityHashMap<UnsafeDirectLittleEndian, StackTrace> bufferMap = new IdentityHashMap<>();

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    boolean released = super.release(decrement);
    if (TRACK_BUFFERS) {
      if (released) {
        final Object object;
        synchronized (bufferMap) {
          object = bufferMap.remove(this);
        }
        if (object == null) {
          throw new IllegalStateException("no such buffer");
        }

        if (initCap != -1) {
          bufferCount.decrementAndGet();
          bufferSize.addAndGet(-initCap);
        }
      }
    }

    return released;
  }


  public static int getBufferCount() {
    return bufferMap.size();
  }

  public static void releaseBuffers() {
    synchronized(bufferMap) {
      final Set<UnsafeDirectLittleEndian> bufferSet = bufferMap.keySet();
      final LinkedList<UnsafeDirectLittleEndian> bufferList = new LinkedList<>(bufferSet);
      while(!bufferList.isEmpty()) {
        final UnsafeDirectLittleEndian udle = bufferList.removeFirst();
        udle.release(udle.refCnt());
      }
    }
  }

  public static void logBuffers(final Logger logger) {
    synchronized (bufferMap) {
      int count = 0;
      final Set<UnsafeDirectLittleEndian> bufferSet = bufferMap.keySet();
      for (final UnsafeDirectLittleEndian udle : bufferSet) {
        final StackTrace stackTrace = bufferMap.get(udle);
        ++count;
        logger.debug("#" + count + " active buffer allocated at\n" + stackTrace);
      }
    }
  }

  UnsafeDirectLittleEndian(LargeBuffer buf) {
    this(buf, true);
  }

  UnsafeDirectLittleEndian(PooledUnsafeDirectByteBuf buf, AtomicLong bufferCount, AtomicLong bufferSize) {
    this(buf, true);
    this.bufferCount = bufferCount;
    this.bufferSize = bufferSize;

    // initCap is used if we're tracking memory release. If we're in non-debug mode, we'll skip this.
    initCap = AssertionUtil.ASSERT_ENABLED ? capacity() : -1;
  }

  private UnsafeDirectLittleEndian(AbstractByteBuf buf, boolean fake) {
    super(buf);
    if (!NATIVE_ORDER || buf.order() != ByteOrder.BIG_ENDIAN) {
      throw new IllegalStateException("Drill only runs on LittleEndian systems.");
    }
    wrapped = buf;
    memoryAddress = buf.memoryAddress();

    if (TRACK_BUFFERS) {
      synchronized (bufferMap) {
        bufferMap.put(this, new StackTrace());
      }
    }
  }

    private long addr(int index) {
      return memoryAddress + index;
    }

    @Override
    public long getLong(int index) {
      return PlatformDependent.getLong(addr(index));
    }

    @Override
    public float getFloat(int index) {
      return Float.intBitsToFloat(getInt(index));
    }

  @Override
  public ByteBuf slice() {
    return slice(this.readerIndex(), readableBytes());
  }

  @Override
  public ByteBuf slice(int index, int length) {
    return new SlicedByteBuf(this, index, length);
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
    return PlatformDependent.getInt(addr(index));
  }

  @Override
  public int getUnsignedShort(int index) {
    return getShort(index) & 0xFFFF;
  }

  @Override
  public short getShort(int index) {
    return PlatformDependent.getShort(addr(index));
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    wrapped.checkIndex(index, 2);
    _setShort(index, value);
    return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    wrapped.checkIndex(index, 4);
    _setInt(index, value);
    return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    wrapped.checkIndex(index, 8);
    _setLong(index, value);
    return this;
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    setShort(index, value);
    return this;
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    setInt(index, Float.floatToRawIntBits(value));
    return this;
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    setLong(index, Double.doubleToRawLongBits(value));
    return this;
  }


  @Override
  public ByteBuf writeShort(int value) {
    wrapped.ensureWritable(2);
    _setShort(wrapped.writerIndex, value);
    wrapped.writerIndex += 2;
    return this;
  }

  @Override
  public ByteBuf writeInt(int value) {
    wrapped.ensureWritable(4);
    _setInt(wrapped.writerIndex, value);
    wrapped.writerIndex += 4;
    return this;
  }

  @Override
  public ByteBuf writeLong(long value) {
    wrapped.ensureWritable(8);
    _setLong(wrapped.writerIndex, value);
    wrapped.writerIndex += 8;
    return this;
  }

  @Override
  public ByteBuf writeChar(int value) {
    writeShort(value);
    return this;
  }

  @Override
  public ByteBuf writeFloat(float value) {
    writeInt(Float.floatToRawIntBits(value));
    return this;
  }

  @Override
  public ByteBuf writeDouble(double value) {
    writeLong(Double.doubleToRawLongBits(value));
    return this;
  }

  private void _setShort(int index, int value) {
    PlatformDependent.putShort(addr(index), (short) value);
  }

  private void _setInt(int index, int value) {
    PlatformDependent.putInt(addr(index), value);
  }

  private void _setLong(int index, long value) {
    PlatformDependent.putLong(addr(index), value);
  }

  @Override
  public byte getByte(int index) {
    return PlatformDependent.getByte(addr(index));
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    PlatformDependent.putByte(addr(index), (byte) value);
    return this;
  }
}
