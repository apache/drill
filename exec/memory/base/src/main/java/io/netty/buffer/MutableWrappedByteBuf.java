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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * This is basically a complete copy of DuplicatedByteBuf. We copy because we want to override some behaviors and make
 * buffer mutable.
 */
abstract class MutableWrappedByteBuf extends AbstractByteBuf {

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return unwrap().nioBuffer(index, length);
  }

  ByteBuf buffer;

  public MutableWrappedByteBuf(ByteBuf buffer) {
    super(buffer.maxCapacity());

    if (buffer instanceof MutableWrappedByteBuf) {
      this.buffer = ((MutableWrappedByteBuf) buffer).buffer;
    } else {
      this.buffer = buffer;
    }

    setIndex(buffer.readerIndex(), buffer.writerIndex());
  }

  @Override
  public ByteBuf unwrap() {
    return buffer;
  }

  @Override
  public ByteBufAllocator alloc() {
    return buffer.alloc();
  }

  @Override
  public ByteOrder order() {
    return buffer.order();
  }

  @Override
  public boolean isDirect() {
    return buffer.isDirect();
  }

  @Override
  public int capacity() {
    return buffer.capacity();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    buffer.capacity(newCapacity);
    return this;
  }

  @Override
  public boolean hasArray() {
    return buffer.hasArray();
  }

  @Override
  public byte[] array() {
    return buffer.array();
  }

  @Override
  public int arrayOffset() {
    return buffer.arrayOffset();
  }

  @Override
  public boolean hasMemoryAddress() {
    return buffer.hasMemoryAddress();
  }

  @Override
  public long memoryAddress() {
    return buffer.memoryAddress();
  }

  @Override
  public byte getByte(int index) {
    return _getByte(index);
  }

  @Override
  protected byte _getByte(int index) {
    return buffer.getByte(index);
  }

  @Override
  public short getShort(int index) {
    return _getShort(index);
  }

  @Override
  protected short _getShort(int index) {
    return buffer.getShort(index);
  }

  @Override
  public int getUnsignedMedium(int index) {
    return _getUnsignedMedium(index);
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    return buffer.getUnsignedMedium(index);
  }

  @Override
  public int getInt(int index) {
    return _getInt(index);
  }

  @Override
  protected int _getInt(int index) {
    return buffer.getInt(index);
  }

  @Override
  public long getLong(int index) {
    return _getLong(index);
  }

  @Override
  protected long _getLong(int index) {
    return buffer.getLong(index);
  }

  @Override
  public abstract ByteBuf copy(int index, int length);

  @Override
  public ByteBuf slice(int index, int length) {
    return new SlicedByteBuf(this, index, length);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    buffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    buffer.getBytes(index, dst);
    return this;
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    _setByte(index, value);
    return this;
  }

  @Override
  protected void _setByte(int index, int value) {
    buffer.setByte(index, value);
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    _setShort(index, value);
    return this;
  }

  @Override
  protected void _setShort(int index, int value) {
    buffer.setShort(index, value);
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    _setMedium(index, value);
    return this;
  }

  @Override
  protected void _setMedium(int index, int value) {
    buffer.setMedium(index, value);
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    _setInt(index, value);
    return this;
  }

  @Override
  protected void _setInt(int index, int value) {
    buffer.setInt(index, value);
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    _setLong(index, value);
    return this;
  }

  @Override
  protected void _setLong(int index, long value) {
    buffer.setLong(index, value);
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    buffer.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    buffer.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    buffer.setBytes(index, src);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length)
      throws IOException {
    buffer.getBytes(index, out, length);
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length)
      throws IOException {
    return buffer.getBytes(index, out, length);
  }

  @Override
  public int setBytes(int index, InputStream in, int length)
      throws IOException {
    return buffer.setBytes(index, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length)
      throws IOException {
    return buffer.setBytes(index, in, length);
  }

  @Override
  public int nioBufferCount() {
    return buffer.nioBufferCount();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return buffer.nioBuffers(index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return nioBuffer(index, length);
  }

  @Override
  public int forEachByte(int index, int length, ByteBufProcessor processor) {
    return buffer.forEachByte(index, length, processor);
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
    return buffer.forEachByteDesc(index, length, processor);
  }

  @Override
  public final int refCnt() {
    return unwrap().refCnt();
  }

  @Override
  public final ByteBuf retain() {
    unwrap().retain();
    return this;
  }

  @Override
  public final ByteBuf retain(int increment) {
    unwrap().retain(increment);
    return this;
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    boolean released = unwrap().release(decrement);
    return released;
  }

}
