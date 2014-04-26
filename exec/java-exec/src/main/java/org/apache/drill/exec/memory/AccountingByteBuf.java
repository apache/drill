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
package org.apache.drill.exec.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.DuplicatedByteBuf;
import io.netty.buffer.PooledUnsafeDirectByteBufL;
import io.netty.buffer.SlicedByteBuf;
import io.netty.buffer.SwappedByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

public class AccountingByteBuf extends ByteBuf{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccountingByteBuf.class);
  
  private final PooledUnsafeDirectByteBufL b;
  private final Accountor acct;
  private int size;
  
  public AccountingByteBuf(Accountor a, PooledUnsafeDirectByteBufL b) {
    super();
    this.b = b;
    this.acct = a;
    this.size = b.capacity();
  }

  @Override
  public int refCnt() {
    return b.refCnt();
  }

  @Override
  public boolean release() {
    if(b.release()){
      acct.release(this, size);
      return true;
    }
    return false;
  }

  @Override
  public boolean release(int decrement) {
    if(b.release(decrement)){
      acct.release(this, size);
      return true;
    }
    return false;
  }

  @Override
  public int capacity() {
    return b.capacity();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    if(newCapacity < size){
      // TODO: once DRILL-336 is merged: do trim, update size and return
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public int maxCapacity() {
    return size;
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
    if(endianness != ByteOrder.BIG_ENDIAN) throw new UnsupportedOperationException("Drill buffers only support big endian.");
    return this;
  }

  @Override
  public ByteBuf unwrap() {
    return this;
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public int readerIndex() {
    return b.readerIndex();
  }

  @Override
  public ByteBuf readerIndex(int readerIndex) {
    b.readerIndex(readerIndex);
    return this;
  }

  @Override
  public int writerIndex() {
    return b.writerIndex();
  }

  @Override
  public ByteBuf writerIndex(int writerIndex) {
    b.writerIndex(writerIndex);
    return this;
  }

  @Override
  public ByteBuf setIndex(int readerIndex, int writerIndex) {
    b.setIndex(readerIndex, writerIndex);
    return this;
  }

  @Override
  public int readableBytes() {
    return b.readableBytes();
  }

  @Override
  public int writableBytes() {
    return b.writableBytes();
  }

  @Override
  public int maxWritableBytes() {
    return b.maxWritableBytes();
  }

  @Override
  public boolean isReadable() {
    return b.isReadable();
  }

  @Override
  public boolean isReadable(int size) {
    return b.isReadable(size);
  }

  @Override
  public boolean isWritable() {
    return b.isWritable();
  }

  @Override
  public boolean isWritable(int size) {
    return b.isWritable(size);
  }

  @Override
  public ByteBuf clear() {
    b.clear();
    return this;
  }

  @Override
  public ByteBuf markReaderIndex() {
    b.markReaderIndex();
    return this;
  }

  @Override
  public ByteBuf resetReaderIndex() {
    b.resetReaderIndex();
    return this;
  }

  @Override
  public ByteBuf markWriterIndex() {
    b.markWriterIndex();
    return this;
  }

  @Override
  public ByteBuf resetWriterIndex() {
    b.resetWriterIndex();
    return this;
  }

  @Override
  public ByteBuf discardReadBytes() {
    b.discardReadBytes();
    return this;
  }

  @Override
  public ByteBuf discardSomeReadBytes() {
    b.discardSomeReadBytes();
    return this;
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {
    b.ensureWritable(minWritableBytes);
    return this;
  }

  @Override
  public int ensureWritable(int minWritableBytes, boolean force) {
    return b.ensureWritable(minWritableBytes, false);
  }

  @Override
  public boolean getBoolean(int index) {
    return b.getBoolean(index);
  }

  @Override
  public byte getByte(int index) {
    return b.getByte(index);
  }

  @Override
  public short getUnsignedByte(int index) {
    return b.getUnsignedByte(index);
  }

  @Override
  public short getShort(int index) {
    return b.getShort(index);
  }

  @Override
  public int getUnsignedShort(int index) {
    return b.getUnsignedShort(index);
  }

  @Override
  public int getMedium(int index) {
    return b.getMedium(index);
  }

  @Override
  public int getUnsignedMedium(int index) {
    return b.getUnsignedMedium(index);
  }

  @Override
  public int getInt(int index) {
    return b.getInt(index);
  }

  @Override
  public long getUnsignedInt(int index) {
    return b.getUnsignedInt(index);
  }

  @Override
  public long getLong(int index) {
    return b.getLong(index);
  }

  @Override
  public char getChar(int index) {
    return b.getChar(index);
  }

  @Override
  public float getFloat(int index) {
    return b.getFloat(index);
  }

  @Override
  public double getDouble(int index) {
    return b.getDouble(index);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst) {
    b.getBytes(index, dst);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int length) {
    b.getBytes(index, dst, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    b.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst) {
    b.getBytes(index, dst);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    b.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    b.getBytes(index, dst);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    b.getBytes(index, out, length);
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return b.getBytes(index, out, length);
  }

  @Override
  public ByteBuf setBoolean(int index, boolean value) {
    b.setBoolean(index, value);
    return this;
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    b.setByte(index, value);
    return this;
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    b.setShort(index, value);
    return this;
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    b.setMedium(index, value);
    return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    b.setInt(index, value);
    return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    b.setLong(index, value);
    return this;
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    b.setChar(index, value);
    return this;
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    b.setFloat(index, value);
    return this;
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    b.setDouble(index, value);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src) {
    b.setBytes(index, src);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int length) {
    b.setBytes(index, src, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    b.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src) {
    b.setBytes(index, src);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    b.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    b.setBytes(index, src);
    return this;
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return b.setBytes(index, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return b.setBytes(index, in, length);
  }

  @Override
  public ByteBuf setZero(int index, int length) {
    b.setZero(index, length);
    return this;
  }

  @Override
  public boolean readBoolean() {
    return b.readBoolean();
  }

  @Override
  public byte readByte() {
    return b.readByte();
  }

  @Override
  public short readUnsignedByte() {
    return b.readUnsignedByte();
  }

  @Override
  public short readShort() {
    return b.readShort();
  }

  @Override
  public int readUnsignedShort() {
    return b.readUnsignedShort();
  }

  @Override
  public int readMedium() {
    return b.readMedium();
  }

  @Override
  public int readUnsignedMedium() {
    return b.readUnsignedMedium();
  }

  @Override
  public int readInt() {
    return b.readInt();
  }

  @Override
  public long readUnsignedInt() {
    return b.readUnsignedInt();
  }

  @Override
  public long readLong() {
    return b.readLong();
  }

  @Override
  public char readChar() {
    return b.readChar();
  }

  @Override
  public float readFloat() {
    return b.readFloat();
  }

  @Override
  public double readDouble() {
    return b.readDouble();
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
  public ByteBuf readBytes(ByteBuf dst) {
    b.readBytes(dst);
    return this;
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst, int length) {
    b.readBytes(dst, length);
    return this;
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
    b.readBytes(dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf readBytes(byte[] dst) {
    b.readBytes(dst);
    return this;
  }

  @Override
  public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
    b.readBytes(dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf readBytes(ByteBuffer dst) {
    b.readBytes(dst);
    return this;
  }

  @Override
  public ByteBuf readBytes(OutputStream out, int length) throws IOException {
    b.readBytes(out, length);
    return null;
  }

  @Override
  public int readBytes(GatheringByteChannel out, int length) throws IOException {
    return b.readBytes(out, length);
  }

  @Override
  public ByteBuf skipBytes(int length) {
    b.skipBytes(length);
    return this;
  }

  @Override
  public ByteBuf writeBoolean(boolean value) {
    b.writeBoolean(value);
    return this;
  }

  @Override
  public ByteBuf writeByte(int value) {
    b.writeByte(value);
    return this;
  }

  @Override
  public ByteBuf writeShort(int value) {
    b.writeShort(value);
    return this;
  }

  @Override
  public ByteBuf writeMedium(int value) {
    b.writeMedium(value);
    return this;
  }

  @Override
  public ByteBuf writeInt(int value) {
    b.writeInt(value);
    return this;
  }

  @Override
  public ByteBuf writeLong(long value) {
    b.writeLong(value);
    return this;
  }

  @Override
  public ByteBuf writeChar(int value) {
    b.writeChar(value);
    return this;
  }

  @Override
  public ByteBuf writeFloat(float value) {
    b.writeFloat(value);
    return this;
  }

  @Override
  public ByteBuf writeDouble(double value) {
    b.writeDouble(value);
    return this;
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src) {
    b.writeBytes(src);
    return this;
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int length) {
    b.writeBytes(src, length);
    return this;
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
    b.writeBytes(src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf writeBytes(byte[] src) {
    b.writeBytes(src);
    return this;
  }

  @Override
  public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
    b.writeBytes(src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf writeBytes(ByteBuffer src) {
    b.writeBytes(src);
    return this;
  }

  @Override
  public int writeBytes(InputStream in, int length) throws IOException {
    return b.writeBytes(in, length);
  }

  @Override
  public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
    return b.writeBytes(in, length);
  }

  @Override
  public ByteBuf writeZero(int length) {
    b.writeZero(length);
    return this;
  }

  @Override
  public int indexOf(int fromIndex, int toIndex, byte value) {
    return b.indexOf(fromIndex, toIndex, value);
  }

  @Override
  public int bytesBefore(byte value) {
    return b.bytesBefore(value);
  }

  @Override
  public int bytesBefore(int length, byte value) {
    return b.bytesBefore(length, value);
  }

  @Override
  public int bytesBefore(int index, int length, byte value) {
    return b.bytesBefore(index, length, value);
  }

  @Override
  public int forEachByte(ByteBufProcessor processor) {
    return b.forEachByte(processor);
  }

  @Override
  public int forEachByte(int index, int length, ByteBufProcessor processor) {
    return b.forEachByte(index, length, processor);
  }

  @Override
  public int forEachByteDesc(ByteBufProcessor processor) {
    return b.forEachByteDesc(processor);
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
    return b.forEachByteDesc(index, length, processor);
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
      return slice(b.readerIndex(), readableBytes());
  }

  @Override
  public ByteBuf slice(int index, int length) {
      if (length == 0) {
          return Unpooled.EMPTY_BUFFER;
      }

      return new SlicedByteBuf(this, index, length);
  }
  
  @Override
  public ByteBuf duplicate() {
    return new DuplicatedByteBuf(this);
  }

  @Override
  public int nioBufferCount() {
    return b.nioBufferCount();
  }

  @Override
  public ByteBuffer nioBuffer() {
    return b.nioBuffer();
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return b.nioBuffer(index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return b.internalNioBuffer(index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    return b.nioBuffers();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return b.nioBuffers(index, length);
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
    return b.hasMemoryAddress();
  }

  @Override
  public long memoryAddress() {
    return b.memoryAddress();
  }

  @Override
  public String toString(Charset charset) {
    return b.toString(charset);
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    return b.toString(index, length, charset);
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
  public String toString() {
    return "AccountingByteBuf [Inner buffer=" + b + ", size=" + size + "]";
  }

  @Override
  public int compareTo(ByteBuf buffer) {
    return b.compareTo(buffer);
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
  
  
}
