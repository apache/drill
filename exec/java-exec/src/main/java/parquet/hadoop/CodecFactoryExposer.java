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
package parquet.hadoop;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DirectDecompressionCodec;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.util.ReflectionUtils;

import parquet.bytes.BytesInput;
import parquet.hadoop.metadata.CompressionCodecName;

public class CodecFactoryExposer{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodecFactoryExposer.class);

  private CodecFactory codecFactory;
  private final Map<String, org.apache.hadoop.io.compress.DirectDecompressionCodec> codecByName = new HashMap<String, org.apache.hadoop.io.compress.DirectDecompressionCodec>();
  private Configuration configuration;

  public CodecFactoryExposer(Configuration config){
    codecFactory = new CodecFactory(config);configuration = config;
  }

  public CodecFactory getCodecFactory() {
    return codecFactory;
  }

  public BytesInput decompress(BytesInput bytes, int uncompressedSize, CompressionCodecName codecName) throws IOException {
    return codecFactory.getDecompressor(codecName).decompress(bytes, uncompressedSize);
  }

  public BytesInput getBytesInput(ByteBuf uncompressedByteBuf, int uncompressedSize) throws IOException {
    ByteBuffer outBuffer=uncompressedByteBuf.nioBuffer(0, uncompressedSize);
    return new HadoopByteBufBytesInput(outBuffer, 0, outBuffer.limit());
  }

  public BytesInput decompress(CompressionCodecName codecName,
                               ByteBuf compressedByteBuf,
                               ByteBuf uncompressedByteBuf,
                               int compressedSize,
                               int uncompressedSize) throws IOException {
    ByteBuffer inpBuffer=compressedByteBuf.nioBuffer(0, compressedSize);
    ByteBuffer outBuffer=uncompressedByteBuf.nioBuffer(0, uncompressedSize);
    CompressionCodec c = getCodec(codecName);
    //TODO: Create the decompressor only once at init time.
    Class<?> cx = c.getClass();

    DirectDecompressionCodec d=null;
    DirectDecompressor decompr=null;

    if (DirectDecompressionCodec.class.isAssignableFrom(cx)) {
      d=(DirectDecompressionCodec)c;
    }

    if(d!=null) {
      decompr = d.createDirectDecompressor();
    }

    if(d!=null && decompr!=null){
      decompr.decompress(inpBuffer, outBuffer);
    }else{
      logger.warn("This Hadoop implementation does not support a " + codecName +
        " direct decompression codec interface. "+
        "Direct decompression is available only on *nix systems with Hadoop 2.3 or greater. "+
        "Read operations will be a little slower. ");
      BytesInput outBytesInp = this.decompress(
        new HadoopByteBufBytesInput(inpBuffer, 0, inpBuffer.limit()),
        uncompressedSize,
        codecName);
      // COPY the data back into the output buffer.
      // (DrillBufs can only refer to direct memory, so we cannot pass back a BytesInput backed
      // by a byte array).
      outBuffer.put(outBytesInp.toByteArray());
    }
    return new HadoopByteBufBytesInput(outBuffer, 0, outBuffer.limit());
  }

  private DirectDecompressionCodec getCodec(CompressionCodecName codecName) {
    String codecClassName = codecName.getHadoopCompressionCodecClassName();
    if (codecClassName == null) {
      return null;
    }
    DirectDecompressionCodec codec = codecByName.get(codecClassName);
    if (codec != null) {
      return codec;
    }

    try {
      Class<?> codecClass = Class.forName(codecClassName);
      codec = (DirectDecompressionCodec)ReflectionUtils.newInstance(codecClass, configuration);
      codecByName.put(codecClassName, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
  }

  public class HadoopByteBufBytesInput extends BytesInput{

    private final ByteBuffer byteBuf;
    private final int length;
    private final int offset;

    private HadoopByteBufBytesInput(ByteBuffer byteBuf, int offset, int length) {
      super();
      this.byteBuf = byteBuf;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      final WritableByteChannel outputChannel = Channels.newChannel(out);
      byteBuf.position(offset);
      ByteBuffer tempBuf = byteBuf.slice();
      tempBuf.limit(length);
      outputChannel.write(tempBuf);
    }

    @Override
    public ByteBuffer toByteBuffer() throws IOException {
      byteBuf.position(offset);
      ByteBuffer buf = byteBuf.slice();
      buf.limit(length);
      return buf;
    }

    @Override
    public long size() {
      return length;
    }
  }
}
