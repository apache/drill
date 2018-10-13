/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.compress.zstd.ZstdCompressionInputStream;
import org.apache.hadoop.io.compress.zstd.ZstdCompressor;
import org.apache.hadoop.io.compress.zstd.ZstdDecompressor;

/**
 * This class creates snappy compressors/decompressors.
 */
public class ZstdCodec implements Configurable, CompressionCodec, DirectDecompressionCodec {
  Configuration conf;

  /**
   * Set the configuration to be used by this object.
   *
   * @param conf
   *          the configuration object.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this object.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Are the native snappy libraries loaded & initialized?
   */
  public static void checkNativeCodeLoaded() {

    if (!ZstdCompressor.isNativeCodeLoaded()) {
      throw new RuntimeException("native zstd library not available: ZstdCompressor has not been loaded.");
    }
    if (!ZstdDecompressor.isNativeCodeLoaded()) {
      throw new RuntimeException("native zstd library not available: ZstdDecompressor has not been loaded.");
    }
  }

  public static boolean isNativeCodeLoaded() {
    return ZstdCompressor.isNativeCodeLoaded() && ZstdDecompressor.isNativeCodeLoaded();
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out
   *          the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return CompressionCodec.Util.createOutputStreamWithCodecPool(this, conf, out);
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out
   *          the location for the final output stream
   * @param compressor
   *          compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public Class<? extends Compressor> getCompressorType() {
    checkNativeCodeLoaded();
    return ZstdCompressor.class;
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given input
   * stream.
   *
   * @param in
   *          the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return CompressionCodec.Util.createInputStreamWithCodecPool(this, conf, in);
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}.
   *
   * @param in
   *          the stream to read compressed bytes from
   * @param decompressor
   *          decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
    checkNativeCodeLoaded();
    return new ZstdCompressionInputStream(in, decompressor,
        conf.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
            CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT));
  }

  /**
   * Get the type of {@link Decompressor} needed by this
   * {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    checkNativeCodeLoaded();
    return ZstdDecompressor.class;
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public Decompressor createDecompressor() {
    checkNativeCodeLoaded();
    return new ZstdDecompressor();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DirectDecompressor createDirectDecompressor() {
    // jccote, I did not implement the direct version of the decompressor
    // but if we want it we can look at the SnappyDirectDecompressor.
    throw new UnsupportedOperationException();
  }

  /**
   * Get the default filename extension for this kind of compression.
   *
   * @return <code>.snappy</code>.
   */
  @Override
  public String getDefaultExtension() {
    return ".zst";
  }
}
