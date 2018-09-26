package org.apache.hadoop.io.compress.zstd;
/*
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.github.luben.zstd.ZstdOutputStream;

public class TestZstdCompressionInputStream {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testZstdCompressionInputStream() throws Exception {
    TestZstdUtils.copyResource("emp.json", folder.getRoot());
    testZstdCompressionInputStream("emp.json");
    TestZstdUtils.generateLargeFile("large.data", folder.getRoot());
    testZstdCompressionInputStream("large.data");
  }

  public void testZstdCompressionInputStream(String fileName) throws Exception {

    File originalFile = new File(folder.getRoot() + File.pathSeparator + fileName);
    FileInputStream originalIn = new FileInputStream(originalFile);

    File compressedfile = new File(folder.getRoot() + File.pathSeparator + fileName + ".zstd");
    ZstdOutputStream compressedOut = new ZstdOutputStream(new FileOutputStream(compressedfile));
    try {
      IOUtils.copy(originalIn, compressedOut);
    } finally {
      compressedOut.close();
      originalIn.close();
    }

    FileInputStream compressedIn = new FileInputStream(compressedfile);
    File decompressedFile = new File(folder.getRoot() + File.pathSeparator + fileName + ".decompressed");
    FileOutputStream decompressedOut = new FileOutputStream(decompressedFile);

    ZstdDecompressor decompressor = new ZstdDecompressor();
    ZstdCompressionInputStream zstdCompressionInputStream = new ZstdCompressionInputStream(compressedIn, decompressor,
        1 * 1024);
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      IOUtils.copyLarge(zstdCompressionInputStream, decompressedOut);
      stopwatch.stop();
      System.out.println("took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } finally {
      zstdCompressionInputStream.close();
      zstdCompressionInputStream.close();
    }

    FileUtils.contentEquals(originalFile, decompressedFile);
  }

}
