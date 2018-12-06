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

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

public class TestZstdDecompressor {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testZstdStream() throws Exception {
    TestZstdUtils.copyResource("emp.json", folder.getRoot());
    testZstdStream("emp.json");
    TestZstdUtils.generateLargeFile("large.data", folder.getRoot());
    testZstdStream("large.data");
  }

  public void testZstdStream(String fileName) throws Exception {

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

    ZstdInputStream compressedIn = new ZstdInputStream(new FileInputStream(compressedfile));
    File decompressedFile = new File(folder.getRoot() + File.pathSeparator + fileName + ".decompressed");
    FileOutputStream decompressedOut = new FileOutputStream(decompressedFile);
    try {
      IOUtils.copy(compressedIn, decompressedOut);
    } finally {
      decompressedOut.close();
      compressedIn.close();
    }

    FileUtils.contentEquals(originalFile, decompressedFile);
  }

  @Test
  public void testZstdDecompressor() throws Exception {
    TestZstdUtils.copyResource("emp.json", folder.getRoot());
    testZstdDecompressor("emp.json");
    TestZstdUtils.generateLargeFile("large.data", folder.getRoot());
    testZstdDecompressor("large.data");
  }

  public void testZstdDecompressor(String fileName) throws Exception {

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
    try {
      ZstdDecompressor z = new ZstdDecompressor();
      byte[] compressed = new byte[4 * 1024];
      byte[] uncompressed = new byte[8 * 1024];

      Stopwatch stopwatch = Stopwatch.createStarted();
      int n = compressedIn.read(compressed);
      if (n > 0) {
        z.setInput(compressed, 0, n);
      }
      while (n > 0) {
        int w = 1;
        while (w > 0) {
          w = z.decompress(uncompressed, 0, uncompressed.length);
          decompressedOut.write(uncompressed, 0, w);
        }
        n = compressedIn.read(compressed);
        if (n > 0) {
          z.setInput(compressed, 0, n);
        }
      }
      stopwatch.stop();
      System.out.println("took: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } finally {
      if (compressedIn != null) {
        compressedIn.close();
      }
      if (decompressedOut != null) {
        decompressedOut.close();
      }
    }
    FileUtils.contentEquals(originalFile, decompressedFile);
  }

}
