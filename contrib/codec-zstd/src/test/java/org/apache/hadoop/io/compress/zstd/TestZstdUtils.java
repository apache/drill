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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

public class TestZstdUtils {

  public static void copyResource(String fileName, File folder) throws Exception {
    InputStream in = TestZstdUtils.class.getClassLoader().getResourceAsStream(fileName);
    assertNotNull(in);
    File outfile = new File(folder.getAbsolutePath() + File.pathSeparator + fileName);

    FileOutputStream out = new FileOutputStream(outfile);
    try {
      IOUtils.copy(in, out);
    } finally {
      out.close();
      in.close();
    }
  }

  public static void generateLargeFile(String fileName, File folder) throws Exception {
    File outfile = new File(folder.getAbsolutePath() + File.pathSeparator + fileName);

    OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(outfile));
    try {
      for (int i = 0; i < 10000; i++) {
        String uuid = UUID.randomUUID().toString();
        w.write("This is line #" + i + " of data : " + uuid + "\n");
      }
    } finally {
      w.close();
    }
  }
}