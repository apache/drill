/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.rops;

import java.io.IOException;

import org.apache.drill.common.logical.data.Write;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class JSONWriter extends BaseSinkROP<Write> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONWriter.class);

  private FSDataOutputStream output;
  private JSONDataWriter writer;

  public JSONWriter(Write config) {
    super(config);
  }

  @Override
  protected void setupSink() throws IOException {
    final Path sinkPath = new Path(config.getFileName());
    final String sinkScheme = sinkPath.toUri().getScheme();

    if ("console".equals(sinkScheme)) {
      final String std = sinkPath.getName();
      if ("stdout".equals(std)) {
        output = new FSDataOutputStream(System.out);
      } else if ("stderr".equals(std)) {
        output = new FSDataOutputStream(System.err);
      } else {
        throw new IOException(std + ": Invalid console sink");
      }
    } else {
      final FileSystem fs = FileSystem.get(new Configuration());
      output = fs.create(new Path(config.getFileName()));
    }
    writer = new JSONDataWriter(output);
  }

  @Override
  public long sinkRecord(RecordPointer r) throws IOException {
    r.write(writer);
    return output.getPos();
  }

  @Override
  public void cleanup(RunOutcome.OutcomeType outcome) {
    try {
      writer.close();
      output.close();
    } catch (IOException e) {
      logger.warn("Error while closing output stream.", e);
    }
  }

}
