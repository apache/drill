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

package org.apache.drill.exec.vector.complex.fn;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.fasterxml.jackson.core.JsonParseException;

public class JsonReaderWithState {

  public static enum WriteState {
    WRITE_SUCCEED, WRITE_FAILED, NO_MORE
  }

  private Reader reader;
  private JsonRecordSplitter splitter;
  private JsonReader jsonReader;

  public JsonReaderWithState(JsonRecordSplitter splitter, DrillBuf workspace, List<SchemaPath> columns, boolean allTextMode) throws IOException{
    this.splitter = splitter;
    reader = splitter.getNextReader();
    jsonReader = new JsonReader(workspace, columns, allTextMode);
  }

  public JsonReaderWithState(JsonRecordSplitter splitter) throws IOException{
    this(splitter, null, GroupScan.ALL_COLUMNS, false);
  }

  public List<SchemaPath> getNullColumns() {
    return jsonReader.getNullColumns();
  }

  public WriteState write(ComplexWriter writer) throws JsonParseException, IOException {
    if (reader == null) {
      reader = splitter.getNextReader();
      if (reader == null)
        return WriteState.NO_MORE;

    }

    jsonReader.write(reader, writer);

    if (!writer.ok()) {
      reader.reset();
      return WriteState.WRITE_FAILED;
    } else {
      reader = null;
      return WriteState.WRITE_SUCCEED;
    }
  }
}
