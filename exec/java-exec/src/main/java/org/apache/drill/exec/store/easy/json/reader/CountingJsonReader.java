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
package org.apache.drill.exec.store.easy.json.reader;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.vector.complex.writer.BaseWriter;

public class CountingJsonReader extends BaseJsonProcessor {

  public CountingJsonReader(DrillBuf workBuf, boolean enableNanInf) {
    super(workBuf, enableNanInf);
  }

  @Override
  public ReadState write(BaseWriter.ComplexWriter writer) throws IOException {
    try {
      JsonToken token = lastSeenJsonToken;
      if (token == null || token == JsonToken.END_OBJECT){
        token = parser.nextToken();
      }
      lastSeenJsonToken = null;
      if (token == JsonToken.FIELD_NAME) {
        currentFieldName = parser.getText();
      }
      if (!parser.hasCurrentToken()) {
        return ReadState.END_OF_STREAM;
      } else if (token != JsonToken.START_OBJECT) {
        throw new com.fasterxml.jackson.core.JsonParseException(
            parser, String.format("Cannot read from the middle of a record. Current token was %s ", token));
      }
      writer.rootAsMap().bit("count").writeBit(1);
      parser.skipChildren();
    } catch (com.fasterxml.jackson.core.JsonParseException ex) {
      if (ignoreJSONParseError()) {
        if (processJSONException() == JsonExceptionProcessingState.END_OF_STREAM){
          return ReadState.JSON_RECORD_PARSE_EOF_ERROR;
        }
        else{
          return ReadState.JSON_RECORD_PARSE_ERROR;
        }
      } else {
        throw ex;
      }
    }
    return ReadState.WRITE_SUCCEED;
  }

  @Override
  public void ensureAtLeastOneField(BaseWriter.ComplexWriter writer) {

  }
}
