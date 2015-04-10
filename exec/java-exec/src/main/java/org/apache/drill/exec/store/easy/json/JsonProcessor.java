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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import com.fasterxml.jackson.databind.JsonNode;

public interface JsonProcessor {

  public static enum ReadState {
    END_OF_STREAM,
    WRITE_SUCCEED
  }

  ReadState write(BaseWriter.ComplexWriter writer) throws IOException;

  void setSource(InputStream is) throws IOException;
  void setSource(JsonNode node);

  void ensureAtLeastOneField(BaseWriter.ComplexWriter writer);

  public UserException.Builder getExceptionWithContext(UserException.Builder exceptionBuilder,
                                                       String field,
                                                       String msg,
                                                       Object... args);

  public UserException.Builder getExceptionWithContext(Throwable exception,
                                                       String field,
                                                       String msg,
                                                       Object... args);

}
