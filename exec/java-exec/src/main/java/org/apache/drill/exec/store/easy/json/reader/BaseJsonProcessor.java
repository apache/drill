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
package org.apache.drill.exec.store.easy.json.reader;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.exec.store.easy.json.JsonProcessor;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.UserException;

public abstract class BaseJsonProcessor implements JsonProcessor {

  private static final ObjectMapper MAPPER = new ObjectMapper()
    .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
    .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

  protected JsonParser parser;
  protected DrillBuf workBuf;

  public BaseJsonProcessor(DrillBuf workBuf) {
    workBuf = Preconditions.checkNotNull(workBuf);
  }

  @Override
  public void setSource(InputStream is) throws IOException {
    parser = MAPPER.getFactory().createParser(is);
  }

  @Override
  public void setSource(JsonNode node) {
    this.parser = new TreeTraversingParser(node);
  }

  @Override
  public UserException.Builder getExceptionWithContext(UserException.Builder exceptionBuilder,
                                                       String field,
                                                       String msg,
                                                       Object... args) {
    if (msg != null) {
      exceptionBuilder.message(msg, args);
    }
    if(field != null) {
      exceptionBuilder.pushContext("Field ", field);
    }
    exceptionBuilder.pushContext("Column ", parser.getCurrentLocation().getColumnNr()+1)
            .pushContext("Line ", parser.getCurrentLocation().getLineNr());
    return exceptionBuilder;
  }

  @Override
  public UserException.Builder getExceptionWithContext(Throwable e,
                                                       String field,
                                                       String msg,
                                                       Object... args) {
    UserException.Builder exceptionBuilder = UserException.dataReadError(e);
    return getExceptionWithContext(exceptionBuilder, field, msg, args);
  }
}
