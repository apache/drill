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
package org.apache.drill.exec.store.msgpack;

import java.io.IOException;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

public class CountingMsgpackReader extends BaseMsgpackReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CountingMsgpackReader.class);

  public CountingMsgpackReader() {
  }

  @Override
  public ReadState write(ComplexWriter writer) throws IOException {
    if (!unpacker.hasNext()) {
      return ReadState.END_OF_STREAM;
    }

    Value v = unpacker.unpackValue();
    ValueType type = v.getValueType();
    switch (type) {
    case MAP:
      writer.rootAsMap().bit("count").writeBit(1);
      break;
    default:
      throw new DrillRuntimeException("Root objects must be of MAP type. Found: " + type);
    }

    return ReadState.WRITE_SUCCEED;
  }

  public UserException.Builder getExceptionWithContext(UserException.Builder exceptionBuilder, String field, String msg,
      Object... args) {
    return null;
  }

  public UserException.Builder getExceptionWithContext(Throwable exception, String field, String msg, Object... args) {
    return null;
  }

  @Override
  public void ensureAtLeastOneField(ComplexWriter writer) {
  }
}