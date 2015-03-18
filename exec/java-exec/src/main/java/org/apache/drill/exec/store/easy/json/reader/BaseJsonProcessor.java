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

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.sym.BytesToNameCanonicalizer;
import com.fasterxml.jackson.core.util.BufferRecycler;
import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.RewindableUtf8Reader;

public abstract class BaseJsonProcessor implements JsonProcessor {

  protected final RewindableUtf8Reader parser;
  protected DrillBuf workBuf;

  public BaseJsonProcessor(DrillBuf workBuf) {
    this.workBuf = Preconditions.checkNotNull(workBuf);
    this.parser = Preconditions.checkNotNull(createParser());
  }

  protected RewindableUtf8Reader createParser() {
    final BufferRecycler recycler = new BufferRecycler();
    final IOContext context = new IOContext(recycler, this, false);
    final int features = JsonParser.Feature.collectDefaults() //
        | JsonParser.Feature.ALLOW_COMMENTS.getMask() //
        | JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES.getMask();

    final BytesToNameCanonicalizer can = BytesToNameCanonicalizer.createRoot();
    return new RewindableUtf8Reader<>(context, features, can.makeChild(JsonFactory.Feature.collectDefaults()), context.allocReadIOBuffer());
  }

  @Override
  public void setSource(InputStream is) throws IOException {
    parser.setInputStream(is);
  }
}
