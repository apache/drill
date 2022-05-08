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
package org.apache.drill.exec.store.easy.json;

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;

import java.io.InputStream;

/**
 * EVF based JSON reader which uses input stream as data source.
 */
public class JsonStreamBatchReader implements ManagedReader<SchemaNegotiator> {

  private JsonLoader jsonLoader;
  private final InputStream source;

  public JsonStreamBatchReader(InputStream source) {
    this.source = source;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {

    CustomErrorContext errorContext = new ChildErrorContext(negotiator.parentErrorContext());
    negotiator.setErrorContext(errorContext);

    // Create the JSON loader (high-level parser).
    jsonLoader = new JsonLoaderBuilder()
        .resultSetLoader(negotiator.build())
        .standardOptions(negotiator.queryOptions())
        .providedSchema(negotiator.providedSchema())
        .errorContext(errorContext)
        .fromStream(source)
        .build();
    return true;
  }

  @Override
  public boolean next() {
    return jsonLoader.readBatch();
  }

  @Override
  public void close() {
    if (jsonLoader != null) {
      jsonLoader.close();
      jsonLoader = null;
    }
  }

}
