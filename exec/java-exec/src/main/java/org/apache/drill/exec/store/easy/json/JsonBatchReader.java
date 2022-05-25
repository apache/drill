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

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EVF based reader. It is using by default to read JSON files - store.json.enable_v2_reader = true
 * The old deprecated one is {@link JSONRecordReader}
 */
public class JsonBatchReader implements ManagedReader<FileSchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(JsonBatchReader.class);

  private JsonLoader jsonLoader;

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    DrillFileSystem fileSystem = negotiator.fileSystem();
    FileSplit split = negotiator.split();

    InputStream stream;
    try {
      stream = fileSystem.openPossiblyCompressedStream(split.getPath());
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .addContext("Failure to open JSON file", split.getPath().toString())
          .build(logger);
    }
    CustomErrorContext errorContext = new ChildErrorContext(negotiator.parentErrorContext()) {
      @Override
      public void addContext(UserException.Builder builder) {
        super.addContext(builder);
        builder.addContext("File name", split.getPath().toString());
      }
    };
    negotiator.setErrorContext(errorContext);

    // Create the JSON loader (high-level parser).
    jsonLoader = new JsonLoaderBuilder()
        .resultSetLoader(negotiator.build())
        .standardOptions(negotiator.queryOptions())
        .providedSchema(negotiator.providedSchema())
        .errorContext(errorContext)
        .fromStream(stream)
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
