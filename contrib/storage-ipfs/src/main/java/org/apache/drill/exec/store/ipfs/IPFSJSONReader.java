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


package org.apache.drill.exec.store.ipfs;

import io.ipfs.multihash.Multihash;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.store.easy.json.loader.JsonLoader;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class IPFSJSONReader implements ManagedReader<SchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(IPFSJSONReader.class);
  private final IPFSContext ipfsContext;
  private final Multihash block;
  private JsonLoader jsonLoader;

  public IPFSJSONReader(IPFSContext ipfsContext, Multihash block) {
    this.ipfsContext = ipfsContext;
    this.block = block;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    CustomErrorContext errorContext = new ChildErrorContext(negotiator.parentErrorContext()) {
      @Override
      public void addContext(UserException.Builder builder) {
        super.addContext(builder);
        builder.addContext("hash", block.toString());
      }
    };
    negotiator.setErrorContext(errorContext);

    IPFSHelper helper = ipfsContext.getIPFSHelper();

    byte[] rawDataBytes;
    if (block.equals(IPFSHelper.IPFS_NULL_OBJECT)) {
      // An empty ipfs object, but an empty string will make Jackson ObjectMapper fail
      // so treat it specially
      rawDataBytes = "[{}]".getBytes();
    } else {
      try {
        rawDataBytes = helper.getObjectDataTimeout(block);
      } catch (final IOException e) {
        throw UserException
            .dataReadError(e)
            .message("Failed to retrieve data from IPFS block")
            .addContext("Error message", e.getMessage())
            .addContext(errorContext)
            .build(logger);
      }
    }

    String rootJson = new String(rawDataBytes);
    int start = rootJson.indexOf("{");
    int end = rootJson.lastIndexOf("}");
    rootJson = rootJson.substring(start, end + 1);
    InputStream inStream = new ByteArrayInputStream(rootJson.getBytes());

    try {
      jsonLoader = new JsonLoaderImpl.JsonLoaderBuilder()
          .resultSetLoader(negotiator.build())
          .standardOptions(negotiator.queryOptions())
          .errorContext(errorContext)
          .fromStream(inStream)
          .build();
    } catch (Throwable t) {

      // Paranoia: ensure stream is closed if anything goes wrong.
      // After this, the JSON loader will close the stream.
      AutoCloseables.closeSilently(inStream);
      throw t;
    }

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
