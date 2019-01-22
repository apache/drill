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
package org.apache.drill.exec.physical.impl.scan.framework;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;

/**
 * Basic scan framework for simple non-file readers. Includes only
 * schema negotiation, but no implicit columns. Readers are assumed
 * to be created ahead of time and passed into the framework
 * in the constructor.
 */

public class BasicScanFramework extends AbstractScanFramework<SchemaNegotiator> {

  private Iterator<ManagedReader<SchemaNegotiator>> iterator;

  public BasicScanFramework(List<SchemaPath> projection,
      Iterator<ManagedReader<SchemaNegotiator>> iterator) {
    super(projection);
    this.iterator = iterator;
  }

  @Override
  public RowBatchReader nextReader() {
    if (! iterator.hasNext()) {
      return null;
    }
    ManagedReader<SchemaNegotiator> reader = iterator.next();
    return new ShimBatchReader<SchemaNegotiator>(this, reader);
  }

  @Override
  public boolean openReader(ShimBatchReader<SchemaNegotiator> shim,
      ManagedReader<SchemaNegotiator> reader) {
    SchemaNegotiatorImpl schemaNegotiator = new SchemaNegotiatorImpl(this, shim);
    return reader.open(schemaNegotiator);
  }
}
