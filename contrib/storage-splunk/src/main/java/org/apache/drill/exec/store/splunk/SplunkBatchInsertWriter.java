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

package org.apache.drill.exec.store.splunk;

import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.record.VectorAccessible;

import java.util.List;

public class SplunkBatchInsertWriter extends SplunkBatchWriter {

  public SplunkBatchInsertWriter(UserCredentials userCredentials, List<String> tableIdentifier, SplunkWriter config) {
    super(userCredentials, tableIdentifier, config);
    String indexName = tableIdentifier.get(0);
    destinationIndex = splunkService.getIndexes().get(indexName);
  }

  @Override
  public void updateSchema(VectorAccessible batch) {
    // No op
  }
}
