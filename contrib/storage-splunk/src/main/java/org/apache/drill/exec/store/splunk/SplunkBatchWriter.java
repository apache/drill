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


import com.splunk.IndexCollection;
import com.splunk.Service;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SplunkBatchWriter extends AbstractRecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(SplunkBatchWriter.class);

  private final UserCredentials userCredentials;
  private final List<String> tableIdentifier;
  private final SplunkWriter config;
  private final Service splunkService;


  public SplunkBatchWriter(UserCredentials userCredentials, List<String> tableIdentifier, SplunkWriter config) {
    this.config = config;
    this.tableIdentifier = tableIdentifier;
    this.userCredentials = userCredentials;

    SplunkConnection connection = new SplunkConnection(config.getPluginConfig(), userCredentials.getUserName());
    this.splunkService = connection.connect();
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    // No op
  }

  /**
   * Update the schema in RecordWriter. Called at least once before starting writing the records. In this case,
   * we add the index to Splunk here. Splunk's API is a little sparse and doesn't really do much in the way
   * of error checking or providing feedback if the operation fails.
   *
   * @param batch {@link VectorAccessible} The incoming batch
   */
  @Override
  public void updateSchema(VectorAccessible batch) {
    logger.debug("Updating schema for Splunk");

    //Get the collection of indexes
    IndexCollection indexes = splunkService.getIndexes();
    try {
      indexes.create(tableIdentifier.get(0));
    } catch (Exception e) {
      // We have to catch a generic exception here, as Splunk's SDK does not really provide any kind of
      // failure messaging.
      throw UserException.systemError(e)
        .message("Error creating new index in Splunk plugin: " + e.getMessage())
        .build(logger);
    }
  }

  /**
   * Called before starting writing fields in a record.
   *
   * @throws IOException
   */
  @Override
  public void startRecord() throws IOException {
    logger.debug("Starting record");
  }

  /**
   * Called after adding all fields in a particular record are added using add{TypeHolder}
   * (fieldId, TypeHolder) methods.
   *
   * @throws IOException
   */
  @Override
  public void endRecord() throws IOException {
    logger.debug("Ending record");
  }

  @Override
  public void abort() throws IOException {

  }

  @Override
  public void cleanup() throws IOException {

  }
}
