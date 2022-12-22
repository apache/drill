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


import com.splunk.Index;
import com.splunk.IndexCollection;
import com.splunk.ReceiverBehavior;
import com.splunk.Service;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.util.Text;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SplunkBatchWriter extends AbstractRecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(SplunkBatchWriter.class);
  private static final String DEFAULT_SOURCETYPE = "drill";
  private final UserCredentials userCredentials;
  private final List<String> tableIdentifier;
  private final SplunkWriter config;
  protected final Service splunkService;
  private JSONObject splunkEvent;
  private final List<JSONObject> eventBuffer;
  protected Index destinationIndex;
  private int recordCount;


  public SplunkBatchWriter(UserCredentials userCredentials, List<String> tableIdentifier, SplunkWriter config) {
    this.config = config;
    this.tableIdentifier = tableIdentifier;
    this.userCredentials = userCredentials;
    this.splunkEvent = new JSONObject();
    eventBuffer = new ArrayList<>();
    SplunkConnection connection = new SplunkConnection(config.getPluginConfig(), userCredentials.getUserName());
    this.splunkService = connection.connect();
  }

  @Override
  public void init(Map<String, String> writerOptions) {
    // No op
  }

  /**
   * Update the schema in RecordWriter. Called before starting writing the records. In this case,
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
      String indexName = tableIdentifier.get(0);
      indexes.create(indexName);
      destinationIndex = splunkService.getIndexes().get(indexName);
    } catch (Exception e) {
      // We have to catch a generic exception here, as Splunk's SDK does not really provide any kind of
      // failure messaging.
      throw UserException.systemError(e)
        .message("Error creating new index in Splunk plugin: " + e.getMessage())
        .build(logger);
    }
  }


  @Override
  public void startRecord() {
    logger.debug("Starting record");
    // Ensure that the new record is empty.
    splunkEvent = new JSONObject();
  }

  @Override
  public void endRecord() {
    logger.debug("Ending record");
    recordCount++;

    // Put event in buffer
    eventBuffer.add(splunkEvent);

    // Write the event to the Splunk index
    if (recordCount >= config.getPluginConfig().getWriterBatchSize()) {
      try {
        writeEvents();
      } catch (IOException e) {
        throw  UserException.dataWriteError(e)
            .message("Error writing data to Splunk: " + e.getMessage())
            .build(logger);
      }

      // Reset record count
      recordCount = 0;
    }
  }


  /*
  args – Optional arguments for this stream. Valid parameters are: "host", "host_regex", "source", and "sourcetype".
   */
  @Override
  public void abort() {
    logger.debug("Aborting writing records to Splunk.");
    // No op
  }

  @Override
  public void cleanup() {
    try {
      writeEvents();
    } catch (IOException e) {
      throw  UserException.dataWriteError(e)
          .message("Error writing data to Splunk: " + e.getMessage())
          .build(logger);
    }
  }

  private void writeEvents() throws IOException {
    // Open the socket and stream, set up a timestamp
    destinationIndex.attachWith(new ReceiverBehavior() {
      public void run(OutputStream stream) throws IOException {
        String eventText;

        for (JSONObject tempEvent : eventBuffer) {
          eventText = tempEvent.toJSONString() + "'\r\n";
          stream.write(eventText.getBytes(StandardCharsets.UTF_8));
        }
      }
    });

    // Clear buffer
    eventBuffer.clear();
  }


  @Override
  public FieldConverter getNewNullableIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewSmallIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewTinyIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewFloat4Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewVarDecimalConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableTimeConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewTimeConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VarCharSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewTimeStampConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VarCharSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewNullableBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewBitConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ScalarSplunkConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexFieldConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewUnionConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexFieldConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewRepeatedMapConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexFieldConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewRepeatedListConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexFieldConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewDictConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexFieldConverter(fieldId, fieldName, reader);
  }

  @Override
  public FieldConverter getNewRepeatedDictConverter(int fieldId, String fieldName, FieldReader reader) {
    return new ComplexFieldConverter(fieldId, fieldName, reader);
  }

  public class VarCharSplunkConverter extends FieldConverter {

    public VarCharSplunkConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      Text text = reader.readText();
      if (text != null && text.getLength() > 0) {
        byte[] bytes =text.copyBytes();
        splunkEvent.put(fieldName, new String(bytes));
      }
    }
  }

  public class ScalarSplunkConverter extends FieldConverter {
    public ScalarSplunkConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() {
      splunkEvent.put(fieldName, String.valueOf(reader.readObject()));
    }
  }

  public class ComplexFieldConverter extends FieldConverter {
    public ComplexFieldConverter(int fieldID, String fieldName, FieldReader reader) {
      super(fieldID, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
      splunkEvent.put(fieldName, reader.readObject());
    }
  }
}
