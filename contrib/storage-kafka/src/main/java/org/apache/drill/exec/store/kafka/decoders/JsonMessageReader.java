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
package org.apache.drill.exec.store.kafka.decoders;

import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_MSG_KEY;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_OFFSET;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_PARTITION_ID;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_TIMESTAMP;
import static org.apache.drill.exec.store.kafka.MetaDataField.KAFKA_TOPIC;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.reader.BaseJsonProcessor;
import org.apache.drill.exec.store.kafka.KafkaStoragePlugin;
import org.apache.drill.exec.store.kafka.ReadOptions;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;

import io.netty.buffer.DrillBuf;

/**
 * MessageReader class which will convert ConsumerRecord into JSON and writes to
 * VectorContainerWriter of JsonReader
 */
public class JsonMessageReader implements MessageReader {

  private static final Logger logger = LoggerFactory.getLogger(JsonMessageReader.class);
  private JsonReader jsonReader;
  private VectorContainerWriter writer;
  private ObjectMapper objectMapper;

  @Override
  public void init(DrillBuf buf, List<SchemaPath> columns, VectorContainerWriter writer, ReadOptions readOptions) {
    // set skipOuterList to false as it doesn't applicable for JSON records and it's only applicable for JSON files.
    this.jsonReader = new JsonReader.Builder(buf)
      .schemaPathColumns(columns)
      .allTextMode(readOptions.isAllTextMode())
      .readNumbersAsDouble(readOptions.isReadNumbersAsDouble())
      .enableNanInf(readOptions.isAllowNanInf())
      .enableEscapeAnyChar(readOptions.isAllowEscapeAnyChar())
      .build();
    jsonReader.setIgnoreJSONParseErrors(readOptions.isSkipInvalidRecords());
    this.writer = writer;
    this.objectMapper = BaseJsonProcessor.getDefaultMapper()
      .configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, readOptions.isAllowNanInf())
      .configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, readOptions.isAllowEscapeAnyChar());
  }

  @Override
  public boolean readMessage(ConsumerRecord<?, ?> record) {
    byte[] recordArray = (byte[]) record.value();
    String data = new String(recordArray, Charsets.UTF_8);
    try {
      JsonNode jsonNode = objectMapper.readTree(data);
      if (jsonNode != null && jsonNode.isObject()) {
        ObjectNode objectNode = (ObjectNode) jsonNode;
        objectNode.put(KAFKA_TOPIC.getFieldName(), record.topic());
        objectNode.put(KAFKA_PARTITION_ID.getFieldName(), record.partition());
        objectNode.put(KAFKA_OFFSET.getFieldName(), record.offset());
        objectNode.put(KAFKA_TIMESTAMP.getFieldName(), record.timestamp());
        objectNode.put(KAFKA_MSG_KEY.getFieldName(), record.key() != null ? record.key().toString() : null);
      } else {
        throw new IOException("Unsupported node type: " + (jsonNode == null ? "NO CONTENT" : jsonNode.getNodeType()));
      }
      jsonReader.setSource(jsonNode);
      return convertJsonReadState(jsonReader.write(writer));
    } catch (IOException | IllegalArgumentException e) {
      String message = String.format("JSON record %s: %s", data, e.getMessage());
      if (jsonReader.ignoreJSONParseError()) {
        logger.debug("Skipping {}", message, e);
        return false;
      }
      throw UserException.dataReadError(e)
        .message("Failed to read " + message)
        .addContext("MessageReader", JsonMessageReader.class.getName())
        .build(logger);
    }
  }

  @Override
  public void ensureAtLeastOneField() {
    jsonReader.ensureAtLeastOneField(writer);
  }

  @Override
  public KafkaConsumer<byte[], byte[]> getConsumer(KafkaStoragePlugin plugin) {
    return new KafkaConsumer<>(plugin.getConfig().getKafkaConsumerProps(),
      new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  @Override
  public void close() {
    this.writer.clear();
    try {
      this.writer.close();
    } catch (Exception e) {
      logger.warn("Error while closing JsonMessageReader: {}", e.getMessage());
    }
  }

  @Override
  public String toString() {
    return "JsonMessageReader[jsonReader=" + jsonReader + "]";
  }

  /**
   * Converts {@link JsonProcessor.ReadState} into true / false result.
   *
   * @param jsonReadState JSON reader read state
   * @return true if read was successful, false otherwise
   * @throws IllegalArgumentException if unexpected read state was encountered
   */
  private boolean convertJsonReadState(JsonProcessor.ReadState jsonReadState) {
    switch (jsonReadState) {
      case WRITE_SUCCEED:
      case END_OF_STREAM:
        return true;
      case JSON_RECORD_PARSE_ERROR:
      case JSON_RECORD_PARSE_EOF_ERROR:
        return false;
      default:
        throw new IllegalArgumentException("Unexpected JSON read state: " + jsonReadState);
    }
  }
}
