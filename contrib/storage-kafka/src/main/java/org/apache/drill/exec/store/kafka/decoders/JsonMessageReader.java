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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.kafka.KafkaStoragePlugin;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.netty.buffer.DrillBuf;

/**
 * MessageReader class which will convert ConsumerRecord into JSON and writes to
 * VectorContainerWriter of JsonReader
 *
 */
public class JsonMessageReader implements MessageReader {

  private static final Logger logger = LoggerFactory.getLogger(JsonMessageReader.class);
  private JsonReader jsonReader;
  private VectorContainerWriter writer;

  @Override
  public void init(DrillBuf buf, List<SchemaPath> columns, VectorContainerWriter writer, boolean allTextMode,
      boolean readNumbersAsDouble) {
    // set skipOuterList to false as it doesn't applicable for JSON records and it's only applicable for JSON files.
    this.jsonReader = new JsonReader.Builder(buf)
            .schemaPathColumns(columns)
            .allTextMode(allTextMode)
            .readNumbersAsDouble(readNumbersAsDouble)
            .build();
    this.writer = writer;
  }

  @Override
  public void readMessage(ConsumerRecord<?, ?> record) {
    try {
      byte[] recordArray = (byte[]) record.value();
      JsonObject jsonObj = (new JsonParser()).parse(new String(recordArray, Charsets.UTF_8)).getAsJsonObject();
      jsonObj.addProperty(KAFKA_TOPIC.getFieldName(), record.topic());
      jsonObj.addProperty(KAFKA_PARTITION_ID.getFieldName(), record.partition());
      jsonObj.addProperty(KAFKA_OFFSET.getFieldName(), record.offset());
      jsonObj.addProperty(KAFKA_TIMESTAMP.getFieldName(), record.timestamp());
      jsonObj.addProperty(KAFKA_MSG_KEY.getFieldName(), record.key() != null ? record.key().toString() : null);
      jsonReader.setSource(jsonObj.toString().getBytes(Charsets.UTF_8));
      jsonReader.write(writer);
    } catch (IOException e) {
      throw UserException.dataReadError(e).message(e.getMessage())
          .addContext("MessageReader", JsonMessageReader.class.getName()).build(logger);
    }
  }

  @Override
  public void ensureAtLeastOneField() {
    jsonReader.ensureAtLeastOneField(writer);
  }

  @Override
  public KafkaConsumer<byte[], byte[]> getConsumer(KafkaStoragePlugin plugin) {
    return plugin.registerConsumer(new KafkaConsumer<>(plugin.getConfig().getKafkaConsumerProps(),
        new ByteArrayDeserializer(), new ByteArrayDeserializer()));
  }

  @Override
  public void close() throws IOException {
    this.writer.clear();
    try {
      this.writer.close();
    } catch (Exception e) {
      logger.warn("Error while closing JsonMessageReader", e);
    }
  }

  @Override
  public String toString() {
    return "JsonMessageReader[jsonReader=" + jsonReader + "]";
  }
}
