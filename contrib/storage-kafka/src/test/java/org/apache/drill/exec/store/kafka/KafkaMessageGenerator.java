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
package org.apache.drill.exec.store.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class KafkaMessageGenerator {

  private static final Logger logger = LoggerFactory.getLogger(KafkaMessageGenerator.class);
  private Properties producerProperties = new Properties();

  public KafkaMessageGenerator (final String broker, Class<?> valueSerializer) {
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
    producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
    producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "drill-test-kafka-client");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
    producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); //So that retries do not cause duplicates
  }

  public void populateAvroMsgIntoKafka(String topic, int numMsg) throws IOException {
    KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(producerProperties);
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(Resources.getResource("drill-avro-test.avsc").openStream());
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    Random rand = new Random();
    for (int i = 0; i < numMsg; ++i) {
      builder.set("key1", UUID.randomUUID().toString());
      builder.set("key2", rand.nextInt());
      builder.set("key3", rand.nextBoolean());

      List<Integer> list = Lists.newArrayList();
      list.add(rand.nextInt(100));
      list.add(rand.nextInt(100));
      list.add(rand.nextInt(100));
      builder.set("key5", list);

      Map<String, Double> map = Maps.newHashMap();
      map.put("key61", rand.nextDouble());
      map.put("key62", rand.nextDouble());
      builder.set("key6", map);

      Record producerRecord = builder.build();

      ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(topic, producerRecord);
      producer.send(record);
    }
    producer.close();
  }

  public void populateJsonMsgIntoKafka(String topic, int numMsg) throws InterruptedException, ExecutionException {
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
    Random rand = new Random();
    try {
      for (int i = 0; i < numMsg; ++i) {
        JsonObject object = new JsonObject();
        object.addProperty("key1", UUID.randomUUID().toString());
        object.addProperty("key2", rand.nextInt());
        object.addProperty("key3", rand.nextBoolean());

        JsonArray element2 = new JsonArray();
        element2.add(new JsonPrimitive(rand.nextInt(100)));
        element2.add(new JsonPrimitive(rand.nextInt(100)));
        element2.add(new JsonPrimitive(rand.nextInt(100)));

        object.add("key5", element2);

        JsonObject element3 = new JsonObject();
        element3.addProperty("key61", rand.nextDouble());
        element3.addProperty("key62", rand.nextDouble());
        object.add("key6", element3);

        ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic, object.toString());
        logger.info("Publishing message : {}", message);
        Future<RecordMetadata> future = producer.send(message);
        logger.info("Committed offset of the message : {}", future.get().offset());
      }
    } catch (Throwable th) {
      logger.error(th.getMessage(), th);
      throw new DrillRuntimeException(th.getMessage(), th);
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

  public void populateJsonMsgWithTimestamps(String topic, int numMsg) {
    KafkaProducer<String, String> producer = null;
    Random rand = new Random();
    try {
      producer = new KafkaProducer<String, String>(producerProperties);
      int halfCount = numMsg / 2;

      for(PartitionInfo tpInfo : producer.partitionsFor(topic)) {
        for (int i = 1; i <= numMsg; ++i) {
          JsonObject object = new JsonObject();
          object.addProperty("stringKey", UUID.randomUUID().toString());
          object.addProperty("intKey", numMsg - i);
          object.addProperty("boolKey", i % 2 == 0);

          long timestamp = i < halfCount ? (halfCount - i) : i;
          ProducerRecord<String, String> message =
              new ProducerRecord<String, String>(tpInfo.topic(), tpInfo.partition(), timestamp, "key"+i, object.toString());
          logger.info("Publishing message : {}", message);
          Future<RecordMetadata> future = producer.send(message);
          logger.info("Committed offset of the message : {}", future.get().offset());
        }

      }
    } catch (Throwable th) {
      logger.error(th.getMessage(), th);
      throw new DrillRuntimeException(th.getMessage(), th);
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

}
