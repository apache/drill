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

import java.util.Map;
import java.util.Properties;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(KafkaStoragePluginConfig.NAME)
public class KafkaStoragePluginConfig extends StoragePluginConfig {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStoragePluginConfig.class);
  public static final String NAME = "kafka";
  private Properties kafkaConsumerProps;

  @JsonCreator
  public KafkaStoragePluginConfig(@JsonProperty("kafkaConsumerProps") Map<String, String> kafkaConsumerProps) {
    this.kafkaConsumerProps = new Properties();
    this.kafkaConsumerProps.putAll(kafkaConsumerProps);
    logger.debug("Kafka Consumer Props {}", this.kafkaConsumerProps);
  }

  public Properties getKafkaConsumerProps() {
    return kafkaConsumerProps;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((kafkaConsumerProps == null) ? 0 : kafkaConsumerProps.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    KafkaStoragePluginConfig other = (KafkaStoragePluginConfig) obj;
    if (kafkaConsumerProps == null && other.kafkaConsumerProps == null) {
      return true;
    }
    if (kafkaConsumerProps == null || other.kafkaConsumerProps == null) {
      return false;
    }
    return kafkaConsumerProps.equals(other.kafkaConsumerProps);
  }

}
