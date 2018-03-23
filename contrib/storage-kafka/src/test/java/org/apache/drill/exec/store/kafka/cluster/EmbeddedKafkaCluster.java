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
package org.apache.drill.exec.store.kafka.cluster;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.store.kafka.KafkaStoragePluginConfig;
import org.apache.drill.exec.store.kafka.TestQueryConstants;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class EmbeddedKafkaCluster implements TestQueryConstants {
  private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
  private List<KafkaServerStartable> brokers;
  private final ZookeeperHelper zkHelper;
  private final Properties props;

  public EmbeddedKafkaCluster() throws IOException {
    this(new Properties());
  }

  public EmbeddedKafkaCluster(Properties props) throws IOException {
    this(props, 1);
  }

  public EmbeddedKafkaCluster(Properties basePorps, int numberOfBrokers) throws IOException {
    this.props = new Properties();
    props.putAll(basePorps);
    this.zkHelper = new ZookeeperHelper();
    zkHelper.startZookeeper(1);
    this.brokers = new ArrayList<>(numberOfBrokers);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numberOfBrokers; ++i) {
      if (i != 0) {
        sb.append(BROKER_DELIM);
      }
      int ephemeralBrokerPort = getEphemeralPort();
      sb.append(LOCAL_HOST + ":" + ephemeralBrokerPort);
      addBroker(props, i, ephemeralBrokerPort);
    }

    this.props.put("metadata.broker.list", sb.toString());
    this.props.put(KafkaConfig.ZkConnectProp(), this.zkHelper.getConnectionString());
    logger.info("Initialized Kafka Server");
  }

  private void addBroker(Properties props, int brokerID, int ephemeralBrokerPort) {
    Properties properties = new Properties();
    properties.putAll(props);
    properties.put(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp(), String.valueOf(1));
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp(), String.valueOf(1));
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), String.valueOf(1));
    properties.put(KafkaConfig.DefaultReplicationFactorProp(), String.valueOf(1));
    properties.put(KafkaConfig.GroupMinSessionTimeoutMsProp(), String.valueOf(100));
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp(), Boolean.TRUE);
    properties.put(KafkaConfig.ZkConnectProp(), zkHelper.getConnectionString());
    properties.put(KafkaConfig.BrokerIdProp(), String.valueOf(brokerID + 1));
    properties.put(KafkaConfig.HostNameProp(), String.valueOf(LOCAL_HOST));
    properties.put(KafkaConfig.AdvertisedHostNameProp(), String.valueOf(LOCAL_HOST));
    properties.put(KafkaConfig.PortProp(), String.valueOf(ephemeralBrokerPort));
    properties.put(KafkaConfig.DeleteTopicEnableProp(), Boolean.FALSE);
    properties.put(KafkaConfig.LogDirsProp(), getTemporaryDir().getAbsolutePath());
    properties.put(KafkaConfig.LogFlushIntervalMessagesProp(), String.valueOf(1));
    brokers.add(getBroker(properties));
  }

  private static KafkaServerStartable getBroker(Properties properties) {
    KafkaServerStartable broker = new KafkaServerStartable(new KafkaConfig(properties));
    broker.startup();
    return broker;
  }

  public void shutDownCluster() throws IOException {
    // set Kafka log level to ERROR
    Level level = LogManager.getLogger(KafkaStoragePluginConfig.NAME).getLevel();
    LogManager.getLogger(KafkaStoragePluginConfig.NAME).setLevel(Level.ERROR);

    for (KafkaServerStartable broker : brokers) {
      broker.shutdown();
    }

    // revert back the level
    LogManager.getLogger(KafkaStoragePluginConfig.NAME).setLevel(level);
    zkHelper.stopZookeeper();
  }

  public void shutDownBroker(int brokerId) {
    for (KafkaServerStartable broker : brokers) {
      if (Integer.valueOf(broker.serverConfig().getString(KafkaConfig.BrokerIdProp())) == brokerId) {
        broker.shutdown();
        return;
      }
    }
  }

  public Properties getProps() {
    Properties tmpProps = new Properties();
    tmpProps.putAll(this.props);
    return tmpProps;
  }

  public List<KafkaServerStartable> getBrokers() {
    return brokers;
  }

  public void setBrokers(List<KafkaServerStartable> brokers) {
    this.brokers = brokers;
  }

  public ZookeeperHelper getZkServer() {
    return zkHelper;
  }

  public String getKafkaBrokerList() {
    StringBuilder sb = new StringBuilder();
    for (KafkaServerStartable broker : brokers) {
      KafkaConfig serverConfig = broker.serverConfig();
      sb.append(serverConfig.hostName() + ":" + serverConfig.port());
      sb.append(",");
    }
    return sb.toString().substring(0, sb.toString().length() - 1);
  }

  private int getEphemeralPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private File getTemporaryDir() {
    File file = new File(System.getProperty("java.io.tmpdir"), ZK_TMP + System.nanoTime());
    if (!file.mkdir()) {
      logger.error("Failed to create temp Dir");
      throw new RuntimeException("Failed to create temp Dir");
    }
    return file;
  }

}
