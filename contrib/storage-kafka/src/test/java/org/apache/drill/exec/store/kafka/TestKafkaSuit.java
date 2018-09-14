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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.drill.categories.KafkaStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.ZookeeperTestUtil;
import org.apache.drill.exec.store.kafka.cluster.EmbeddedKafkaCluster;
import org.apache.drill.exec.store.kafka.decoders.MessageReaderFactoryTest;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.security.JaasUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

@Category({KafkaStorageTest.class, SlowTest.class})
@RunWith(Suite.class)
@SuiteClasses({ KafkaQueriesTest.class, MessageIteratorTest.class, MessageReaderFactoryTest.class,
    KafkaFilterPushdownTest.class })
public class TestKafkaSuit {
  private static final Logger logger = LoggerFactory.getLogger(LoggerFactory.class);
  private static final String LOGIN_CONF_RESOURCE_PATHNAME = "login.conf";

  public static EmbeddedKafkaCluster embeddedKafkaCluster;
  private static ZkClient zkClient;

  private static volatile AtomicInteger initCount = new AtomicInteger(0);
  static final int NUM_JSON_MSG = 10;
  static final int CONN_TIMEOUT = 8 * 1000;
  static final int SESSION_TIMEOUT = 10 * 1000;

  static String kafkaBroker;
  private static volatile boolean runningSuite = false;

  @BeforeClass
  public static void initKafka() throws Exception {
    synchronized (TestKafkaSuit.class) {
      if (initCount.get() == 0) {
        ZookeeperTestUtil.setZookeeperSaslTestConfigProps();
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, ClassLoader.getSystemResource(LOGIN_CONF_RESOURCE_PATHNAME).getFile());
        embeddedKafkaCluster = new EmbeddedKafkaCluster();
        Properties topicProps = new Properties();
        zkClient = new ZkClient(embeddedKafkaCluster.getZkServer().getConnectionString(), SESSION_TIMEOUT, CONN_TIMEOUT, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(embeddedKafkaCluster.getZkServer().getConnectionString()), false);
        AdminUtils.createTopic(zkUtils, TestQueryConstants.JSON_TOPIC, 1, 1, topicProps, RackAwareMode.Disabled$.MODULE$);

        org.apache.kafka.common.requests.MetadataResponse.TopicMetadata fetchTopicMetadataFromZk = AdminUtils
            .fetchTopicMetadataFromZk(TestQueryConstants.JSON_TOPIC, zkUtils);
        logger.info("Topic Metadata: " + fetchTopicMetadataFromZk);

        KafkaMessageGenerator generator = new KafkaMessageGenerator(embeddedKafkaCluster.getKafkaBrokerList(),
            StringSerializer.class);
        generator.populateJsonMsgIntoKafka(TestQueryConstants.JSON_TOPIC, NUM_JSON_MSG);
      }
      initCount.incrementAndGet();
      runningSuite = true;
    }
    logger.info("Initialized Embedded Zookeeper and Kafka");
  }

  public static boolean isRunningSuite() {
    return runningSuite;
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    synchronized (TestKafkaSuit.class) {
      if (initCount.decrementAndGet() == 0) {
        if (zkClient != null) {
          zkClient.close();
        }
        if (embeddedKafkaCluster != null && !embeddedKafkaCluster.getBrokers().isEmpty()) {
          embeddedKafkaCluster.shutDownCluster();
        }
      }
    }
  }

  public static void createTopicHelper(final String topicName, final int partitions) {

    Properties topicProps = new Properties();
    topicProps.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime");
    topicProps.put(TopicConfig.RETENTION_MS_CONFIG, "-1");
    ZkUtils zkUtils = new ZkUtils(zkClient,
        new ZkConnection(embeddedKafkaCluster.getZkServer().getConnectionString()), false);
    AdminUtils.createTopic(zkUtils, topicName, partitions, 1,
        topicProps, RackAwareMode.Disabled$.MODULE$);

    org.apache.kafka.common.requests.MetadataResponse.TopicMetadata fetchTopicMetadataFromZk =
        AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils);
    logger.info("Topic Metadata: " + fetchTopicMetadataFromZk);
  }

}
