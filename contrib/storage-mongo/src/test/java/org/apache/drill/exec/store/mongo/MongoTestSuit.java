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
package org.apache.drill.exec.store.mongo;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.categories.MongoStorageTest;
import org.apache.drill.categories.SlowTest;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongoCmdOptions;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.IMongosConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.MongosConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongosSystemForTestFactory;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.runtime.Network;

@RunWith(Suite.class)
@SuiteClasses({ TestMongoFilterPushDown.class, TestMongoProjectPushDown.class,
    TestMongoQueries.class, TestMongoChunkAssignment.class })
@Category({SlowTest.class, MongoStorageTest.class})
public class MongoTestSuit implements MongoTestConstants {

  private static final Logger logger = LoggerFactory
      .getLogger(MongoTestSuit.class);

  protected static MongoClient mongoClient;

  private static boolean distMode = System.getProperty(
      "drill.mongo.tests.shardMode", "true").equalsIgnoreCase("true");
  private static boolean authEnabled = System.getProperty(
      "drill.mongo.tests.authEnabled", "false").equalsIgnoreCase("true");

  private static volatile AtomicInteger initCount = new AtomicInteger(0);

  private static volatile boolean runningSuite = false;

  public static boolean isRunningSuite() {
    return runningSuite;
  }

  private static class DistributedMode {
    private static MongosSystemForTestFactory mongosTestFactory;

    private static void setup() throws Exception {
      // creating configServers
      List<IMongodConfig> configServers = new ArrayList<>(1);
      configServers.add(crateConfigServerConfig(CONFIG_SERVER_1_PORT));
      configServers.add(crateConfigServerConfig(CONFIG_SERVER_2_PORT));
      configServers.add(crateConfigServerConfig(CONFIG_SERVER_3_PORT));

      // creating replicaSets
      // A LinkedHashMap ensures that the config servers are started first.
      Map<String, List<IMongodConfig>> replicaSets = new LinkedHashMap<>();

      List<IMongodConfig> replicaSet1 = new ArrayList<>();
      replicaSet1.add(crateIMongodConfig(MONGOD_1_PORT, false,
          REPLICA_SET_1_NAME));
      replicaSet1.add(crateIMongodConfig(MONGOD_2_PORT, false,
          REPLICA_SET_1_NAME));
      replicaSet1.add(crateIMongodConfig(MONGOD_3_PORT, false,
          REPLICA_SET_1_NAME));
      List<IMongodConfig> replicaSet2 = new ArrayList<>();
      replicaSet2.add(crateIMongodConfig(MONGOD_4_PORT, false,
          REPLICA_SET_2_NAME));
      replicaSet2.add(crateIMongodConfig(MONGOD_5_PORT, false,
          REPLICA_SET_2_NAME));
      replicaSet2.add(crateIMongodConfig(MONGOD_6_PORT, false,
          REPLICA_SET_2_NAME));

      replicaSets.put(CONFIG_REPLICA_SET, configServers);
      replicaSets.put(REPLICA_SET_1_NAME, replicaSet1);
      replicaSets.put(REPLICA_SET_2_NAME, replicaSet2);

      // create mongos
      IMongosConfig mongosConfig = createIMongosConfig();
      mongosTestFactory = new MongosSystemForTestFactory(mongosConfig,
          replicaSets, Lists.newArrayList(), EMPLOYEE_DB, EMPINFO_COLLECTION,
          "employee_id");
      try {
        mongosTestFactory.start();
        mongoClient = (MongoClient) mongosTestFactory.getMongo();
      } catch (Throwable e) {
        logger.error(" Error while starting shrded cluster. ", e);
        throw new Exception(" Error while starting shrded cluster. ", e);
      }
      createDbAndCollections(DONUTS_DB, DONUTS_COLLECTION, "id");
      createDbAndCollections(EMPLOYEE_DB, EMPTY_COLLECTION, "field_2");
      createDbAndCollections(DATATYPE_DB, DATATYPE_COLLECTION, "_id");
    }

    private static IMongodConfig crateConfigServerConfig(int configServerPort) throws UnknownHostException, IOException {
      IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder()
        .useNoPrealloc(false)
        .useSmallFiles(false)
        .useNoJournal(false)
        .useStorageEngine(STORAGE_ENGINE)
        .verbose(false)
        .build();

      Storage replication = new Storage(null, CONFIG_REPLICA_SET, 0);

      IMongodConfig mongodConfig = new MongodConfigBuilder()
          .version(Version.Main.V3_4)
          .net(new Net(LOCALHOST, configServerPort, Network.localhostIsIPv6()))
          .replication(replication)
          .shardServer(false)
          .configServer(true).cmdOptions(cmdOptions).build();
      return mongodConfig;
    }

    private static IMongodConfig crateIMongodConfig(int mongodPort,
        boolean flag, String replicaName) throws UnknownHostException,
        IOException {
      IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder()
        .useNoPrealloc(false)
        .useSmallFiles(false)
        .useNoJournal(false)
        .useStorageEngine(STORAGE_ENGINE)
        .verbose(false)
        .build();

      Storage replication = new Storage(null, replicaName, 0);
      IMongodConfig mongodConfig = new MongodConfigBuilder()
          .version(Version.Main.V3_4)
        .shardServer(true)
          .net(new Net(LOCALHOST, mongodPort, Network.localhostIsIPv6()))
          .configServer(flag).replication(replication).cmdOptions(cmdOptions)
          .build();

      return mongodConfig;
    }

    private static IMongosConfig createIMongosConfig()
        throws UnknownHostException, IOException {
      IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder()
        .useNoPrealloc(false)
        .useSmallFiles(false)
        .useNoJournal(false)
        .useStorageEngine(STORAGE_ENGINE)
        .verbose(false)
        .build();

      IMongosConfig mongosConfig = new MongosConfigBuilder()
          .version(Version.Main.V3_4)
          .net(new Net(LOCALHOST, MONGOS_PORT, Network.localhostIsIPv6()))
          .replicaSet(CONFIG_REPLICA_SET)
          .configDB(LOCALHOST + ":" + CONFIG_SERVER_1_PORT)
          .cmdOptions(cmdOptions).build();
      return mongosConfig;
    }

    private static void cleanup() {
      if (mongosTestFactory != null) {
        // ignoring exception because sometimes provided time isn't enough to stop mongod processes
        try {
            mongosTestFactory.stop();
          } catch (IllegalStateException e) {
            logger.warn("Failed to close all mongod processes during provided timeout", e);
          }
      }
    }

  }

  private static class SingleMode {

    private static MongodExecutable mongodExecutable;
    private static MongodProcess mongod;

    private static void setup() throws UnknownHostException, IOException {
      IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder().verbose(false)
          .enableAuth(authEnabled).build();

      IMongodConfig mongodConfig = new MongodConfigBuilder()
          .version(Version.Main.V3_4)
          .net(new Net(LOCALHOST, MONGOS_PORT, Network.localhostIsIPv6()))
          .cmdOptions(cmdOptions).build();

      IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder().defaults(
          Command.MongoD).build();
      mongodExecutable = MongodStarter.getInstance(runtimeConfig).prepare(
          mongodConfig);
      mongod = mongodExecutable.start();
      mongoClient = new MongoClient(new ServerAddress(LOCALHOST, MONGOS_PORT));
      createDbAndCollections(EMPLOYEE_DB, EMPINFO_COLLECTION, "employee_id");
      createDbAndCollections(EMPLOYEE_DB, SCHEMA_CHANGE_COLLECTION, "field_2");
      createDbAndCollections(EMPLOYEE_DB, EMPTY_COLLECTION, "field_2");
      createDbAndCollections(DATATYPE_DB, DATATYPE_COLLECTION, "_id");
    }

    private static void cleanup() {
      if (mongod != null) {
        mongod.stop();
      }
      if (mongodExecutable != null) {
        mongodExecutable.stop();
      }
    }
  }

  @BeforeClass
  public static void initMongo() throws Exception {
    synchronized (MongoTestSuit.class) {
      if (initCount.get() == 0) {
        if (distMode) {
          logger.info("Executing tests in distributed mode");
          DistributedMode.setup();
        } else {
          logger.info("Executing tests in single mode");
          SingleMode.setup();
        }
        TestTableGenerator.importData(EMPLOYEE_DB, EMPINFO_COLLECTION, EMP_DATA);
        TestTableGenerator.importData(EMPLOYEE_DB, SCHEMA_CHANGE_COLLECTION, SCHEMA_CHANGE_DATA);
        TestTableGenerator.importData(DONUTS_DB, DONUTS_COLLECTION, DONUTS_DATA);
        TestTableGenerator.importData(DATATYPE_DB, DATATYPE_COLLECTION, DATATYPE_DATA);
      }
      initCount.incrementAndGet();
      runningSuite = true;
    }
  }

  private static void createDbAndCollections(String dbName,
      String collectionName, String indexFieldName) {
    MongoDatabase db = mongoClient.getDatabase(dbName);
    MongoCollection<Document> mongoCollection = db
        .getCollection(collectionName);
    if (mongoCollection == null) {
      db.createCollection(collectionName);
      mongoCollection = db.getCollection(collectionName);
    }

    if (indexFieldName.equals("_id")) {
      // Mongo 3.4 and later already makes an index for a field named _id
      return;
    }

    IndexOptions indexOptions = new IndexOptions().unique(true).background(false).name(indexFieldName);
    Bson keys = Indexes.ascending(indexFieldName);
    mongoCollection.createIndex(keys, indexOptions);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    synchronized (MongoTestSuit.class) {
      if (initCount.decrementAndGet() == 0) {
        try {
          if (mongoClient != null) {
            mongoClient.dropDatabase(EMPLOYEE_DB);
            mongoClient.dropDatabase(DATATYPE_DB);
            mongoClient.dropDatabase(DONUTS_DB);
          }
        }
        finally {
          runningSuite = false;
          if (mongoClient != null) {
            mongoClient.close();
          }
          if (distMode) {
            DistributedMode.cleanup();
          } else {
            SingleMode.cleanup();
          }
        }
      }
    }
  }
}
