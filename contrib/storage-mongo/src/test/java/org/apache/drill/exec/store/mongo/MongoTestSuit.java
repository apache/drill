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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
      IMongodConfig configIMongodConfig = crateConfigServerConfig(
          CONFIG_SERVER_PORT, true);
      configServers.add(configIMongodConfig);

      // creating replicaSets
      Map<String, List<IMongodConfig>> replicaSets = new HashMap<>();
      List<IMongodConfig> replicaSet1 = new ArrayList<>();
      replicaSet1.add(crateIMongodConfig(MONGOD_1_PORT, false,
          REPLICA_SET_1_NAME));
      replicaSet1.add(crateIMongodConfig(MONGOD_2_PORT, false,
          REPLICA_SET_1_NAME));
      replicaSet1.add(crateIMongodConfig(MONGOD_3_PORT, false,
          REPLICA_SET_1_NAME));
      replicaSets.put(REPLICA_SET_1_NAME, replicaSet1);
      List<IMongodConfig> replicaSet2 = new ArrayList<>();
      replicaSet2.add(crateIMongodConfig(MONGOD_4_PORT, false,
          REPLICA_SET_2_NAME));
      replicaSet2.add(crateIMongodConfig(MONGOD_5_PORT, false,
          REPLICA_SET_2_NAME));
      replicaSet2.add(crateIMongodConfig(MONGOD_6_PORT, false,
          REPLICA_SET_2_NAME));
      replicaSets.put(REPLICA_SET_2_NAME, replicaSet2);

      // create mongos
      IMongosConfig mongosConfig = createIMongosConfig();
      mongosTestFactory = new MongosSystemForTestFactory(mongosConfig,
          replicaSets, configServers, EMPLOYEE_DB, EMPINFO_COLLECTION,
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

    private static IMongodConfig crateConfigServerConfig(int configServerPort,
        boolean flag) throws UnknownHostException, IOException {
      IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder().useNoJournal(false).verbose(false)
          .build();

      IMongodConfig mongodConfig = new MongodConfigBuilder()
          .version(Version.Main.PRODUCTION)
          .net(new Net(LOCALHOST, configServerPort, Network.localhostIsIPv6()))
          .configServer(flag).cmdOptions(cmdOptions).build();
      return mongodConfig;
    }

    private static IMongodConfig crateIMongodConfig(int mongodPort,
        boolean flag, String replicaName) throws UnknownHostException,
        IOException {
      IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder().useNoJournal(false).verbose(false)
          .build();

      Storage replication = new Storage(null, replicaName, 0);
      IMongodConfig mongodConfig = new MongodConfigBuilder()
          .version(Version.Main.PRODUCTION)
          .net(new Net(LOCALHOST, mongodPort, Network.localhostIsIPv6()))
          .configServer(flag).replication(replication).cmdOptions(cmdOptions)
          .build();
      return mongodConfig;
    }

    private static IMongosConfig createIMongosConfig()
        throws UnknownHostException, IOException {
      IMongoCmdOptions cmdOptions = new MongoCmdOptionsBuilder().useNoJournal(false).verbose(false)
          .build();

      IMongosConfig mongosConfig = new MongosConfigBuilder()
          .version(Version.Main.PRODUCTION)
          .net(new Net(LOCALHOST, MONGOS_PORT, Network.localhostIsIPv6()))
          .configDB(LOCALHOST + ":" + CONFIG_SERVER_PORT)
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
          .version(Version.Main.PRODUCTION)
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
    IndexOptions indexOptions = new IndexOptions().unique(true)
        .background(false).name(indexFieldName);
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
