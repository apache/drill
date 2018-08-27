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

import de.flapdoodle.embed.mongo.MongoImportProcess;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.io.Resources;

import de.flapdoodle.embed.mongo.MongoImportExecutable;
import de.flapdoodle.embed.mongo.MongoImportStarter;
import de.flapdoodle.embed.mongo.config.IMongoImportConfig;
import de.flapdoodle.embed.mongo.config.MongoImportConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class TestTableGenerator implements MongoTestConstants {

  private static final Logger logger = LoggerFactory
      .getLogger(TestTableGenerator.class);

  public static void importData(String dbName, String collectionName,
      String fileName) throws InterruptedException,IOException {
    String jsonFile = Resources.getResource(fileName).toString();
    jsonFile = jsonFile.replaceFirst("file:", StringUtils.EMPTY);
    generateTable(dbName, collectionName, jsonFile, true, true, false);
  }

  public static void generateTable(String dbName, String collection,
      String jsonFile, Boolean jsonArray, Boolean upsert, Boolean drop)
      throws InterruptedException, IOException {
    logger.info("Started importing file {} into collection {} ", jsonFile,
        collection);
    IMongoImportConfig mongoImportConfig = new MongoImportConfigBuilder()
        .version(Version.Main.V3_4)
        .net(new Net(MONGOS_PORT, Network.localhostIsIPv6())).db(dbName)
        .collection(collection).upsert(upsert).dropCollection(drop)
        .jsonArray(jsonArray).importFile(jsonFile).build();
    MongoImportExecutable importExecutable = MongoImportStarter
        .getDefaultInstance().prepare(mongoImportConfig);
    MongoImportProcess importProcess = importExecutable.start();

    // import is in a separate process, we should wait until the process exit
    while (importProcess.isProcessRunning()) {
        Thread.sleep(1000);
    }

    logger.info("Imported file {} into collection {} ", jsonFile, collection);
  }

}
