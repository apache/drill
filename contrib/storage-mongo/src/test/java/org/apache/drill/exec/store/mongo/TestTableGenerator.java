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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;

public class TestTableGenerator implements MongoTestConstants {

  private static final Logger logger = LoggerFactory
      .getLogger(TestTableGenerator.class);

  public static void importData(GenericContainer<?> mongo, String dbName, String collectionName,
                                String fileName) throws InterruptedException, IOException {
    Container.ExecResult execResult = mongo.execInContainer("/bin/bash", "-c",
        "mongoimport --db " + dbName + " --collection " + collectionName + " --jsonArray --upsert --file " + fileName);
    logger.info(execResult.toString());

    logger.info("Imported file {} into collection {} ", fileName, collectionName);
  }

}
