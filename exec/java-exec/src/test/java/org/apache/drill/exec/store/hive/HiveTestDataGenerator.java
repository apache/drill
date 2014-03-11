/**
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

package org.apache.drill.exec.store.hive;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveTestDataGenerator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveTestDataGenerator.class);

  static int RETRIES = 5;
  private Driver hiveDriver = null;
  private static final String DB_DIR = "/tmp/drill_hive_db";
  private static final String WH_DIR = "/tmp/drill_hive_wh";
  
  public static void main(String[] args) throws Exception {
    HiveTestDataGenerator htd = new HiveTestDataGenerator();
    htd.generateTestData();
  }

  private void cleanDir(String dir) throws IOException{
    File f = new File(dir);
    if(f.exists()){
      FileUtils.cleanDirectory(f);
      FileUtils.forceDelete(f);
    }
  }
  
  public void generateTestData() throws Exception {
    
    // remove data from previous runs.
    cleanDir(DB_DIR);
    cleanDir(WH_DIR);
    
    HiveConf conf = new HiveConf();

    conf.set("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s;create=true", DB_DIR));
    conf.set("fs.default.name", "file:///");
    conf.set("hive.metastore.warehouse.dir", WH_DIR);

    String tableName = "kv";

    SessionState ss = new SessionState(new HiveConf(SessionState.class));
    SessionState.start(ss);
    hiveDriver = new Driver(conf);
    executeQuery(String.format("CREATE TABLE IF NOT EXISTS default.kv(key INT, value STRING) "+
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE", tableName));
    executeQuery(String.format("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s", generateTestDataFile(), tableName));
      ss.close();
  }

  private String generateTestDataFile() throws Exception {
    File file = null;
    while (true) {
      file = File.createTempFile("drill-hive-test", ".txt");
      if (file.exists()) {
        boolean success = file.delete();
        if (success) {
          break;
        }
      }
      logger.debug("retry creating tmp file");
    }

    PrintWriter printWriter = new PrintWriter(file);
    for (int i=1; i<=5; i++)
      printWriter.println (String.format("%d, key_%d", i, i));
    printWriter.close();

    return file.getPath();
  }

  private void executeQuery(String query) {
    CommandProcessorResponse response = null;
    boolean failed = false;
    int retryCount = RETRIES;

    try {
      response = hiveDriver.run(query);
    } catch(CommandNeedRetryException ex) {
      if (--retryCount == 0)
        failed = true;
    }

    if (failed || response.getResponseCode() != 0 )
      throw new RuntimeException(String.format("Failed to execute command '%s', errorMsg = '%s'",
        query, (response != null ? response.getErrorMessage() : "")));
  }
}