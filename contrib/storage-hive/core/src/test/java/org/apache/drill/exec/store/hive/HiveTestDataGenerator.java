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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.fs.FileSystem;
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

  public void createAndAddHiveTestPlugin(StoragePluginRegistry pluginRegistry) throws Exception {
    // generate test tables and data
    generateTestData();

    // add Hive plugin to given registry
    Map<String, String> config = Maps.newHashMap();
    config.put("hive.metastore.uris", "");
    config.put("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s;create=true", DB_DIR));
    config.put("hive.metastore.warehouse.dir", WH_DIR);
    config.put("fs.default.name", "file:///");
    HiveStoragePluginConfig pluginConfig = new HiveStoragePluginConfig(config);
    pluginConfig.setEnabled(true);

    pluginRegistry.createOrUpdate("hive", pluginConfig, true);
  }

  public void generateTestData() throws Exception {
    
    // remove data from previous runs.
    cleanDir(DB_DIR);
    cleanDir(WH_DIR);
    
    HiveConf conf = new HiveConf();

    conf.set("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s;create=true", DB_DIR));
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    conf.set("hive.metastore.warehouse.dir", WH_DIR);

    SessionState ss = new SessionState(new HiveConf(SessionState.class));
    SessionState.start(ss);
    hiveDriver = new Driver(conf);

    // generate (key, value) test data
    String testDataFile = generateTestDataFile();

    createTableAndLoadData("default", "kv", testDataFile);
    executeQuery("CREATE DATABASE IF NOT EXISTS db1");
    createTableAndLoadData("db1", "kv_db1", testDataFile);

    // Generate data with date and timestamp data type
    String testDateDataFile = generateTestDataFileWithDate();

    // create table with date and timestamp data type
    executeQuery("USE default");
    executeQuery("CREATE TABLE IF NOT EXISTS default.foodate(a DATE, b TIMESTAMP) "+
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
    executeQuery(String.format("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE default.foodate", testDateDataFile));

    // create a table with no data
    executeQuery("CREATE TABLE IF NOT EXISTS default.empty_table(a INT, b STRING)");
    // delete the table location of empty table
    File emptyTableLocation = new File(WH_DIR + "/empty_table");
    if (emptyTableLocation.exists()) {
      FileUtils.forceDelete(emptyTableLocation);
    }

    // create a Hive table that has columns with data types which are supported for reading in Drill.
    testDataFile = generateAllTypesDataFile();
    executeQuery(
        "CREATE TABLE IF NOT EXISTS readtest (" +
        "  binary_field BINARY," +
        "  boolean_field BOOLEAN," +
        "  tinyint_field TINYINT," +
        "  decimal_field DECIMAL," +
        "  double_field DOUBLE," +
        "  float_field FLOAT," +
        "  int_field INT," +
        "  bigint_field BIGINT," +
        "  smallint_field SMALLINT," +
        "  string_field STRING," +
        "  varchar_field VARCHAR(50)," +
        "  timestamp_field TIMESTAMP," +
        "  date_field DATE" +
        ") PARTITIONED BY (" +
        "  binary_part BINARY," +
        "  boolean_part BOOLEAN," +
        "  tinyint_part TINYINT," +
        "  decimal_part DECIMAL," +
        "  double_part DOUBLE," +
        "  float_part FLOAT," +
        "  int_part INT," +
        "  bigint_part BIGINT," +
        "  smallint_part SMALLINT," +
        "  string_part STRING," +
        "  varchar_part VARCHAR(50)," +
        "  timestamp_part TIMESTAMP," +
        "  date_part DATE" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
    );

    // Add a partition to table 'readtest'
    executeQuery(
        "ALTER TABLE readtest ADD IF NOT EXISTS PARTITION ( " +
        "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='64', " +
        "  decimal_part='3489423929323435243', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05')"
    );

    // Load data into table 'readtest'
    executeQuery(String.format("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE default.readtest PARTITION (" +
        "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='64', " +
        "  decimal_part='3489423929323435243', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05')", testDataFile));

    // create a table that has all Hive types. This is to test how hive tables metadata is populated in
    // Drill's INFORMATION_SCHEMA.
    executeQuery("CREATE TABLE IF NOT EXISTS infoschematest(" +
        "booleanType BOOLEAN, " +
        "tinyintType TINYINT, " +
        "smallintType SMALLINT, " +
        "intType INT, " +
        "bigintType BIGINT, " +
        "floatType FLOAT, " +
        "doubleType DOUBLE, " +
        "dataType DATE, " +
        "timestampType TIMESTAMP, " +
        "binaryType BINARY, " +
        "decimalType DECIMAL, " +
        "stringType STRING, " +
        "varCharType VARCHAR(20), " +
        "listType ARRAY<STRING>, " +
        "mapType MAP<STRING,INT>, " +
        "structType STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>, " +
        "uniontypeType UNIONTYPE<int, double, array<string>>)"
    );

    // create a Hive view to test how its metadata is populated in Drill's INFORMATION_SCHEMA
    executeQuery("CREATE VIEW IF NOT EXISTS hiveview AS SELECT * FROM kv");

    ss.close();
  }

  private void createTableAndLoadData(String dbName, String tblName, String dataFile) {
    executeQuery(String.format("USE %s", dbName));
    executeQuery(String.format("CREATE TABLE IF NOT EXISTS %s.%s(key INT, value STRING) "+
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE", dbName, tblName));
    executeQuery(String.format("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s.%s", dataFile, dbName, tblName));
  }

  private File getTempFile() throws Exception {
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

    return file;
  }

  private String generateTestDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    for (int i=1; i<=5; i++)
      printWriter.println (String.format("%d, key_%d", i, i));
    printWriter.close();

    return file.getPath();
  }

  private String generateTestDataFileWithDate() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    for (int i=1; i<=5; i++) {
      Date date = new Date(System.currentTimeMillis());
      Timestamp ts = new Timestamp(System.currentTimeMillis());
      printWriter.println (String.format("%s,%s", date.toString(), ts.toString()));
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateAllTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    printWriter.println("YmluYXJ5ZmllbGQ=,false,34,3489423929323435243,8.345,4.67,123456,234235,3455,stringfield,varcharfield,2013-07-05 17:01:00,2013-07-05");
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
