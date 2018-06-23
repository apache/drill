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

import org.bson.Document;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MongoScanSpec {
  private String dbName;
  private String collectionName;

  private Document filters;

  @JsonCreator
  public MongoScanSpec(@JsonProperty("dbName") String dbName,
      @JsonProperty("collectionName") String collectionName) {
    this.dbName = dbName;
    this.collectionName = collectionName;
  }

  public MongoScanSpec(String dbName, String collectionName,
      Document filters) {
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.filters = filters;
  }

  public String getDbName() {
    return dbName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public Document getFilters() {
    return filters;
  }

  @Override
  public String toString() {
    return "MongoScanSpec [dbName=" + dbName + ", collectionName="
        + collectionName + ", filters=" + filters + "]";
  }

}
