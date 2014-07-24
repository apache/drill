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
package org.apache.drill.exec.store.mongo;

import java.util.List;

import org.apache.drill.exec.store.mongo.common.MongoFilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoScanSpec {
  private DB dbName;
  private DBCollection collectionName;
  private List<MongoFilter> filters;

  @JsonCreator
  public MongoScanSpec(@JsonProperty("dbName") DB dbName,
      @JsonProperty("collectionName") DBCollection dbCollection,
      @JsonProperty("queryFilters") List<MongoFilter> filters) {
    this.dbName = dbName;
    this.collectionName = dbCollection;
    this.filters = filters;
  }

  public MongoScanSpec(DB dbName, DBCollection dbCollection) {
    this.dbName = dbName;
    this.collectionName = dbCollection;
  }

  public DB getDbName() {
    return dbName;
  }

  public DBCollection getCollectionName() {
    return collectionName;
  }

  public List<MongoFilter> getFilters() {
    return filters;
  }

  @Override
  public String toString() {
    return "MongoScanSpec [dbName=" + dbName + ", collectionName="
        + collectionName + ", filters=" + filters + "]";
  }
}
