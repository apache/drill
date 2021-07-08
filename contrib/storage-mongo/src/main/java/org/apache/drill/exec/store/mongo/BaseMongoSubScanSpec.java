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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.PlanStringBuilder;

import java.util.List;

public class BaseMongoSubScanSpec {

  protected String dbName;

  protected String collectionName;

  protected List<String> hosts;

  @JsonCreator
  public BaseMongoSubScanSpec(@JsonProperty("dbName") String dbName,
      @JsonProperty("collectionName") String collectionName,
      @JsonProperty("hosts") List<String> hosts) {
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.hosts = hosts;
  }

  BaseMongoSubScanSpec() {
  }

  public String getDbName() {
    return dbName;
  }

  public BaseMongoSubScanSpec setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public BaseMongoSubScanSpec setCollectionName(String collectionName) {
    this.collectionName = collectionName;
    return this;
  }

  public List<String> getHosts() {
    return hosts;
  }

  public BaseMongoSubScanSpec setHosts(List<String> hosts) {
    this.hosts = hosts;
    return this;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("dbName", dbName)
        .field("collectionName", collectionName)
        .field("hosts", hosts)
        .toString();

  }
}
