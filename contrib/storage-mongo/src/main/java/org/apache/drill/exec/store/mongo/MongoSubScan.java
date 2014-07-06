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

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class MongoSubScan extends AbstractBase implements SubScan {

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return null;
  }

  @Override
  public int getOperatorType() {
    return 0;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return null;
  }

  public static class MongoSubScanSpec {
    protected MongoClient mongoClient;
    protected String dbName;
    protected String collectionName;
    protected WriteConcern writeConcern;

    public MongoClient getMongoClient() {
      return mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient) {
      this.mongoClient = mongoClient;
    }

    public String getDbName() {
      return dbName;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    public String getCollectionName() {
      return collectionName;
    }

    public void setCollectionName(String collectionName) {
      this.collectionName = collectionName;
    }

    public WriteConcern getWriteConcern() {
      return writeConcern;
    }

    public void setWriteConcern(WriteConcern writeConcern) {
      this.writeConcern = writeConcern;
    }
  }

}
