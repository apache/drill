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

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class MongoDrillClientTest {
  private static MongoClient mongoClient;

  public MongoDrillClientTest() throws UnknownHostException {
    mongoClient = new MongoClient();
  }

  public MongoDrillClientTest(String connHostName) throws UnknownHostException {
    mongoClient = new MongoClient(connHostName);
  }

  public MongoDrillClientTest(String connHostName, int connPort)
      throws UnknownHostException {
    mongoClient = new MongoClient(connHostName, connPort);
  }

  public MongoDrillClientTest(List<ServerAddress> seedServers) {
    mongoClient = new MongoClient(seedServers);
  }

  public static void main(String[] args) throws UnknownHostException {
    MongoDrillClientTest mongoClinet = new MongoDrillClientTest("localhost",
        27017);
    DB db = mongoClient.getDB("mydb");

    // get the collections
    Set<String> colls = db.getCollectionNames();
    for (String s : colls) {
      System.out.println(s);
    }

    DBCollection coll = db.getCollection("testData");
    // set the write concern
    mongoClient.setWriteConcern(WriteConcern.JOURNALED);

    // inserting records.
    BasicDBObject doc = new BasicDBObject("name", "MongoDB")
        .append("type", "database").append("count", 1)
        .append("info", new BasicDBObject("x", 100).append("y", 200));
    coll.insert(doc);

    // insert multiple
    for (int i = 0; i < 10; ++i) {
      coll.insert(new BasicDBObject("i", i));
    }

    // get the first doc
    DBObject resultDoc = coll.findOne();
    System.out.println(resultDoc);

    // get All docs using cursor
    DBCursor cursor = coll.find();

    try {
      while (cursor.hasNext()) {
        System.out.println(cursor.next());
      }
    } finally {
      cursor.close();
    }

    // query with filter condition

    BasicDBObject query = new BasicDBObject("i", 71);
    cursor = coll.find(query);
    try {
      while (cursor.hasNext()) {
        System.out.println(cursor.next());
      }
    } finally {
      cursor.close();
    }

    // range condn's
    query = new BasicDBObject("j", new BasicDBObject("$ne", 3)).append("k",
        new BasicDBObject("$gt", 10));
    cursor = coll.find(query);
    try {
      while (cursor.hasNext()) {
        System.out.println(cursor.next());
      }
    } finally {
      cursor.close();
    }

    query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte",
        30));
    cursor = coll.find(query);
    try {
      while (cursor.hasNext()) {
        System.out.println(cursor.next());
      }
    } finally {
      cursor.close();
    }

    // creating indexing
    coll.createIndex(new BasicDBObject("i", 1));

    // create a text index on the "content" field
    coll.createIndex(new BasicDBObject("content", "text"));

    // Insert some documents
    coll.insert(new BasicDBObject("_id", 0)
        .append("content", "textual content"));
    coll.insert(new BasicDBObject("_id", 1).append("content",
        "additional content"));
    coll.insert(new BasicDBObject("_id", 2).append("content",
        "irrelevant content"));

    // Find using the text index
    BasicDBObject search = new BasicDBObject("$search",
        "textual content -irrelevant");
    BasicDBObject textSearch = new BasicDBObject("$text", search);
    int matchCount = coll.find(textSearch).count();
    System.out.println("Text search matches: " + matchCount);

    // Find using the $language operator
    textSearch = new BasicDBObject("$text", search.append("$language",
        "english"));
    matchCount = coll.find(textSearch).count();
    System.out.println("Text search matches (english): " + matchCount);

    // Find the highest scoring match
    BasicDBObject projection = new BasicDBObject("score", new BasicDBObject(
        "$meta", "textScore"));
    DBObject myDoc = coll.findOne(textSearch, projection);
    System.out.println("Highest scoring document: " + myDoc);

    // 1. Ordered bulk operation
    // BulkWriteOperation builder = coll.initializeOrderedBulkOperation();
    // builder.insert(new BasicDBObject("_id", 1));
    // builder.insert(new BasicDBObject("_id", 2));
    // builder.insert(new BasicDBObject("_id", 3));
    //
    // builder.find(new BasicDBObject("_id", 1)).updateOne(new
    // BasicDBObject("$set", new BasicDBObject("x", 2)));
    // builder.find(new BasicDBObject("_id", 2)).removeOne();
    // builder.find(new BasicDBObject("_id", 3)).replaceOne(new
    // BasicDBObject("_id", 3).append("x", 4));
    //
    // BulkWriteResult result = builder.execute();
    //
    // // 2. Unordered bulk operation - no guarantee of order of operation
    // builder = coll.initializeUnorderedBulkOperation();
    // builder.find(new BasicDBObject("_id", 1)).removeOne();
    // builder.find(new BasicDBObject("_id", 2)).removeOne();
    //
    // result = builder.execute();

  }

}
