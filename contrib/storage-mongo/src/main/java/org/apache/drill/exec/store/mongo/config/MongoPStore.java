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
package org.apache.drill.exec.store.mongo.config;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.mongo.DrillMongoConstants;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

import static org.apache.drill.exec.store.mongo.config.MongoPStoreProvider.pKey;

public class MongoPStore<V> implements PStore<V>, DrillMongoConstants {

  static final Logger logger = LoggerFactory.getLogger(MongoPStore.class);

  private final PStoreConfig<V> config;

  private final MongoCollection<Document> collection;

  public MongoPStore(PStoreConfig<V> config, MongoCollection<Document> collection)
      throws IOException {
//    this.config = config;
//    this.collection = collection;
    throw new UnsupportedOperationException("Mongo DB PStore not currently supported");
  }

  @Override
  public V get(String key) {
    try {
      Bson query = Filters.eq(ID, key);
      Document document = collection.find(query).first();
      if (document != null) {
        return value((byte[]) document.get(pKey));
      } else {
        return null;
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public void put(String key, V value) {
    try {
      Document putObj = new Document(ID, key).append(pKey, bytes(value));
      collection.insertOne(putObj);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    try {
      Bson query = Filters.eq(ID, key);
      Bson update = Updates.set(pKey, bytes(value));
      UpdateResult updateResult = collection.updateOne(query, update, new UpdateOptions().upsert(true));
      return updateResult.getModifiedCount() == 1;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public void delete(String key) {
    try {
      Bson query = Filters.eq(ID, key);
      collection.deleteOne(query);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  private byte[] bytes(V value) {
    try {
      return config.getSerializer().serialize(value);
    } catch (IOException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  private V value(byte[] serialize) {
    try {
      return config.getSerializer().deserialize(serialize);
    } catch (IOException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public Iterator<Entry<String, V>> iterator() {
    return new MongoIterator();
  }

  private class MongoIterator implements Iterator<Entry<String, V>> {

    private MongoCursor<Document> cursor;

    public MongoIterator() {
      cursor = collection.find().iterator();
    }

    @Override
    public boolean hasNext() {
      return cursor.hasNext();
    }

    @Override
    public Entry<String, V> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return new DeferredEntry(cursor.next());
    }

    @Override
    public void remove() {
      cursor.remove();
    }
  }

  private class DeferredEntry implements Entry<String, V> {

    private Document result;

    public DeferredEntry(Document result) {
      this.result = result;
    }

    @Override
    public String getKey() {
      return result.get(ID).toString();
    }

    @Override
    public V getValue() {
      return get(result.get(ID).toString());
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void close() {
  }
}
