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
import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.commons.lang.SerializationUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.mongo.DrillMongoConstants;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public class MongoPStore<V> implements PStore<V>, DrillMongoConstants {

  static final Logger logger = LoggerFactory.getLogger(MongoPStore.class);

  private final PStoreConfig<V> config;

  private final DBCollection collection;

  public MongoPStore(PStoreConfig<V> config, DBCollection collection)
      throws IOException {
    this.config = config;
    this.collection = collection;
  }

  @Override
  public V get(String key) {
    DBObject get = new BasicDBObject().append(ID, key);
    DBCursor cursor = collection.find(get);
    return value(cursor.next().get(MongoPStoreProvider.pKey));
  }

  @Override
  public void put(String key, V value) {
    DBObject putObj = new BasicDBObject(2);
    putObj.put(ID, key);
    putObj.put(MongoPStoreProvider.pKey, value);
    collection.insert(putObj);
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    DBObject check = new BasicDBObject(1).append(ID, key);
    DBObject putObj = new BasicDBObject(2);
    putObj.put(MongoPStoreProvider.pKey, value);
    WriteResult wr = collection.update(check, putObj, true, false);
    return wr.getN() == 1 ? true : false;
  }

  @Override
  public void delete(String key) {
    DBObject delete = new BasicDBObject(1).append(ID, key);
    collection.remove(delete);
  }

  private V value(Object obj) {
    try {
      byte[] serialize = SerializationUtils.serialize((Serializable) obj);
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

    private DBCursor cursor;

    public MongoIterator() {
      cursor = collection.find();
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

    private DBObject result;

    public DeferredEntry(DBObject result) {
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

}
