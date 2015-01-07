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
package org.apache.drill.exec.store.hbase.config;

import static org.apache.drill.exec.store.hbase.config.HBasePStoreProvider.FAMILY;
import static org.apache.drill.exec.store.hbase.config.HBasePStoreProvider.QUALIFIER;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreConfig.Mode;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePStore<V> implements PStore<V> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBasePStore.class);

  private final PStoreConfig<V> config;

  private final HTableInterface table;

  private final String tableName;
  private final byte[] tableNameStartKey;
  private final byte[] tableNameStopKey;

  public HBasePStore(PStoreConfig<V> config, HTableInterface table) throws IOException {
    this.tableName = config.getName() + '\0';
    this.tableNameStartKey = Bytes.toBytes(tableName); // "tableName\x00"
    this.tableNameStopKey = this.tableNameStartKey.clone();
    this.tableNameStopKey[tableNameStartKey.length-1] = 1;
    this.config = config;
    this.table = table;
  }

  @Override
  public V get(String key) {
    return get(key, FAMILY);
  }

  protected synchronized V get(String key, byte[] family) {
    try {
      Get get = new Get(row(key));
      get.addColumn(family, QUALIFIER);
      Result r = table.get(get);
      if(r.isEmpty()){
        return null;
      }
      return value(r);
    } catch (IOException e) {
      throw new DrillRuntimeException("Caught error while getting row '" + key + "' from for table:" + Bytes.toString(table.getTableName()), e);
    }
  }

  @Override
  public void put(String key, V value) {
    put(key, FAMILY, value);
  }

  protected synchronized void put(String key, byte[] family, V value) {
    try {
      Put put = new Put(row(key));
      put.add(family, QUALIFIER, bytes(value));
      table.put(put);
    } catch (IOException e) {
      throw new DrillRuntimeException("Caught error while putting row '" + key + "' from for table:" + Bytes.toString(table.getTableName()), e);
    }
  }

  @Override
  public synchronized boolean putIfAbsent(String key, V value) {
    try {
      Put put = new Put(row(key));
      put.add(FAMILY, QUALIFIER, bytes(value));
      return table.checkAndPut(put.getRow(), FAMILY, QUALIFIER, null /*absent*/, put);
    } catch (IOException e) {
      throw new DrillRuntimeException("Caught error while putting row '" + key + "' from for table:" + Bytes.toString(table.getTableName()), e);
    }
  }

  @Override
  public synchronized void delete(String key) {
    delete(row(key));
  }

  @Override
  public Iterator<Entry<String, V>> iterator() {
    return new Iter();
  }

  private byte[] row(String key) {
    return Bytes.toBytes(this.tableName + key);
  }

  private byte[] bytes(V value) {
    try {
      return config.getSerializer().serialize(value);
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private V value(Result result) {
    try {
      return config.getSerializer().deserialize(result.value());
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private void delete(byte[] row) {
    try {
      Delete del = new Delete(row);
      table.delete(del);
    } catch (IOException e) {
      throw new DrillRuntimeException("Caught error while deleting row '" + Bytes.toStringBinary(row)
          + "' from for table:" + Bytes.toString(table.getTableName()), e);
    }
  }

  private class Iter implements Iterator<Entry<String, V>> {
    private ResultScanner scanner;
    private Result current = null;
    private Result last = null;
    private boolean done = false;
    private int rowsRead = 0;

    Iter() {
      try {
        Scan scan = new Scan(tableNameStartKey, tableNameStopKey);
        scan.addColumn(FAMILY, QUALIFIER);
        scan.setCaching(config.getMaxIteratorSize() > 100 ? 100 : config.getMaxIteratorSize());
        scanner = table.getScanner(scan);
      } catch (IOException e) {
        throw new DrillRuntimeException("Caught error while creating HBase scanner for table:" + Bytes.toString(table.getTableName()), e);
      }
    }

    @Override
    public boolean hasNext()  {
      if (config.getMode() == Mode.BLOB_PERSISTENT
          && rowsRead >= config.getMaxIteratorSize()) {
        done = true;
      } else if (!done && current == null) {
        try {
          if ((current = scanner.next()) == null) {
            done = true;
          }
        } catch (IOException e) {
          throw new DrillRuntimeException("Caught error while fetching rows from for table:" + Bytes.toString(table.getTableName()), e);
        }
      }

      if (done && scanner != null) {
        scanner.close();
      }
      return (current != null);
    }

    @Override
    public Entry<String, V> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      last = current;
      current = null;
      rowsRead++;
      return new DeferredEntry(last);
    }

    @Override
    public void remove() {
      if (last == null) {
        throw new IllegalStateException("remove() called on HBase persistent store iterator, but there is no last row.");
      }
      delete(last.getRow());
    }

  }

  private class DeferredEntry implements Entry<String, V>{

    private Result result;

    public DeferredEntry(Result result) {
      super();
      this.result = result;
    }

    @Override
    public String getKey() {
      return Bytes.toString(result.getRow(), tableNameStartKey.length, result.getRow().length-tableNameStartKey.length);
    }

    @Override
    public V getValue() {
      return value(result);
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
