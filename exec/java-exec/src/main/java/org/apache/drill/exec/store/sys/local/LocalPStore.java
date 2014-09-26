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
package org.apache.drill.exec.store.sys.local;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class LocalPStore<V> implements PStore<V> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalPStore.class);

  private static final String BLOB_QUALIFIER = "blob";

  private final File basePath;
  private final File blobPath;
  private final PStoreConfig<V> config;
  public LocalPStore(File base, PStoreConfig<V> config) {
    super();
    this.basePath = new File(base, config.getName());
    this.blobPath = new File(basePath, BLOB_QUALIFIER);
    if (!blobPath.exists()) {
      blobPath.mkdirs();
    }
    this.config = config;
  }

  @Override
  public Iterator<Entry<String, V>> iterator() {
    String[] f = basePath.list();
    if (f == null) {
      return Collections.emptyIterator();
    }
    List<String> files = Lists.newArrayList();
    for (String s : f) {
      if (s.endsWith(DRILL_SYS_FILE_SUFFIX)) {
        files.add(s.substring(0, s.length() - DRILL_SYS_FILE_SUFFIX.length()));
      }
    }

    return new Iter(files.iterator());
  }

  private File p(String name, boolean blob) throws IOException {
    Preconditions.checkArgument(
        !name.contains("/") &&
        !name.contains(":") &&
        !name.contains(".."));

    File f = new File(blob ? blobPath : basePath, name + DRILL_SYS_FILE_SUFFIX);
    // do this to check file name.
    f.getCanonicalPath();
    return f;
  }

  @Override
  public V get(String key) {
    return get(key, false);
  }

  @Override
  public V getBlob(String key) {
    return get(key, true);
  }

  protected V get(String key, boolean blob) {
    try (InputStream is = new FileInputStream(p(key, blob))) {
      return config.getSerializer().deserialize(IOUtils.toByteArray(is));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(String key, V value) {
    put(key, false, value);
  }

  @Override
  public void putBlob(String key, V value) {
    put(key, true, value);
  }

  protected void put(String key, boolean blob, V value) {
    try (OutputStream os = new FileOutputStream(p(key, blob))) {
      IOUtils.write(config.getSerializer().serialize(value), os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    try {
      File f = p(key, false);
      if (f.exists()) {
        return false;
      } else {
        put(key, value);
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(String key) {
    try {
      delete(key, false);
      delete(key, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void delete(String key, boolean blob) throws IOException {
    try {
      p(key, blob).delete();
    } catch (FileNotFoundException e) { /* ignored */ }
  }

  private class Iter implements Iterator<Entry<String, V>>{

    private Iterator<String> keys;
    private String current;

    public Iter(Iterator<String> keys) {
      super();
      this.keys = keys;
    }

    @Override
    public boolean hasNext() {
      return keys.hasNext();
    }

    @Override
    public Entry<String, V> next() {
      current = keys.next();
      return new DeferredEntry(current);
    }

    @Override
    public void remove() {
      delete(current);
      keys.remove();
    }

    private class DeferredEntry implements Entry<String, V> {

      private String name;

      public DeferredEntry(String name) {
        super();
        this.name = name;
      }

      @Override
      public String getKey() {
        return name;
      }

      @Override
      public V getValue() {
        return get(name);
      }

      @Override
      public V setValue(V value) {
        throw new UnsupportedOperationException();
      }

    }
  }

}
