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
package org.apache.drill.exec.store.sys.zk;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * This is the abstract class that is shared by ZkPStore (Persistent store) and ZkEStore (Ephemeral Store)
 * @param <V>
 */
public abstract class ZkAbstractStore<V> implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZkAbstractStore.class);

  protected CuratorFramework framework;
  protected PStoreConfig<V> config;
  private final PathChildrenCache childrenCache;
  private String prefix;
  private String parent;

  public ZkAbstractStore(CuratorFramework framework, PStoreConfig<V> config)
      throws IOException {
    this.parent = "/" + config.getName();
    this.prefix = parent + "/";
    this.framework = framework;
    this.config = config;

    // make sure the parent node exists.
    try {
      if (framework.checkExists().forPath(parent) == null) {
        framework.create().withMode(CreateMode.PERSISTENT).forPath(parent);
      }

      this.childrenCache = new PathChildrenCache(framework, parent, true);
      this.childrenCache.start(StartMode.BUILD_INITIAL_CACHE);

    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper for PStore: " + e.getMessage(), e);
    }

  }

  public Iterator<Entry<String, V>> iterator() {
    try {
      return new Iter(childrenCache.getCurrentData());
    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper. " + e.getMessage(), e);
    }
  }

  protected String p(String key) {
    Preconditions.checkArgument(!key.contains("/"),
        "You cannot use keys that have slashes in them when using the Zookeeper SystemTable storage interface.");
    return prefix + key;
  }

  public V get(String key) {
    try {
      ChildData d = childrenCache.getCurrentData(p(key));
      if(d == null || d.getData() == null){
        return null;
      }
      byte[] bytes = d.getData();
      return config.getSerializer().deserialize(bytes);

    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper. " + e.getMessage(), e);
    }
  }

  public void put(String key, V value) {
    try {
      if (childrenCache.getCurrentData(p(key)) != null) {
        framework.setData().forPath(p(key), config.getSerializer().serialize(value));
      } else {
        createNodeInZK(key, value);
      }
      childrenCache.rebuildNode(p(key));

    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper. " + e.getMessage(), e);
    }
  }

  public void delete(String key) {
    try {
        framework.delete().forPath(p(key));
        childrenCache.rebuildNode(p(key));
    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper. " + e.getMessage(), e);
    }
  }

  public boolean putIfAbsent(String key, V value) {
    try {
      if (childrenCache.getCurrentData(p(key)) != null) {
        return false;
      } else {
        createNodeInZK(key, value);
        childrenCache.rebuildNode(p(key));
        return true;
      }

    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper", e);
    }
  }

  public abstract void createNodeInZK (String key, V value);

  private class Iter implements Iterator<Entry<String, V>>{

    private Iterator<ChildData> keys;
    private ChildData current;

    public Iter(List<ChildData> children) {
      super();
      List<ChildData> sortedChildren = Lists.newArrayList(children);
      Collections.sort(sortedChildren, new Comparator<ChildData>(){
        @Override
        public int compare(ChildData o1, ChildData o2) {
          return o1.getPath().compareTo(o2.getPath());
        }});
      this.keys = sortedChildren.iterator();
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
      delete(keyFromPath(current));
    }

    private String keyFromPath(ChildData data){
      String path = data.getPath();
      return path.substring(prefix.length(), path.length());
    }

    private class DeferredEntry implements Entry<String, V>{

      private ChildData data;

      public DeferredEntry(ChildData data) {
        super();
        this.data = data;
      }

      @Override
      public String getKey() {
        return keyFromPath(data);
      }

      @Override
      public V getValue() {
        try {
          return config.getSerializer().deserialize(data.getData());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public V setValue(V value) {
        throw new UnsupportedOperationException();
      }

    }

  }

  @Override
  public void close() {
    try{
      childrenCache.close();
    }catch(IOException e){
      logger.warn("Failure while closing out abstract store.", e);
    }
  }


}
