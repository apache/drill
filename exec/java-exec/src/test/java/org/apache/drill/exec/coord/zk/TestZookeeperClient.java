/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.coord.zk;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.EnsurePath;
import org.apache.drill.common.collections.ImmutableEntry;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestZookeeperClient {
  private final static String root = "/test";
  private final static String path = "test-key";
  private final static String abspath = PathUtils.join(root, path);
  private final static byte[] data = "testing".getBytes();
  private final static CreateMode mode = CreateMode.PERSISTENT;

  private TestingServer server;
  private CuratorFramework curator;
  private ZookeeperClient client;

  static class ClientWithMockCache extends ZookeeperClient {
    private final PathChildrenCache cacheMock = Mockito.mock(PathChildrenCache.class);

    public ClientWithMockCache(final CuratorFramework curator, final String root, final CreateMode mode) {
      super(curator, root, mode);
    }

    @Override
    public PathChildrenCache getCache() {
      return cacheMock;
    }
  }

  @Before
  public void setUp() throws Exception {
    server = new TestingServer();
    final RetryPolicy policy = new RetryNTimes(1, 1000);
    curator = CuratorFrameworkFactory.newClient(server.getConnectString(), policy);
    client = new ClientWithMockCache(curator, root, mode);

    server.start();
    curator.start();
    client.start();
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    curator.close();
    server.close();
  }

  @Test
  public void testStartingClientEnablesCacheAndEnsuresRootNodeExists() throws Exception {
    Assert.assertTrue("start must create the root node", client.hasPath("", true));

    Mockito
        .verify(client.getCache())
        .start();
  }

  @Test
  public void testHasPathWithEventualConsistencyHitsCache() {
    final String path = "test-key";
    final String absPath = PathUtils.join(root, path);

    Mockito
        .when(client.getCache().getCurrentData(absPath))
        .thenReturn(null);

    Assert.assertFalse(client.hasPath(path)); // test convenience method

    Mockito
        .when(client.getCache().getCurrentData(absPath))
        .thenReturn(new ChildData(absPath, null, null));

    Assert.assertTrue(client.hasPath(path, false));    // test actual method
  }

  @Test(expected = DrillRuntimeException.class)
  public void testHasPathThrowsDrillRuntimeException() {
    final String path = "test-key";
    final String absPath = PathUtils.join(root, path);

    Mockito
        .when(client.getCache().getCurrentData(absPath))
        .thenThrow(Exception.class);

    client.hasPath(path);
  }

  @Test
  public void testPutAndGetWorks() {
    client.put(path, data);
    final byte[] actual = client.get(path, true);
    Assert.assertArrayEquals("data mismatch", data, actual);
  }

  @Test
  public void testGetWithEventualConsistencyHitsCache() {
    Mockito
        .when(client.getCache().getCurrentData(abspath))
        .thenReturn(null);

    Assert.assertEquals("get should return null", null, client.get(path));

    Mockito
        .when(client.getCache().getCurrentData(abspath))
        .thenReturn(new ChildData(abspath, null, data));

    Assert.assertEquals("get should return data", data, client.get(path, false));
  }

  @Test
  public void testCreate() throws Exception {
    client.create(path);
    Assert.assertTrue("path must exist", client.hasPath(path, true));

    // ensure invoking create also rebuilds cache
    Mockito
        .verify(client.getCache(), Mockito.times(1))
        .rebuildNode(abspath);
  }

  @Test
  public void testDelete() throws Exception {
    client.create(path);
    Assert.assertTrue("path must exist", client.hasPath(path, true));
    client.delete(path);
    Assert.assertFalse("path must not exist", client.hasPath(path, true));

    // ensure cache is rebuilt
    Mockito
        .verify(client.getCache(), Mockito.times(2))
        .rebuildNode(abspath);
  }


  @Test
  public void testEntriesReturnsRelativePaths() throws Exception {
    final ChildData child = Mockito.mock(ChildData.class);
    Mockito
        .when(child.getPath())
        .thenReturn(abspath);

    Mockito
        .when(child.getData())
        .thenReturn(data);

    final List<ChildData> children = Lists.newArrayList(child);
    Mockito
        .when(client.getCache().getCurrentData())
        .thenReturn(children);

    final Iterator<Map.Entry<String, byte[]>> entries = client.entries();

    // returned entry must contain the given relative path
    final Map.Entry<String, byte[]> expected = new ImmutableEntry<>(path, data);

    Assert.assertEquals("entries do not match", expected, entries.next());
  }


}
