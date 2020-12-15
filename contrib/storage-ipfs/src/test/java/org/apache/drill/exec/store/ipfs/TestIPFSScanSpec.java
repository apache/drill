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


package org.apache.drill.exec.store.ipfs;

import org.apache.drill.categories.IPFSStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.exceptions.UserException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

@Category({SlowTest.class, IPFSStorageTest.class})
public class TestIPFSScanSpec extends IPFSTestBase implements IPFSTestConstants {
  @Mock
  private IPFSContext context;

  @Before
  public void before() {
    context = Mockito.mock(IPFSContext.class);
  }

  @Test
  public void testSimpleDatasetPath() {
    IPFSScanSpec spec = new IPFSScanSpec(context, IPFSTestConstants.getQueryPath(SIMPLE_DATASET_MULTIHASH));
    assertEquals(spec.getPath(), SIMPLE_DATASET_HASH_STRING);
  }

  @Test(expected = UserException.class)
  public void testInvalidPathWithBadPrefix() {
    IPFSScanSpec spec = new IPFSScanSpec(context, "/root/data/dataset.json");
  }

  @Test(expected = UserException.class)
  public void testInvalidPathWithNoExtension() {
    IPFSScanSpec spec = new IPFSScanSpec(context, String.format("/ipfs/%s", SIMPLE_DATASET_HASH_STRING));
  }

  @Test
  public void testPathWithCIDv1() {
    IPFSScanSpec spec = new IPFSScanSpec(context, IPFSTestConstants.getQueryPath(SIMPLE_DATASET_CID_V1_STRING));
    assertEquals(spec.getPath(), SIMPLE_DATASET_CID_V1_STRING);
  }

  @Test
  public void testChunkedDatasetPath() {
    IPFSScanSpec spec = new IPFSScanSpec(context, String.format("/ipfs/%s/1#json", CHUNKED_DATASET_HASH_STRING));
    assertEquals(spec.getPath(), CHUNKED_DATASET_HASH_STRING + "/1");
  }

  @Test
  public void testDataFileWithExplicitExtensionName() {
    IPFSScanSpec spec = new IPFSScanSpec(context, "/ipfs/QmSeX1YAGWMXoPrgeKBTq2Be6NdRzTVESeeWyt7mQFuvzo/1.json");
    assertEquals(spec.getPath(), "QmSeX1YAGWMXoPrgeKBTq2Be6NdRzTVESeeWyt7mQFuvzo/1.json");
  }
}
