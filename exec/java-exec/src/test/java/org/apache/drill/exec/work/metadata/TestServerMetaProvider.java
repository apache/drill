/*
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
package org.apache.drill.exec.work.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.UserProtos.GetServerMetaResp;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.ServerMeta;
import org.junit.Test;

/**
 * Tests for server metadata provider APIs.
 */
public class TestServerMetaProvider extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestServerMetaProvider.class);

  @Test
  public void testServerMeta() throws Exception {
    GetServerMetaResp resp = client.getServerMeta().get();
    assertNotNull(resp);
    assertEquals(RequestStatus.OK, resp.getStatus());
    assertNotNull(resp.getServerMeta());

    ServerMeta serverMeta = resp.getServerMeta();
    logger.trace("Server metadata: {}", serverMeta);

    assertEquals(Quoting.BACK_TICK.string, serverMeta.getIdentifierQuoteString());
  }
}
