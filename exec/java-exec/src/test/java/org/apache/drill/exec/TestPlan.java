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
package org.apache.drill.exec;

import org.apache.drill.exec.client.QuerySubmitter;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: sphillips
 * Date: 1/24/14
 * Time: 3:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestPlan {

  String location = "/Users/sphillips/hive-lineitem-orderkey";
  String type = "physical";
  String zkQuorum = null;
  boolean local = true;
  int bits = 1;


  @Test
  @Ignore
  public void testSubmitPlan() throws Exception {
    QuerySubmitter submitter = new QuerySubmitter();
    submitter.submitQuery(location, type, zkQuorum, local, bits);
  }
}
