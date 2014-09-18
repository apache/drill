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
package org.apache.drill.exec.rpc.control;

import java.io.Closeable;

import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

/**
 * Service that allows one Drillbit to communicate with another. Internally manages whether each particular bit is a
 * server or a client depending on who initially made the connection. If no connection exists, the Controller is responsible for
 * making a connection. TODO: Controller should automatically straight route local BitCommunication rather than connecting to its
 * self.
 */
public interface Controller extends Closeable {

  /**
   * Get a Bit to Bit communication tunnel. If the BitCom doesn't have a tunnel attached to the node already, it will
   * start creating one. This create the connection asynchronously.
   *
   * @param node
   * @return
   */
  public ControlTunnel getTunnel(DrillbitEndpoint node) ;

  public DrillbitEndpoint start(DrillbitEndpoint partialEndpoint) throws InterruptedException, DrillbitStartupException;


}
