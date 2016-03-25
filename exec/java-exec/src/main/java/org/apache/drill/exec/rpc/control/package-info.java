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

/**
 * From a control message exchange perspective, Drillbits form a unstructured peer-to-peer network. The bit initiating
 * a connection is the {@link org.apache.drill.exec.rpc.control.ControlClient}, and its peer is the
 * {@link org.apache.drill.exec.rpc.control.ControlServer} for the lifetime of this connection. Both peers are capable
 * of {@link org.apache.drill.exec.work.batch.ControlMessageHandler handling requests} defined in
 * {@link org.apache.drill.exec.rpc.control.ControlRpcConfig RPC configuration}. These messages are sent through a
 * {@link org.apache.drill.exec.rpc.control.ControlConnection}. A registry of all current connections to other bits is
 * maintained by a {@link org.apache.drill.exec.rpc.control.ConnectionManagerRegistry}.
 *
 * For developers: Use {@link org.apache.drill.exec.rpc.control.Controller#getTunnel} to get a
 * {@link org.apache.drill.exec.rpc.control.ControlTunnel} to a specific bit in the cluster. A tunnel is an abstraction
 * over a control connection that provides various methods to send messages, synchronously or asynchronously. Custom
 * messages can also be sent through {@link org.apache.drill.exec.rpc.control.ControlTunnel#getCustomTunnel custom
 * tunnels}.
 */
package org.apache.drill.exec.rpc.control;
