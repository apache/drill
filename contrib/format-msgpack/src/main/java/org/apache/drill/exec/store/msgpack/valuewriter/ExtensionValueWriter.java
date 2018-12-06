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
package org.apache.drill.exec.store.msgpack.valuewriter;

import org.msgpack.core.ExtensionTypeHeader;

/**
 * This interface handles msgpack extended types. You can implemented your own
 * handler. You register your handler via the Java Plugin Service. That is using
 * META-INF/org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter.
 * You can take a look at the
 * {@link org.apache.drill.exec.store.msgpack.valuewriter.TimestampValueWriter}
 * as an example.
 */
public interface ExtensionValueWriter extends ScalarValueWriter {

  /**
   * @return 0 to 127 are application specific types.
   */
  public byte getExtensionTypeNumber();

  public void setExtensionTypeHeader(ExtensionTypeHeader header);

}
