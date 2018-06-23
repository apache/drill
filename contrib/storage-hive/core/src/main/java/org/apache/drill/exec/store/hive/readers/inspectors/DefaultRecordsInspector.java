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
package org.apache.drill.exec.store.hive.readers.inspectors;

/**
 * Default records inspector that uses the same value holder for each record.
 * Each value once written is immediately processed thus value holder can be re-used.
 */
public class DefaultRecordsInspector extends AbstractRecordsInspector {

  private final Object value;

  public DefaultRecordsInspector(Object value) {
    this.value = value;
  }

  @Override
  public Object getValueHolder() {
    return value;
  }

  @Override
  public Object getNextValue() {
    return value;
  }

}
