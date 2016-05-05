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
package org.apache.drill.exec.physical.impl.common;

import org.apache.drill.common.logical.data.NamedExpression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonTypeName("hashtable-config")
public class HashTableConfig  {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashTableConfig.class);

  private final int initialCapacity;
  private final float loadFactor;
  private final List<NamedExpression> keyExprsBuild;
  private final List<NamedExpression> keyExprsProbe;

  @JsonCreator
  public HashTableConfig(@JsonProperty("initialCapacity") int initialCapacity, @JsonProperty("loadFactor") float loadFactor,
                         @JsonProperty("keyExprsBuild") List<NamedExpression> keyExprsBuild,
                         @JsonProperty("keyExprsProbe") List<NamedExpression> keyExprsProbe) {
    this.initialCapacity = initialCapacity;
    this.loadFactor = loadFactor;
    this.keyExprsBuild = keyExprsBuild;
    this.keyExprsProbe = keyExprsProbe;
  }

  public int getInitialCapacity() {
    return initialCapacity;
  }

  public float getLoadFactor() {
    return loadFactor;
  }

  public List<NamedExpression> getKeyExprsBuild() {
    return keyExprsBuild;
  }

  public List<NamedExpression> getKeyExprsProbe() {
    return keyExprsProbe;
  }

}
