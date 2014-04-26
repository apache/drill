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
package org.apache.drill.exec.cache;

import com.google.common.base.Preconditions;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public abstract class ProtoMap<K extends MessageLite, V extends MessageLite, HK extends ProtoBufWrap<K>, HV extends ProtoBufWrap<V>> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoMap.class);

  private IMap<HK, HV> hzMap;
  
  public ProtoMap(HazelcastInstance instance, String mapName){
    hzMap = instance.getMap(mapName);
  }
  
  public V get(K key){
    Preconditions.checkNotNull(key);
    HK hk = getNewKey(key);
    HV hv = hzMap.get(hk);
    if(hv == null) return null;
    return hv.get();
  }
  
  public V put(K key, V value){
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    HV oldValue = hzMap.put(getNewKey(key), getNewValue(value));
    return oldValue == null ? null : oldValue.get();
  }
  
  public abstract HK getNewKey(K key);
  public abstract HV getNewValue(V key);
}
