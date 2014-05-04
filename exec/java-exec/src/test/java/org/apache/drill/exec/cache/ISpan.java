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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import com.google.hive12.common.collect.Lists;

public class ISpan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ISpan.class);


  public static void main(String[] args) throws Exception{
    GlobalConfiguration gc = new GlobalConfigurationBuilder().transport().defaultTransport().build();
    Configuration c = new ConfigurationBuilder() //
    .clustering().cacheMode(CacheMode.DIST_ASYNC) //
    .storeAsBinary()
    .build();
    EmbeddedCacheManager ecm = new DefaultCacheManager(gc, c);

    Cache<String, List<XT>> cache = ecm.getCache();
    List<XT> items = Lists.newArrayList();
    items.add(new XT(1));
    items.add(new XT(2));

    cache.put("items", items);
    for(XT x : cache.get("items")){
      System.out.println(x.i);
    }


  }

  private static class XT extends AbstractDataSerializable{

    int i =0;


    public XT(int i) {
      super();
      this.i = i;
    }

    @Override
    public void read(DataInput input) throws IOException {
      i = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeInt(i);
    }

    @Override
    public String toString() {
      return "XT [i=" + i + "]";
    }

  }
}
