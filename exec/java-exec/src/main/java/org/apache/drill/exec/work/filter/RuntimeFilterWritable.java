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
package org.apache.drill.exec.work.filter;


import io.netty.buffer.DrillBuf;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.exec.proto.BitData;

import java.util.ArrayList;
import java.util.List;

/**
 * A binary wire transferable representation of the RuntimeFilter which contains
 * the runtime filter definition and its corresponding data.
 */
public class RuntimeFilterWritable implements AutoCloseables.Closeable {

  private BitData.RuntimeFilterBDef runtimeFilterBDef;

  private DrillBuf[] data;

  public RuntimeFilterWritable(BitData.RuntimeFilterBDef runtimeFilterBDef, DrillBuf... data) {
    this.runtimeFilterBDef = runtimeFilterBDef;
    this.data = data;
  }


  public BitData.RuntimeFilterBDef getRuntimeFilterBDef() {
    return runtimeFilterBDef;
  }

  public DrillBuf[] getData() {
    return data;
  }

  public void setData(DrillBuf... data) {
    this.data = data;
  }


  public List<BloomFilter> unwrap() {
    List<Integer> sizeInBytes = runtimeFilterBDef.getBloomFilterSizeInBytesList();
    List<BloomFilter> bloomFilters = new ArrayList<>(sizeInBytes.size());
    for (int i = 0; i < sizeInBytes.size(); i++) {
      DrillBuf byteBuf = data[i];
      int offset = 0;
      int size = sizeInBytes.get(i);
      DrillBuf bloomFilterContent = byteBuf.slice(offset, size);
      BloomFilter bloomFilter = new BloomFilter(bloomFilterContent);
      bloomFilters.add(bloomFilter);
    }
    return bloomFilters;
  }

  public void aggregate(RuntimeFilterWritable runtimeFilterWritable) {
    List<BloomFilter> thisFilters = this.unwrap();
    List<BloomFilter> otherFilters = runtimeFilterWritable.unwrap();
    for (int i = 0; i < thisFilters.size(); i++) {
      BloomFilter thisOne = thisFilters.get(i);
      BloomFilter otherOne = otherFilters.get(i);
      thisOne.or(otherOne);
    }
    for (BloomFilter bloomFilter : otherFilters) {
      bloomFilter.getContent().clear();
    }
  }

  @Override
  public void close() {
    for (DrillBuf buf : data) {
      buf.release();
    }
  }

}
