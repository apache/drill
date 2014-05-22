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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public abstract class ProtoSerializable<V extends Message> extends AbstractStreamSerializable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoSerializable.class);

  private Parser<V> protoParser;
  private V obj;

  ProtoSerializable(Parser<V> protoParser, V obj) {
    super();
    this.protoParser = protoParser;
    this.obj = obj;
  }

  public V getObject(){
    return obj;
  }

  @Override
  public void readFromStream(InputStream input) throws IOException {
    obj = protoParser.parseDelimitedFrom(input);
  }

  @Override
  public void writeToStream(OutputStream output) throws IOException {
    obj.writeDelimitedTo(output);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((obj == null) ? 0 : obj.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ProtoSerializable other = (ProtoSerializable) obj;
    if (this.obj == null) {
      if (other.obj != null)
        return false;
    } else if (!this.obj.equals(other.obj))
      return false;
    return true;
  }

  public static class PlanFragmentSerializable extends ProtoSerializable<PlanFragment>{
    public PlanFragmentSerializable(PlanFragment obj) {super(PlanFragment.PARSER, obj);}
    public PlanFragmentSerializable(){this(null);}
  }
  public static class FragmentHandleSerializable extends ProtoSerializable<FragmentHandle>{
    public FragmentHandleSerializable(FragmentHandle obj) {super(FragmentHandle.PARSER, obj);}
    public FragmentHandleSerializable(){this(null);}
  }

}
