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
package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.StructVector;
import org.apache.drill.exec.vector.complex.StateTool;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class ComplexWriterImpl extends AbstractFieldWriter implements ComplexWriter {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComplexWriterImpl.class);

  private SingleStructWriter structRoot;
  private SingleListWriter listRoot;
  private final StructVector container;

  Mode mode = Mode.INIT;
  private final String name;
  private final boolean unionEnabled;

  private enum Mode { INIT, STRUCT, LIST };

  public ComplexWriterImpl(String name, StructVector container, boolean unionEnabled){
    super(null);
    this.name = name;
    this.container = container;
    this.unionEnabled = unionEnabled;
  }

  public ComplexWriterImpl(String name, StructVector container){
    this(name, container, false);
  }

  @Override
  public MaterializedField getField() {
    return container.getField();
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  private void check(Mode... modes){
    StateTool.check(mode, modes);
  }

  @Override
  public void reset(){
    setPosition(0);
  }

  @Override
  public void close() throws Exception {
    clear();
    structRoot.close();
    if (listRoot != null) {
      listRoot.close();
    }
  }

  @Override
  public void clear(){
    switch (mode) {
      case STRUCT:
        structRoot.clear();
        break;
      case LIST:
        listRoot.clear();
        break;
    }
  }

  @Override
  public void setValueCount(int count){
    switch (mode) {
      case STRUCT:
        structRoot.setValueCount(count);
        break;
      case LIST:
        listRoot.setValueCount(count);
        break;
    }
  }

  @Override
  public void setPosition(int index){
    super.setPosition(index);
    switch(mode){
    case STRUCT:
      structRoot.setPosition(index);
      break;
    case LIST:
      listRoot.setPosition(index);
      break;
    }
  }


  public StructWriter directMap(){
    Preconditions.checkArgument(name == null);

    switch(mode){

    case INIT:
      structRoot = new SingleStructWriter(container, this, unionEnabled);
      structRoot.setPosition(idx());
      mode = Mode.STRUCT;
      break;

    case STRUCT:
      break;

    default:
        check(Mode.INIT, Mode.STRUCT);
    }

    return structRoot;
  }

  @Override
  public StructWriter rootAsStruct() {
    switch(mode){

    case INIT:
      StructVector struct = container.addOrGet(name, Types.required(MinorType.STRUCT), StructVector.class);
      structRoot = new SingleStructWriter(struct, this, unionEnabled);
      structRoot.setPosition(idx());
      mode = Mode.STRUCT;
      break;

    case STRUCT:
      break;

    default:
        check(Mode.INIT, Mode.STRUCT);
    }

    return structRoot;
  }


  @Override
  public void allocate() {
    if(structRoot != null) {
      structRoot.allocate();
    } else if(listRoot != null) {
      listRoot.allocate();
    }
  }

  @Override
  public ListWriter rootAsList() {
    switch(mode){

    case INIT:
      listRoot = new SingleListWriter(name, container, this);
      listRoot.setPosition(idx());
      mode = Mode.LIST;
      break;

    case LIST:
      break;

    default:
        check(Mode.INIT, Mode.STRUCT);
    }

    return listRoot;
  }


}
