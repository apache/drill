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
package org.apache.drill.exec.vector.complex.impl;

import java.util.Iterator;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.complex.reader.FieldReader;


abstract class AbstractBaseReader implements FieldReader{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractBaseReader.class);

  private int index;

  public AbstractBaseReader() {
    super();
  }

  public void setPosition(int index){
    this.index = index;
  }

  int idx(){
    return index;
  }

  @Override
  public Iterator<String> iterator() {
    throw new IllegalStateException("The current reader doesn't support reading as a map.");
  }

  public MajorType getType(){
    throw new IllegalStateException("The current reader doesn't support getting type information.");
  }

  @Override
  public boolean next() {
    throw new IllegalStateException("The current reader doesn't support getting next information.");
  }

  @Override
  public int size() {
    throw new IllegalStateException("The current reader doesn't support getting size information.");
  }
}
