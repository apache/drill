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
package org.apache.drill.exec.compile.bytecode;

import org.objectweb.asm.Type;
import org.objectweb.asm.tree.analysis.BasicValue;

public class ReplacingBasicValue extends BasicValue{

  ValueHolderIden iden;
  int index;
  Type type;
  boolean isFunctionReturn = false;

  public ReplacingBasicValue(Type type, ValueHolderIden iden, int index) {
    super(type);
    this.index = index;
    this.iden = iden;
    this.type = type;
  }

  public void markFunctionReturn(){
    this.isFunctionReturn = true;
  }

  public void disableFunctionReturn(){
    this.isFunctionReturn = false;
  }

  public ValueHolderIden getIden() {
    return iden;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public Type getType(){
    return type;
  }

}
