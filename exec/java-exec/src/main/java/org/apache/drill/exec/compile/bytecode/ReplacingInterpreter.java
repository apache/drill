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

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicInterpreter;
import org.objectweb.asm.tree.analysis.BasicValue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ReplacingInterpreter extends BasicInterpreter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReplacingInterpreter.class);

  private int index = 0;

  @Override
  public BasicValue newValue(Type t) {
    if(t != null){
      ValueHolderIden iden = HOLDERS.get(t.getDescriptor());
      if(iden != null){
        ReplacingBasicValue v = new ReplacingBasicValue(t, iden, index++);
        v.markFunctionReturn();
        return v;
      }
    }
    return super.newValue(t);

  }

  @Override
  public BasicValue newOperation(AbstractInsnNode insn) throws AnalyzerException {
    if(insn.getOpcode() == Opcodes.NEW){
      TypeInsnNode t = (TypeInsnNode) insn;
      ValueHolderIden iden = HOLDERS.get(t.desc);

      if(iden != null){
        return new ReplacingBasicValue(Type.getObjectType(t.desc), iden, index++);
      }
    }

    return super.newOperation(insn);
  }

  private static String desc(Class<?> c) {
    Type t = Type.getType(c);
    return t.getDescriptor();
  }



  static {

    ImmutableMap.Builder<String, ValueHolderIden> builder = ImmutableMap.builder();
    ImmutableSet.Builder<String> setB = ImmutableSet.builder();
    for (Class<?> c : ScalarReplacementTypes.CLASSES) {
      String desc = desc(c);
      setB.add(desc);
      String desc2 = desc.substring(1, desc.length() - 1);
      ValueHolderIden vhi = new ValueHolderIden(c);
      builder.put(desc, vhi);
      builder.put(desc2, vhi);
    }
    HOLDER_DESCRIPTORS = setB.build();
    HOLDERS = builder.build();
  }

  private final static ImmutableMap<String, ValueHolderIden> HOLDERS;
  public final static ImmutableSet<String> HOLDER_DESCRIPTORS;


}
