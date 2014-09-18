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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.collect.Lists;

class ValueHolderIden {

  final ObjectIntOpenHashMap<String> fieldMap;
  final Type[] types;
  final String[] names;
  final Type type;

  public ValueHolderIden(Class<?> c) {
    Field[] fields = c.getFields();

    List<Field> fldList = Lists.newArrayList();
    for (Field f : fields) {
      if (!Modifier.isStatic(f.getModifiers())) {
        fldList.add(f);
      }
    }
    this.type = Type.getType(c);
    this.types = new Type[fldList.size()];
    this.names = new String[fldList.size()];
    fieldMap = new ObjectIntOpenHashMap<String>();
    int i =0;
    for (Field f : fldList) {
      types[i] = Type.getType(f.getType());
      names[i] = f.getName();
      fieldMap.put(f.getName(), i);
      i++;
    }
  }

  private static void initType(int index, Type t, DirectSorter v) {
    switch(t.getSort()) {
    case Type.BOOLEAN:
    case Type.BYTE:
    case Type.CHAR:
    case Type.SHORT:
    case Type.INT:
      v.visitInsn(Opcodes.ICONST_0);
      v.directVarInsn(Opcodes.ISTORE, index);
      break;
    case Type.LONG:
      v.visitInsn(Opcodes.LCONST_0);
      v.directVarInsn(Opcodes.LSTORE, index);
      break;
    case Type.FLOAT:
      v.visitInsn(Opcodes.FCONST_0);
      v.directVarInsn(Opcodes.FSTORE, index);
      break;
    case Type.DOUBLE:
      v.visitInsn(Opcodes.DCONST_0);
      v.directVarInsn(Opcodes.DSTORE, index);
      break;
    case Type.OBJECT:
      v.visitInsn(Opcodes.ACONST_NULL);
      v.directVarInsn(Opcodes.ASTORE, index);
      break;
    default:
      throw new UnsupportedOperationException();
    }
  }

  public ValueHolderSub getHolderSub(DirectSorter adder) {
    int first = -1;
    for (int i = 0; i < types.length; i++) {
      int varIndex = adder.newLocal(types[i]);
      if (i == 0) {
        first = varIndex;
      }
    }

    return new ValueHolderSub(first);
  }

  public ValueHolderSub getHolderSubWithDefinedLocals(int first) {
    return new ValueHolderSub(first);
  }

  private int dup(Type t) {
    return t.getSize() == 1 ? Opcodes.DUP : Opcodes.DUP2;
  }

  public void transferToLocal(DirectSorter adder, int localVariable) {
    for (int i = 0; i < types.length; i++) {
      Type t = types[i];
      if (i + 1 < types.length) {
        adder.visitInsn(dup(t)); // don't dup for last value.
      }
      adder.visitFieldInsn(Opcodes.GETFIELD, type.getInternalName(), names[i], t.getDescriptor());
      adder.directVarInsn(t.getOpcode(Opcodes.ISTORE), localVariable+i);
    }
  }

  public int createLocalAndTrasfer(DirectSorter adder) {
    int first = 0;
    for (int i = 0; i < types.length; i++) {
      Type t = types[i];
      int varIndex = adder.newLocal(t);
      if (i == 0) {
        first = varIndex;
      }
    }
    transferToLocal(adder, first);
    return first;
  }

  public class ValueHolderSub {
    private int first;

    public ValueHolderSub(int first) {
      assert first != -1 : "Create Holder for sub that doesn't have any fields.";
      this.first = first;
    }

    public ValueHolderIden iden() {
      return ValueHolderIden.this;
    }

    public void init(DirectSorter mv) {
      for (int i = 0; i < types.length; i++) {
        initType(first+i, types[i], mv);
      }
    }
    public int size() {
      return types.length;
    }

    public int first() {
      return first;
    }

    public void updateFirst(int newFirst) {
      this.first = newFirst;
    }

    private int field(String name, InstructionModifier mv) {
      if (!fieldMap.containsKey(name)) {
        throw new IllegalArgumentException(String.format("Unknown name '%s' on line %d.", name, mv.lastLineNumber));
      }
      return fieldMap.lget();
    }

    public void addInsn(String name, InstructionModifier mv, int opcode) {
      switch (opcode) {
      case Opcodes.GETFIELD:
        addKnownInsn(name, mv, Opcodes.ILOAD);
        return;

      case Opcodes.PUTFIELD:
        addKnownInsn(name, mv, Opcodes.ISTORE);
      }
    }

    public void transfer(InstructionModifier mv, int newStart) {
      if (first == newStart) {
        return;
      }
      for (int i =0; i < types.length; i++) {
        mv.directVarInsn(types[i].getOpcode(Opcodes.ILOAD), first + i);
        mv.directVarInsn(types[i].getOpcode(Opcodes.ISTORE), newStart + i);
      }
      this.first = newStart;
    }

    private void addKnownInsn(String name, InstructionModifier mv, int analogOpcode) {
      int f = field(name, mv);
      Type t = types[f];
      mv.directVarInsn(t.getOpcode(analogOpcode), first + f);
    }

    public void transferToExternal(DirectSorter adder, String owner, String name, String desc) {

      // create a new object and assign it to the desired field.
      adder.visitTypeInsn(Opcodes.NEW, type.getInternalName());
      adder.visitInsn(dup(type));
      adder.visitMethodInsn(Opcodes.INVOKESPECIAL, type.getInternalName(), "<init>", "()V");

      // now we need to set all of the values of this new object.
      for (int i = 0; i < types.length; i++) {
        Type t = types[i];

        // dup the object where we are putting the field.
        adder.visitInsn(dup(type)); // dup for every as we want to save in place at end.
        adder.directVarInsn(t.getOpcode(Opcodes.ILOAD), first+i);
        adder.visitFieldInsn(Opcodes.PUTFIELD, type.getInternalName(), names[i], t.getDescriptor());
      }

      // lastly we save it to the desired field.
      adder.visitFieldInsn(Opcodes.PUTFIELD, owner, name, desc);
    }

  }

}
