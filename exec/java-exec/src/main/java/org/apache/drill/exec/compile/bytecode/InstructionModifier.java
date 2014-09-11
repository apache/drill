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

import org.apache.drill.exec.compile.bytecode.ValueHolderIden.ValueHolderSub;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;

public class InstructionModifier extends MethodVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InstructionModifier.class);

  private final IntObjectOpenHashMap<ValueHolderIden.ValueHolderSub> oldToNew = new IntObjectOpenHashMap<>();
  private final IntIntOpenHashMap oldLocalToFirst = new IntIntOpenHashMap();

  private DirectSorter adder;
  int lastLineNumber = 0;
  private TrackingInstructionList list;

  public InstructionModifier(int access, String name, String desc, String signature, String[] exceptions,
      TrackingInstructionList list, MethodVisitor inner) {
    super(Opcodes.ASM4, new DirectSorter(access, desc, inner));
    this.list = list;
    this.adder = (DirectSorter) mv;
  }

  public void setList(TrackingInstructionList list) {
    this.list = list;
  }

  private ReplacingBasicValue local(int var) {
    Object o = list.currentFrame.getLocal(var);
    if (o instanceof ReplacingBasicValue) {
      return (ReplacingBasicValue) o;
    }
    return null;
  }

  private ReplacingBasicValue popCurrent() {
    return popCurrent(false);
  }

  private ReplacingBasicValue popCurrent(boolean includeReturnVals) {
    // for vararg, we could try to pop an empty stack. TODO: handle this better.
    if (list.currentFrame.getStackSize() == 0) {
      return null;
    }

    Object o = list.currentFrame.pop();
    if (o instanceof ReplacingBasicValue) {
      ReplacingBasicValue v = (ReplacingBasicValue) o;
      if (!v.isFunctionReturn || includeReturnVals) {
        return v;
      }
    }
    return null;
  }

  private ReplacingBasicValue getReturn() {
    Object o = list.nextFrame.getStack(list.nextFrame.getStackSize() - 1);
    if (o instanceof ReplacingBasicValue) {
      return (ReplacingBasicValue) o;
    }
    return null;
  }

  @Override
  public void visitInsn(int opcode) {
    switch (opcode) {
    case Opcodes.DUP:
      if (popCurrent() != null) {
        return;
      }
    }
    super.visitInsn(opcode);
  }

  @Override
  public void visitTypeInsn(int opcode, String type) {
    ReplacingBasicValue r = getReturn();
    if (r != null) {
      ValueHolderSub sub = r.getIden().getHolderSub(adder);
      oldToNew.put(r.getIndex(), sub);
    } else {
      super.visitTypeInsn(opcode, type);
    }
  }

  @Override
  public void visitLineNumber(int line, Label start) {
    lastLineNumber = line;
    super.visitLineNumber(line, start);
  }

  @Override
  public void visitVarInsn(int opcode, int var) {
    ReplacingBasicValue v;
    if (opcode == Opcodes.ASTORE && (v = popCurrent(true)) != null) {
      if (!v.isFunctionReturn) {
        ValueHolderSub from = oldToNew.get(v.getIndex());

        ReplacingBasicValue current = local(var);
        // if local var is set, then transfer to it to the existing holders in the local position.
        if (current != null) {
          if (oldToNew.get(current.getIndex()).iden() == from.iden()) {
            int targetFirst = oldToNew.get(current.index).first();
            from.transfer(this, targetFirst);
            return;
          }
        }

        // if local var is not set, then check map to see if existing holders are mapped to local var.
        if (oldLocalToFirst.containsKey(var)) {
          ValueHolderSub sub = oldToNew.get(oldLocalToFirst.lget());
          if (sub.iden() == from.iden()) {
            // if they are, then transfer to that.
            from.transfer(this, oldToNew.get(oldLocalToFirst.lget()).first());
            return;
          }
        }


        // map from variables to global space for future use.
        oldLocalToFirst.put(var, v.getIndex());

      } else {
        // this is storage of a function return, we need to map the fields to the holder spots.
        int first;
        if (oldLocalToFirst.containsKey(var)) {
          first = oldToNew.get(oldLocalToFirst.lget()).first();
          v.iden.transferToLocal(adder, first);
        } else {
          first = v.iden.createLocalAndTrasfer(adder);
        }
        ValueHolderSub from = v.iden.getHolderSubWithDefinedLocals(first);
        oldToNew.put(v.getIndex(), from);
        v.disableFunctionReturn();
      }

    } else if (opcode == Opcodes.ALOAD && (v = getReturn()) != null) {
      // noop.
    } else {
      super.visitVarInsn(opcode, var);
    }

  }

  void directVarInsn(int opcode, int var) {
    adder.directVarInsn(opcode, var);
  }

  @Override
  public void visitFieldInsn(int opcode, String owner, String name, String desc) {
    ReplacingBasicValue v;

    switch (opcode) {
    case Opcodes.PUTFIELD:
      // pop twice for put.
      v = popCurrent(true);
      if (v != null) {
        if (v.isFunctionReturn) {
          super.visitFieldInsn(opcode, owner, name, desc);
          return;
        } else {
          // we are trying to store a replaced variable in an external context, we need to generate an instance and
          // transfer it out.
          ValueHolderSub sub = oldToNew.get(v.getIndex());
          sub.transferToExternal(adder, owner, name, desc);
          return;
        }
      }

    case Opcodes.GETFIELD:
      // pop again.
      v = popCurrent();
      if (v != null) {
        // super.visitFieldInsn(opcode, owner, name, desc);
        ValueHolderSub sub = oldToNew.get(v.getIndex());
        sub.addInsn(name, this, opcode);
        return;
      }
    }

    super.visitFieldInsn(opcode, owner, name, desc);
  }

  @Override
  public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
    // if (!ReplacingInterpreter.HOLDER_DESCRIPTORS.contains(desc)) {
    // super.visitLocalVariable(name, desc, signature, start, end, index);
    // }
  }

  @Override
  public void visitMethodInsn(int opcode, String owner, String name, String desc) {
    final int len = Type.getArgumentTypes(desc).length;
    final boolean isStatic = opcode == Opcodes.INVOKESTATIC;

    ReplacingBasicValue obj = popCurrent();

    if (obj != null && !isStatic) {
      if ("<init>".equals(name)) {
        oldToNew.get(obj.getIndex()).init(adder);
      } else {
        throw new IllegalStateException("you can't call a method on a value holder.");
      }
      return;
    }

    obj = getReturn();

    if (obj != null) {
      // the return of this method is an actual instance of the object we're escaping. Update so that it get's mapped
      // correctly.
      super.visitMethodInsn(opcode, owner, name, desc);
      obj.markFunctionReturn();
      return;
    }

    int i = isStatic ? 1 : 0;
    for (; i < len; i++) {
      checkArg(name, popCurrent());
    }

    super.visitMethodInsn(opcode, owner, name, desc);
  }

  private void checkArg(String name, ReplacingBasicValue obj) {
    if (obj == null) {
      return;
    }
    throw new IllegalStateException(
        String
            .format(
                "Holder types are not allowed to be passed between methods.  Ran across problem attempting to invoke method '%s' on line number %d",
                name, lastLineNumber));
  }

}
