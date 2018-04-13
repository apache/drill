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
package org.apache.drill.exec.compile.bytecode;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.TypePath;


/**
 * Remove any adjacent ALOAD-POP instruction pairs.
 *
 * The Janino compiler generates an instruction stream where it will ALOAD a
 * holder's objectref, and then immediately POP it because the compiler has
 * recognized that the method call that it loaded the objectref for is static
 * (i.e., no this pointer is required). This causes a problem with our scalar
 * replacement strategy, because once we remove the holders, we simply eliminate
 * the ALOAD instructions. In this case, the POP gets left behind, and breaks
 * the program stack.
 *
 * This class looks for ALOADs that are immediately followed by a POP, and removes
 * the pair of instructions altogether.
 *
 * It is unknown if other compilers besides Janino generate this instruction sequence,
 * but to be on the safe side, we'll use this class unconditionally to filter bytecode.
 *
 * TODO: this might be easier to do by going off an InsnList; the state machine would
 * be in the loop that visits the instructions then.
 */
public class AloadPopRemover extends MethodVisitor {
  private final static int NONE = -1; // var value to indicate no captured ALOAD
  private int var = NONE;

  /**
   * Constructor.
   *
   * See {@link org.objectweb.asm.MethodVisitor#MethodVisitor(int)}.
   */
  public AloadPopRemover(final int api) {
    super(api);
  }

  /**
   * Constructor.
   *
   * See {@link org.objectweb.asm.MethodVisitor#MethodVisitor(int, MethodVisitor)}.
   */
  public AloadPopRemover(final int api, final MethodVisitor mv) {
    super(api, mv);
  }

  /**
   * Process a deferred ALOAD instruction, if there is one.
   *
   * If there is no deferred ALOAD, does nothing, and returns false.
   *
   * If there is a deferred ALOAD, and we're on a POP instruction
   * (indicated by onPop), does nothing (the ALOAD is not forwarded),
   * and returns true.
   *
   * If there is a deferred ALOAD and we're not on a POP instruction,
   * forwards the deferred ALOAD, and returns false
   *
   * @param onPop true if the current instruction is POP
   * @return true if onPop and there was a deferred ALOAD, false otherwise;
   *   basically, returns true if the ALOAD-POP optimization is required
   */
  private boolean processDeferredAload(final boolean onPop) {
    // if the previous instruction wasn't an ALOAD, then there's nothing to do
    if (var == NONE) {
      return false;
    }

    // clear the variable index, but save it for local use
    final int localVar = var;
    var = NONE;

    // if the next instruction is a POP, don't emit the deferred ALOAD
    if (onPop) {
      return true;
    }

    // if we got here, we're not on a POP, so emit the deferred ALOAD
    super.visitVarInsn(Opcodes.ALOAD, localVar);
    return false;
  }

  @Override
  public AnnotationVisitor visitAnnotation(final String desc, final boolean visible) {
    processDeferredAload(false);
    final AnnotationVisitor annotationVisitor = super.visitAnnotation(desc, visible);
    return annotationVisitor;
  }

  @Override
  public AnnotationVisitor visitAnnotationDefault() {
    processDeferredAload(false);
    final AnnotationVisitor annotationVisitor = super.visitAnnotationDefault();
    return annotationVisitor;
  }

  @Override
  public void visitAttribute(final Attribute attr) {
    processDeferredAload(false);
    super.visitAttribute(attr);
  }

  @Override
  public void visitCode() {
    processDeferredAload(false);
    super.visitCode();
  }

  @Override
  public void visitEnd() {
    processDeferredAload(false);
    super.visitEnd();
  }

  @Override
  public void visitFieldInsn(final int opcode, final String owner, final String name,
      final String desc) {
    processDeferredAload(false);
    super.visitFieldInsn(opcode, owner, name, desc);
  }

  @Override
  public void visitFrame(final int type, final int nLocal,
      final Object[] local, final int nStack, final Object[] stack) {
    processDeferredAload(false);
    super.visitFrame(type, nLocal, local, nStack, stack);
  }

  @Override
  public void visitIincInsn(final int var, final int increment) {
    processDeferredAload(false);
    super.visitIincInsn(var, increment);
  }

  @Override
  public void visitInsn(final int opcode) {
    /*
     * If we don't omit an ALOAD with a following POP, then forward this.
     * In other words, if we omit an ALOAD because we're on the following POP,
     * don't forward this POP, which omits the ALOAD-POP pair.
     */
    if (!processDeferredAload(Opcodes.POP == opcode)) {
      super.visitInsn(opcode);
    }
  }

  @Override
  public AnnotationVisitor visitInsnAnnotation(final int typeRef,
      final TypePath typePath, final String desc, final boolean visible) {
    processDeferredAload(false);
    final AnnotationVisitor annotationVisitor = super.visitInsnAnnotation(
        typeRef, typePath, desc, visible);
    return annotationVisitor;
  }

  @Override
  public void visitIntInsn(final int opcode, final int operand) {
    processDeferredAload(false);
    super.visitIntInsn(opcode, operand);
  }

  @Override
  public void visitInvokeDynamicInsn(final String name, final String desc,
      final Handle bsm, final Object... bsmArgs) {
    processDeferredAload(false);
    super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
  }

  @Override
  public void visitJumpInsn(final int opcode, final Label label) {
    processDeferredAload(false);
    super.visitJumpInsn(opcode, label);
  }

  @Override
  public void visitLabel(final Label label) {
    processDeferredAload(false);
    super.visitLabel(label);
  }

  @Override
  public void visitLdcInsn(final Object cst) {
    processDeferredAload(false);
    super.visitLdcInsn(cst);
  }

  @Override
  public void visitLineNumber(final int line, final Label start) {
    processDeferredAload(false);
    super.visitLineNumber(line, start);
  }

  @Override
  public void visitLocalVariable(final String name, final String desc,
      final String signature, final Label start, final Label end, final int index) {
    processDeferredAload(false);
    super.visitLocalVariable(name, desc, signature, start, end, index);
  }

  @Override
  public AnnotationVisitor visitLocalVariableAnnotation(final int typeRef,
      final TypePath typePath, final Label[] start, final Label[] end,
      final int[] index, final String desc, final boolean visible) {
    processDeferredAload(false);
    final AnnotationVisitor annotationVisitor =
        super.visitLocalVariableAnnotation(typeRef, typePath, start, end, index,
            desc, visible);
    return annotationVisitor;
  }

  @Override
  public void visitLookupSwitchInsn(final Label dflt, final int[] keys,
      final Label[] labels) {
    processDeferredAload(false);
    super.visitLookupSwitchInsn(dflt, keys, labels);
  }

  @Override
  public void visitMaxs(final int maxStack, final int maxLocals) {
    processDeferredAload(false);
    super.visitMaxs(maxStack, maxLocals);
  }

  @Deprecated
  @Override
  public void visitMethodInsn(final int opcode, final String owner,
      final String name, final String desc) {
    processDeferredAload(false);
    super.visitMethodInsn(opcode, owner, name, desc);
  }

  @Override
  public void visitMethodInsn(final int opcode, final String owner,
      final String name, final String desc, final boolean itf) {
    processDeferredAload(false);
    super.visitMethodInsn(opcode, owner, name, desc, itf);
  }

  @Override
  public void visitMultiANewArrayInsn(final String desc, final int dims) {
    processDeferredAload(false);
    super.visitMultiANewArrayInsn(desc, dims);
  }

  @Override
  public void visitParameter(final String name, final int access) {
    processDeferredAload(false);
    super.visitParameter(name, access);
  }

  @Override
  public AnnotationVisitor visitParameterAnnotation(final int parameter,
      final String desc, final boolean visible) {
    processDeferredAload(false);
    final AnnotationVisitor annotationVisitor =
        super.visitParameterAnnotation(parameter, desc, visible);
    return annotationVisitor;
  }

  @Override
  public void visitTableSwitchInsn(final int min, final int max,
      final Label dflt, final Label... labels) {
    processDeferredAload(false);
    super.visitTableSwitchInsn(min, max, dflt, labels);
  }

  @Override
  public AnnotationVisitor visitTryCatchAnnotation(final int typeRef,
      final TypePath typePath, final String desc, final boolean visible) {
    processDeferredAload(false);
    final AnnotationVisitor annotationVisitor =
        super.visitTryCatchAnnotation(typeRef, typePath, desc, visible);
    return annotationVisitor;
  }

  @Override
  public void visitTryCatchBlock(final Label start, final Label end,
      final Label handler, final String type) {
    processDeferredAload(false);
    super.visitTryCatchBlock(start, end, handler, type);
  }

  @Override
  public AnnotationVisitor visitTypeAnnotation(final int typeRef,
      final TypePath typePath, final String desc, final boolean visible) {
    processDeferredAload(false);
    final AnnotationVisitor annotationVisitor =
        super.visitTypeAnnotation(typeRef, typePath, desc, visible);
    return annotationVisitor;
  }

  @Override
  public void visitTypeInsn(final int opcode, final String type) {
    processDeferredAload(false);
    super.visitTypeInsn(opcode, type);
  }

  @Override
  public void visitVarInsn(final int opcode, final int var) {
    processDeferredAload(false);

    // if we see an ALOAD, defer forwarding it until we know what the next instruction is
    if (Opcodes.ALOAD == opcode) {
      this.var = var;
    } else {
      super.visitVarInsn(opcode, var);
    }
  }
}
