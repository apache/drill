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

import java.io.PrintWriter;

import org.apache.commons.io.output.StringBuilderWriter;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

public class ValueHolderReplacementVisitor extends ClassVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueHolderReplacementVisitor.class);

  public ValueHolderReplacementVisitor(ClassVisitor cw) {
    super(Opcodes.ASM4, cw);
  }



  @Override
  public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
    MethodVisitor innerVisitor = super.visitMethod(access, name, desc, signature, exceptions);
//     innerVisitor = new Debugger(access, name, desc, signature, exceptions, innerVisitor);
    return new ScalarReplacementNode(access, name, desc, signature, exceptions, innerVisitor);
  }

  private static class Debugger extends MethodNode {

    MethodVisitor inner;

    public Debugger(int access, String name, String desc, String signature, String[] exceptions, MethodVisitor inner) {
      super(access, name, desc, signature, exceptions);
      this.inner = inner;

    }

    @Override
    public void visitEnd() {
      try{
        accept(inner);
      }catch(Exception e){
        Textifier t = new Textifier();
        accept(new TraceMethodVisitor(t));
        StringBuilderWriter sw = new StringBuilderWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.print(pw);
        pw.flush();
        String bytecode = sw.getBuilder().toString();
        logger.error(String.format("Failure while rendering method %s, %s, %s.  ByteCode:\n %s", name, desc, signature, bytecode), e);
        throw new RuntimeException(String.format("Failure while rendering method %s, %s, %s.  ByteCode:\n %s", name, desc, signature, bytecode), e);
      }

    }

  }
}
