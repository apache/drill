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


import org.apache.drill.exec.expr.holders.IntHolder;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;

public class ScalarReplacementNode extends MethodNode {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScalarReplacementNode.class);


  String[] exceptionsArr;
  MethodVisitor inner;

  public ScalarReplacementNode(int access, String name, String desc, String signature, String[] exceptions, MethodVisitor inner) {
    super(access, name, desc, signature, exceptions);
    this.exceptionsArr = exceptions;
    this.inner = inner;
  }


  @Override
  public void visitEnd() {
    super.visitEnd();

    Analyzer<BasicValue> a = new Analyzer<>(new ReplacingInterpreter());
    Frame<BasicValue>[] frames;
    try {
      frames = a.analyze("Object", this);
    } catch (AnalyzerException e) {
      throw new IllegalStateException(e);
    }
    TrackingInstructionList list = new TrackingInstructionList(frames, this.instructions);
    this.instructions = list;
    InstructionModifier holderV = new InstructionModifier(this.access, this.name, this.desc, this.signature, this.exceptionsArr, list, inner);
    accept(holderV);
  }


  IntHolder local;
  public void x(){
    IntHolder h = new IntHolder();
    local = h;
  }
}
