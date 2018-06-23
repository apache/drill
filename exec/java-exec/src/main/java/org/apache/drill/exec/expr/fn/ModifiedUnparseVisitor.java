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
package org.apache.drill.exec.expr.fn;

import java.io.Writer;
import java.util.List;

import org.codehaus.janino.Java;
import org.codehaus.janino.UnparseVisitor;
import org.codehaus.janino.util.AutoIndentWriter;

/**
 * This is a modified version of {@link UnparseVisitor} so that we can avoid
 * rendering few things. Based on janino version 2.7.4.
 */
public class ModifiedUnparseVisitor extends UnparseVisitor {

  private String returnLabel;

  /**
   * Testing of parsing/unparsing.
   * <p>
   * Reads compilation units from the files named on the command line
   * and unparses them to {@link System#out}.
   */
  public static void main(String[] args) throws Exception {
    UnparseVisitor.main(args);
  }

  /**
   * Unparse the given {@link org.codehaus.janino.Java.CompilationUnit} to the given {@link java.io.Writer}.
   */
  public static void unparse(Java.CompilationUnit cu, Writer w) {
    UnparseVisitor.unparse(cu, w);
  }

  public ModifiedUnparseVisitor(Writer w) {
    super(w);
  }

  @Override
  public void visitMethodDeclarator(Java.MethodDeclarator md) {
    if (md.optionalStatements == null) {
      this.pw.print(';');
    } else
      if (md.optionalStatements.isEmpty()) {
        this.pw.print(" {}");
      } else {
        this.pw.println(' ');
        // Add labels to handle return statements within function templates
        String[] fQCN = md.getDeclaringType().getClassName().split("\\.");
        returnLabel = fQCN[fQCN.length - 1] + "_" + md.name;
        this.pw.println(returnLabel + ": {");
        this.pw.print(AutoIndentWriter.INDENT);
        this.unparseStatements(md.optionalStatements);
        this.pw.print(AutoIndentWriter.UNINDENT);
        this.pw.println("}");
        this.pw.print(' ');
      }
  }

  @Override
  public void visitReturnStatement(Java.ReturnStatement rs) {
    this.pw.print("break " + returnLabel);
    if (rs.optionalReturnValue != null) {
      this.pw.print(' ');
      this.unparse(rs.optionalReturnValue);
    }
    this.pw.print(';');
  }

  /*
   * The following helper methods are copied from the parent class since they
   * are declared private in the parent class and can not be used in the child
   * class (this).
   */

  private void unparseStatements(List<? extends Java.BlockStatement> statements) {
    int state = -1;
    for (Java.BlockStatement bs : statements) {
      int x  = (
          bs instanceof Java.Block                             ? 1 :
          bs instanceof Java.LocalClassDeclarationStatement    ? 2 :
          bs instanceof Java.LocalVariableDeclarationStatement ? 3 :
          bs instanceof Java.SynchronizedStatement             ? 4 :
          99
      );
      if (state != -1 && state != x) {
        this.pw.println(AutoIndentWriter.CLEAR_TABULATORS);
      }
      state = x;

      this.unparseBlockStatement(bs);
      this.pw.println();
    }
  }

  private void unparseBlockStatement(Java.BlockStatement blockStatement) {
    blockStatement.accept(this);
  }

  private void unparse(Java.Atom operand) {
    operand.accept(this);
  }
}
