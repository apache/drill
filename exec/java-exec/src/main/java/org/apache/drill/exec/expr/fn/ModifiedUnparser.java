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
import org.codehaus.janino.Unparser;
import org.codehaus.janino.util.AutoIndentWriter;

/**
 * This is a modified version of {@link Unparser} so that we can avoid
 * rendering few things.
 */
public class ModifiedUnparser extends Unparser {

  private String returnLabel;

  public ModifiedUnparser(Writer writer) {
    super(writer);
  }

  @Override
  public void unparseBlockStatement(Java.BlockStatement blockStatement) {
    // Unparser uses anonymous classes for visiting statements,
    // therefore added this check for customizing of handling ReturnStatement.
    if (blockStatement instanceof Java.ReturnStatement) {
      visitReturnStatement((Java.ReturnStatement) blockStatement);
    } else {
      super.unparseBlockStatement(blockStatement);
    }
  }

  /**
   * Parses specified {@link Java.MethodDeclarator}, wraps its content
   * with replaced {@code return} statements by {@code break} ones into the
   * block with label and stores it into {@link java.io.PrintWriter}.
   *
   * @param methodDeclarator method to parse
   */
  public void visitMethodDeclarator(Java.MethodDeclarator methodDeclarator) {
    if (methodDeclarator.optionalStatements == null) {
      pw.print(';');
    } else if (methodDeclarator.optionalStatements.isEmpty()) {
      pw.print(" {}");
    } else {
      pw.println(' ');
      // Add labels to handle return statements within function templates
      String[] fQCN = methodDeclarator.getDeclaringType().getClassName().split("\\.");
      returnLabel = fQCN[fQCN.length - 1] + "_" + methodDeclarator.name;
      pw.print(returnLabel);
      pw.println(": {");
      pw.print(AutoIndentWriter.INDENT);
      unparseStatements(methodDeclarator.optionalStatements);
      pw.print(AutoIndentWriter.UNINDENT);
      pw.println("}");
      pw.print(' ');
    }
  }

  private void visitReturnStatement(Java.ReturnStatement returnStatement) {
    pw.print("break ");
    pw.print(returnLabel);
    if (returnStatement.optionalReturnValue != null) {
      pw.print(' ');
      unparseAtom(returnStatement.optionalReturnValue);
    }
    pw.print(';');
  }

  /**
   * The following helper method is copied from the parent class since it
   * is declared as private in the parent class and can not be used in the child
   * class (this).
   */
  private void unparseStatements(List<? extends Java.BlockStatement> statements) {
    int state = -1;
    for (Java.BlockStatement bs : statements) {
      int x = (
        bs instanceof Java.Block ? 1 :
          bs instanceof Java.LocalClassDeclarationStatement ? 2 :
            bs instanceof Java.LocalVariableDeclarationStatement ? 3 :
              bs instanceof Java.SynchronizedStatement ? 4 : 99
      );
      if (state != -1 && state != x) {
        pw.println(AutoIndentWriter.CLEAR_TABULATORS);
      }
      state = x;

      unparseBlockStatement(bs);
      pw.println();
    }
  }
}
