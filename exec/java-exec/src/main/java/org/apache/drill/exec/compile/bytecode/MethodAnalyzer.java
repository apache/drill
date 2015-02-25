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

import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.Interpreter;
import org.objectweb.asm.tree.analysis.Value;

/**
 * Analyzer that allows us to inject additional functionality into ASMs basic analysis.
 *
 * <p>We need to be able to keep track of local variables that are assigned to each other
 * so that we can infer their replacability (for scalar replacement). In order to do that,
 * we need to know when local variables are assigned (with the old value being overwritten)
 * so that we can associate them with the new value, and hence determine whether they can
 * also be replaced, or not.
 *
 * <p>In order to capture the assignment operation, we have to provide our own Frame<>, but
 * ASM doesn't provide a direct way to do that. Here, we use the Analyzer's newFrame() methods
 * as factories that will provide our own derivative of Frame<> which we use to detect
 */
public class MethodAnalyzer<V extends Value> extends Analyzer <V> {
  /**
   * Custom Frame<> that captures setLocal() calls in order to associate values
   * that are assigned to the same local variable slot.
   *
   * <p>Since this is almost a pass-through, the constructors' arguments match
   * those from Frame<>.
   */
  private static class AssignmentTrackingFrame<V extends Value> extends Frame<V> {
    /**
     * Constructor.
     *
     * @param nLocals the number of locals the frame should have
     * @param nStack the maximum size of the stack the frame should have
     */
    public AssignmentTrackingFrame(final int nLocals, final int nStack) {
      super(nLocals, nStack);
    }

    /**
     * Copy constructor.
     *
     * @param src the frame being copied
     */
    public AssignmentTrackingFrame(final Frame<? extends V> src) {
      super(src);
    }

    @Override
    public void setLocal(final int i, final V value) {
      /*
       * If we're replacing one ReplacingBasicValue with another, we need to
       * associate them together so that they will have the same replacability
       * attributes. We also track the local slot the new value will be stored in.
       */
      if (value instanceof ReplacingBasicValue) {
        final ReplacingBasicValue replacingValue = (ReplacingBasicValue) value;
        replacingValue.setFrameSlot(i);
        final V localValue = getLocal(i);
        if ((localValue != null) && (localValue instanceof ReplacingBasicValue)) {
          final ReplacingBasicValue localReplacingValue = (ReplacingBasicValue) localValue;
          localReplacingValue.associate(replacingValue);
        }
      }

      super.setLocal(i, value);
    }
  }

  /**
   * Constructor.
   *
   * @param interpreter the interpreter to use
   */
  public MethodAnalyzer(final Interpreter<V> interpreter) {
    super(interpreter);
  }

  @Override
  protected Frame<V> newFrame(final int maxLocals, final int maxStack) {
    return new AssignmentTrackingFrame<V>(maxLocals, maxStack);
  }

  @Override
  protected Frame<V> newFrame(final Frame<? extends V> src) {
    return new AssignmentTrackingFrame<V>(src);
  }
}
