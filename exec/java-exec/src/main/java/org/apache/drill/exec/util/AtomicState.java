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
package org.apache.drill.exec.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Internal.EnumLite;

/**
 * Simple wrapper class around AtomicInteger which allows management of a State value extending EnumLite.
 * @param <T> The type of EnumLite to use for state.
 */
public abstract class AtomicState<T extends EnumLite> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AtomicState.class);

  private final AtomicInteger state = new AtomicInteger();

  /**
   * Constructor that defines initial T state.
   * @param initial
   */
  public AtomicState(T initial){
    state.set(initial.getNumber());
  }

  protected abstract T getStateFromNumber(int i);

  /**
   * Does an atomic conditional update from one state to another.
   * @param oldState The expected current state.
   * @param newState The desired new state.
   * @return Whether or not the update was successful.
   */
  public boolean updateState(T oldState, T newState){
    return state.compareAndSet(oldState.getNumber(), newState.getNumber());
  }

  public T getState(){
    return getStateFromNumber(state.get());
  }
}
