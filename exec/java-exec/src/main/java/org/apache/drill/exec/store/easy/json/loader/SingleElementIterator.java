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
package org.apache.drill.exec.store.easy.json.loader;

import java.util.Iterator;

/**
 * It allows setting the current value in the iterator and can be used once after {@link #next} call
 *
 * @param <T> type of the value
 */
public class SingleElementIterator<T> implements Iterator<T> {
    private T value;

    @Override
    public boolean hasNext() {
      return value != null;
    }

    @Override
    public T next() {
      T value = this.value;
      this.value = null;
      return value;
    }

    public void setValue(T value) {
      this.value = value;
    }
  }
