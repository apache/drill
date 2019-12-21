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
package org.apache.drill.exec.vector.accessor;

import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.Period;

import java.math.BigDecimal;

public class AbstractKeyAccessor implements KeyAccessor {

  protected final DictReader dictReader;
  protected final ScalarReader keyReader;

  protected AbstractKeyAccessor(DictReader dictReader, ScalarReader keyReader) {
    this.dictReader = dictReader;
    this.keyReader = keyReader;
  }

  @Override
  public boolean find(boolean key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support boolean key.");
  }

  @Override
  public boolean find(int key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support int key.");
  }

  @Override
  public boolean find(BigDecimal key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support BigDecimal key.");
  }

  @Override
  public boolean find(double key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support double key.");
  }

  @Override
  public boolean find(long key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support long key.");
  }

  @Override
  public boolean find(String key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support String key.");
  }

  @Override
  public boolean find(byte[] key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support byte[] key.");
  }

  @Override
  public boolean find(Period key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support Period key.");
  }

  @Override
  public boolean find(LocalDate key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support LocalDate key.");
  }

  @Override
  public boolean find(LocalTime key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support LocalTime key.");
  }

  @Override
  public boolean find(Instant key) {
    throw new UnsupportedOperationException(this.getClass().getName() + " does not support Instant key.");
  }
}
