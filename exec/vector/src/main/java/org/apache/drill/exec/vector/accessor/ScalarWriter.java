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

import java.math.BigDecimal;

import org.apache.drill.exec.vector.VectorOverflowException;
import org.joda.time.Period;

/**
 * Methods common to the {@link ColumnWriter} and
 * {@link ArrayWriter} interfaces.
 */

public interface ScalarWriter extends ColumnAccessor {
  void setInt(int value) throws VectorOverflowException;
  void setLong(long value) throws VectorOverflowException;
  void setDouble(double value) throws VectorOverflowException;
  void setString(String value) throws VectorOverflowException;
  void setBytes(byte[] value, int len) throws VectorOverflowException;
  void setDecimal(BigDecimal value) throws VectorOverflowException;
  void setPeriod(Period value) throws VectorOverflowException;
}
