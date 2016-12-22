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
package org.apache.drill.exec.store.mock;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;

public class DateGen implements FieldGen {

  private Random rand = new Random( );
  private long baseTime;
  private SimpleDateFormat fmt;

  public DateGen( ) {
    // Start a year ago.
    baseTime = System.currentTimeMillis() - 365 * 24 * 60 * 60 * 1000;
    fmt = new SimpleDateFormat( "yyyy-mm-DD" );
  }

  @Override
  public void setup(ColumnDef colDef) { }

  public long value( ) {
    return baseTime + rand.nextInt( 365 ) * 24 * 60 * 60 * 1000;
  }

  @Override
  public void setValue( ValueVector v, int index ) {
    VarCharVector vector = (VarCharVector) v;
    long randTime = baseTime + value( );
    String str = fmt.format(new Date(randTime));
    vector.getMutator().setSafe(index, str.getBytes());
  }
}
