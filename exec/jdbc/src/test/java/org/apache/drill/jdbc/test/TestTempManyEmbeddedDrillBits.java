/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.drill.jdbc.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;


/**
 * Creates embedded Drillbits until out of memory (not really a clean or useful test).
 */
public class TestTempManyEmbeddedDrillBits extends JdbcTestBase {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule( 900_000 );


  @Ignore( "???" )
  @Test
  public void testCreateStatementsUntilOutOfHeap() throws SQLException, InterruptedException {
    System.err.println( "toSqueeze ..." );
    byte[] toSqueeze = new byte[1_000_000_000];
    for ( int bx = 0; bx < toSqueeze.length; bx++ ) {
      toSqueeze[ bx ] = (byte) bx;
    }
    System.err.println( "... toSqueeze " );

    @SuppressWarnings("unused")
    byte[] reservedForCleanup = new byte[1_000_000];


    int count = 0;
    try {
      while ( true ) {
        count++;
        System.err.println( "count := " + count + " (" + new Date() + ")" );
        @SuppressWarnings("unused")
        final Connection connection =
            new Driver().connect( "jdbc:drill:zk=local", null );
      }
    }
    catch ( OutOfMemoryError e ) {
      // Free enough space to print?
      reservedForCleanup = null;
      //System.gc();

      System.err.println( "end count = " + count );
    }

    // Some reference, just in case compiler would optimize setting pointer to null
    // before here.
    toSqueeze[ 1 ] = toSqueeze[ 10 ];
  }

}
