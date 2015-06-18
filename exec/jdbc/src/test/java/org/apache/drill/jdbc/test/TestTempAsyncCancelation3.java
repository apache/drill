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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.ExecutionCanceledSqlException;
import org.apache.drill.jdbc.JdbcTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * ...; runs one big (LOCAL FILE), has multiple threads cancel it
 *
 */
public class TestTempAsyncCancelation3 extends JdbcTestBase {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule( 900_000 );


  private static Connection connection;
  private static Statement statement;

  @BeforeClass
  public static void setUpBeforeClass() throws SQLException {
    connection = new Driver().connect( "jdbc:drill:zk=local", null );
    statement = connection.createStatement();
  }

  @AfterClass
  public static void tearDownAfterClass() throws SQLException {
    System.err.println( "Closing JDBC connection ..." );
    try {
      connection.close();
      System.err.println( "Closing JDBC connection completed." );
    } catch ( SQLException | RuntimeException e) {
      System.err.println( "Closing JDBC connection threw " + e );
      throw e;
    }
  }

  @Test
  public void testNameThis() throws SQLException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool( 20 ); // cached?

    final CountDownLatch syncLatch = new CountDownLatch( 1 );


    class QueryTask implements Runnable {
      private final int taskId;

      QueryTask( final int taskId ) {
        this.taskId = taskId;
      }

      @Override
      public void run() {
        int rowNum = 0;
        try {
          System.err.println( Thread.currentThread().getName()
                              + ": Q#" + taskId + ": executing ..." );
          ResultSet rs =
              statement.executeQuery(
                  "SELECT * FROM `dfs.root`.`/tmp/1000000_lines_of_99_x_characters.csv` LIMIT 1000000"
                  );
          System.err.println( Thread.currentThread().getName()
                              + ": Q#" + taskId + " ... executed, releasing latch, receiving ..." );
          rowNum = 0;
          rs.next();
          rowNum++;
          System.err.println( "... row " + rowNum + " ..." );
          syncLatch.countDown(); // release cancelers
          while ( rs.next() ) {
            rowNum++;
            if ( 1 == Integer.bitCount( rowNum ) ) {
              System.err.println( "... row " + rowNum + " ..." );
            }
            if ( true ) { Thread.sleep( 1 /* ms */ ); }
          }
          statement.close();
          System.err.println( Thread.currentThread().getName()
                              + ": Q#" + taskId + " ... received" );

        } catch ( final ExecutionCanceledSqlException e ) {
          System.err.println( Thread.currentThread().getName()
                              + ": Q#" + taskId + " ... receiving interrupted" );
        } catch ( InterruptedException | SQLException e) {
          throw new RuntimeException( "Q#" + taskId + ":" + e, e );
        }
        finally {
          System.err.println( "final rowNum = " + rowNum);
        }
      }

    }

    class CancelTask implements Runnable {
      private final int taskId;
      private final int delayUnits;

      CancelTask( final int taskId , final int delayUnits ) {
        this.taskId = taskId;
        this.delayUnits = delayUnits;
      }

      @Override
      public void run() {
        try {
          System.err.println( Thread.currentThread().getName()
              + ": C#" + taskId + ": awaiting latch ..." );
          syncLatch.await();

          final int delayMs = 60_000 + 60_000 + delayUnits * 0;
          System.err.println( Thread.currentThread().getName()
              + ": C#" + taskId + ": awaited latch, delaying " + delayMs + " ms ..." );
          Thread.sleep( delayMs );
          System.err.println( Thread.currentThread().getName()
                              + ": C#" + taskId + ": ... delayed, canceling" );
          statement.cancel();
          Thread.yield();
          statement.cancel();
          Thread.yield();
          statement.cancel();
          Thread.yield();

        } catch (InterruptedException | SQLException e) {
          throw new RuntimeException( "Q#" + taskId + ":" + e, e );
        }
      }
    }

    executor.execute( new QueryTask( 1 ) );
    for ( int i = 1; i <= 10; i++ ) {
      executor.execute( new CancelTask( i, i ) );
    }

    System.err.println( Thread.currentThread().getName()
                        + ": awaiting executor shutdown ..." );
    executor.shutdown();
    boolean result = executor.awaitTermination( 5, TimeUnit.MINUTES );
    System.err.println( Thread.currentThread().getName()
                        + ": ... awaited executor shutdown; terminated = " + result );
    List<Runnable> stillRunning = executor.shutdownNow();
    System.err.println( "stillRunning.size() = " + stillRunning.size() );
  }

}
