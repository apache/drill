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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.ExecutionCanceledSqlException;
import org.apache.drill.jdbc.JdbcTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * ...; runs one big (LOCAL FILE), has multiple threads cancel it
 *
 */
public class TestTempAsyncCancelation3Debug extends JdbcTestBase {

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
      System.err.println( "Closing JDBC connection threw " + e + " :");
      e.printStackTrace( System.err );
      throw e;
    }
  }

  @Test
  public void testNameThis() throws SQLException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool( 20 ); // cached?

    final CountDownLatch startedToCancelLatch = new CountDownLatch( 1 );
    final CountDownLatch tasksToExitLatch = new CountDownLatch( 2 );


    class QueryTask implements Runnable {

      @Override
      public void run() {
        int rowNum = 0;
        try {
          System.err.println( Thread.currentThread().getName()
                              + ": QueryTask: executing ..." );
          ResultSet rs =
              statement.executeQuery(
                  "SELECT * FROM `dfs.root`.`/tmp/1000000_lines_of_99_x_characters.csv` LIMIT 50000"  //???? 1_000_000
                  );
          System.err.println( Thread.currentThread().getName()
                              + ": QueryTask: ... executed, sleeping 10 s, doing first next()..." );
          Thread.sleep( 10_000 );
          rowNum = 0;
          rs.next();
          rowNum++;
          System.err.println( Thread.currentThread().getName()
                              + ": QueryTask: ... row " + rowNum + " ..." );
          System.err.println( Thread.currentThread().getName()
                              + ": QueryTask: ... did first next(), releasing via latch, xxxdoing more next() ..." );
          startedToCancelLatch.countDown(); // release cancelers
          /*????
          while ( rs.next() ) {
            rowNum++;
            if ( 1 == Integer.bitCount( rowNum ) ) {
              System.err.println( Thread.currentThread().getName()
                                  + ": QueryTask: ... row " + rowNum + " ..." );
            }
            if ( true ) { Thread.sleep( 1 /* ms * / ); }
          }
          System.err.println( Thread.currentThread().getName()
                              + ": Q#" + taskId + " ... received" );
          */
          System.err.println( Thread.currentThread().getName()
                              + ": QueryTask: sleeping 15 before closing statement" );
          Thread.sleep( 15_000 );
          statement.close();

        } catch ( final ExecutionCanceledSqlException e ) {
          System.err.println( Thread.currentThread().getName()
                              + ": QueryTask: ... receiving interrupted" );
        } catch ( InterruptedException | SQLException e) {
          throw new RuntimeException( "QueryTask:" + e, e );
        }
        finally {
          System.err.println( "final rowNum = " + rowNum);
          tasksToExitLatch.countDown();
        }
      }

    }

    class CancelTask implements Runnable {

      @Override
      public void run() {
        try {
          System.err.println( Thread.currentThread().getName()
              + ": CancelTask: awaiting latch ..." );
          startedToCancelLatch.await();

          final int delayMs = 5_000;
          System.err.println( Thread.currentThread().getName()
              + ": CancelTask: awaited latch, delaying " + delayMs + " ms ..." );
          Thread.sleep( delayMs );
          System.err.println( Thread.currentThread().getName()
                              + ": CancelTask: ... delayed, canceling" );
          statement.cancel();
          Thread.yield();
          statement.cancel();
          Thread.yield();
          statement.cancel();
          Thread.yield();

        } catch (InterruptedException | SQLException e) {
          throw new RuntimeException( "CancelTask:" + e, e );
        }
        finally {
          tasksToExitLatch.countDown();
        }
      }
    }

    executor.execute( new QueryTask() );
    executor.execute( new CancelTask() );

    System.err.println( Thread.currentThread().getName()
        + ": awaiting task query and cancel task completion ..." );
    tasksToExitLatch.await();
    System.err.println( Thread.currentThread().getName()
                        + ": XXXawaiting executor shutdown ..." );
    executor.shutdown();
    boolean result = executor.awaitTermination( 2, TimeUnit.MINUTES );
    System.err.println( Thread.currentThread().getName()
                        + ": ... XXXawaited executor shutdown; terminated = " + result );
    List<Runnable> stillRunning = executor.shutdownNow();
    System.err.println( "stillRunning.size() = " + stillRunning.size() );
  }

}
