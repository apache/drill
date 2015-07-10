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
 * Runs several concurrent queries, each has thread to cancel it, after varying
 * delays.
 */
public class TestTempAsyncCancelation1 extends JdbcTestBase {

  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule( 900_000 );


  private static Connection connection;

  @BeforeClass
  public static void setUpBeforeClass() throws SQLException {
    connection = new Driver().connect( "jdbc:drill:zk=local", null );
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    connection.close();
  }

  @Test
  public void testNameThis() throws SQLException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool( 20 ); // cached?

    final LinkedTransferQueue<Statement> candidateQueueHack = new LinkedTransferQueue<Statement>();

    class QueryTask implements Runnable {
      private final int taskId;

      QueryTask( final int taskId ) {
        this.taskId = taskId;
      }

      @Override
      public void run() {
        Statement statement;
        try {
          statement = connection.createStatement();
          candidateQueueHack.add( statement );

          System.err.println( Thread.currentThread().getName()
                              + ": Q#" + taskId + ": executing ..." );
          ResultSet rs =
              statement.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.CATALOGS" );
          System.err.println( Thread.currentThread().getName()
                              + ": Q#" + taskId + " ... executed, receiving ..." );
          rs.next();
          while ( rs.next() ) {
            Thread.sleep( 100 /* ms */ );
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
          Statement statement = candidateQueueHack.take();
          System.err.println( Thread.currentThread().getName()
                              + ": C#" + taskId + ": waiting ..." );
          Thread.sleep( delayUnits * 1000 /* ms*/ );
          System.err.println( Thread.currentThread().getName()
                              + ": C#" + taskId + ": ... waited, canceling" );
          statement.cancel();

        } catch (InterruptedException | SQLException e) {
          throw new RuntimeException( "Q#" + taskId + ":" + e, e );
        }
      }
    }

    for ( int i = 1; i <= 10; i++ ) {
      executor.execute( new QueryTask( i ) );
      executor.execute( new CancelTask( i, i ) );
    }

    System.err.println( Thread.currentThread().getName()
                        + ": awaiting executor shutdown ..." );
    executor.shutdown();
    boolean result = executor.awaitTermination( 1, TimeUnit.MINUTES );
    System.err.println( Thread.currentThread().getName()
                        + ": ... awaited executor shutdown; result = " + result );
  }

}
