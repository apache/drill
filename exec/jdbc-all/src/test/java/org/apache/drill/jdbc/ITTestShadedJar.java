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
package org.apache.drill.jdbc;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITTestShadedJar {

  private static DrillbitClassLoader drillbitLoader;
  private static URLClassLoader rootClassLoader;

  private static URL getJdbcUrl() throws MalformedURLException {
    return new URL(
        String.format("%s../../target/drill-jdbc-all-%s.jar",
            ClassLoader.getSystemClassLoader().getResource("").toString(),
            System.getProperty("project.version")
            ));

  }

  @Test
  public void executeJdbcAllQuery() throws Exception {

    // print class path for debugging
    System.out.println("java.class.path:");
    System.out.println(System.getProperty("java.class.path"));

    final URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    method.setAccessible(true);
    method.invoke(loader, getJdbcUrl());

    Class<?> clazz = loader.loadClass("org.apache.drill.jdbc.Driver");
    try {
      Driver driver = (Driver) clazz.newInstance();
      try (Connection c = driver.connect("jdbc:drill:drillbit=localhost:31010", null)) {
        String path = Paths.get("").toAbsolutePath().toString() + "/src/test/resources/types.json";
        printQuery(c, "select * from dfs.`" + path + "`");
      }
    } catch (Exception ex) {
      throw ex;
    }

  }

  private static void printQuery(Connection c, String query) throws SQLException {
    try (Statement s = c.createStatement(); ResultSet result = s.executeQuery(query)) {
      while (result.next()) {
        final int columnCount = result.getMetaData().getColumnCount();
        for(int i = 1; i < columnCount+1; i++){
          System.out.print(result.getObject(i));
          System.out.print('\t');
        }
        System.out.println(result.getObject(1));
      }
    }
  }



  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    drillbitLoader = new DrillbitClassLoader();
    rootClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    try {
    runWithLoader("DrillbitStartThread", drillbitLoader);
    } catch (Exception e) {
      printClassesLoaded("root", rootClassLoader);
      throw e;
    }
  }

  @AfterClass
  public static void closeClient() throws Exception {
    runWithLoader("DrillbitStopThread", drillbitLoader);
  }

  private static int getClassesLoadedCount(ClassLoader classLoader) {
    try {
      Field f = ClassLoader.class.getDeclaredField("classes");
      f.setAccessible(true);
      Vector<Class<?>> classes = (Vector<Class<?>>) f.get(classLoader);
      return classes.size();
    } catch (Exception e) {
      System.out.println("Failure while loading class count.");
      return -1;
    }
  }

  private static void printClassesLoaded(String prefix, ClassLoader classLoader) {
    try {
      Field f = ClassLoader.class.getDeclaredField("classes");
      f.setAccessible(true);
      Vector<Class<?>> classes = (Vector<Class<?>>) f.get(classLoader);
      for (Class<?> c : classes) {
        System.out.println(prefix + ": " + c.getName());
      }
    } catch (Exception e) {
      System.out.println("Failure while printing loaded classes.");
    }
  }

  private static void runWithLoader(String name, ClassLoader loader) throws Exception {
    Class<?> clazz = loader.loadClass(ITTestShadedJar.class.getName() + "$" + name);
    Object o = clazz.getDeclaredConstructors()[0].newInstance(loader);
    clazz.getMethod("go").invoke(o);
  }

  public abstract static class AbstractLoaderThread extends Thread {
    private Exception ex;
    protected final ClassLoader loader;

    public AbstractLoaderThread(ClassLoader loader) {
      this.setContextClassLoader(loader);
      this.loader = loader;
    }

    @Override
    public final void run() {
      try {
        internalRun();
      } catch (Exception e) {
        this.ex = e;
      }
    }

    protected abstract void internalRun() throws Exception;

    public void go() throws Exception {
      start();
      join();
      if (ex != null) {
        throw ex;
      }
    }
  }

  public static class DrillbitStartThread extends AbstractLoaderThread {

    public DrillbitStartThread(ClassLoader loader) {
      super(loader);
    }

    @Override
    protected void internalRun() throws Exception {
      Class<?> clazz = loader.loadClass("org.apache.drill.BaseTestQuery");
      clazz.getMethod("setupDefaultTestCluster").invoke(null);

      // loader.loadClass("org.apache.drill.exec.exception.SchemaChangeException");

      // execute a single query to make sure the drillbit is fully up
      clazz.getMethod("testNoResult", String.class, new Object[] {}.getClass())
          .invoke(null, "select * from (VALUES 1)", new Object[] {});

    }

  }

  public static class DrillbitStopThread extends AbstractLoaderThread {

    public DrillbitStopThread(ClassLoader loader) {
      super(loader);
    }

    @Override
    protected void internalRun() throws Exception {
      Class<?> clazz = loader.loadClass("org.apache.drill.BaseTestQuery");
      clazz.getMethod("setupDefaultTestCluster").invoke(null);
    }
  }

}
