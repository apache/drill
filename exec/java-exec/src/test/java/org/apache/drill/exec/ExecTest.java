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
package org.apache.drill.exec;

import com.codahale.metrics.MetricRegistry;
import mockit.Mock;
import mockit.MockUp;
import mockit.NonStrictExpectations;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.exec.compile.CodeCompilerTestFactory;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReaderTestFactory;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.sys.store.provider.LocalPersistentStoreProvider;
import org.apache.drill.exec.util.GuavaPatcher;
import org.apache.drill.test.DrillTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.BeforeClass;
import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.Locale;


public class ExecTest extends DrillTest {

  protected static SystemOptionManager optionManager;
  static {
    GuavaPatcher.patch();
  }

  protected static final DrillConfig c = DrillConfig.create();

  @After
  public void clear(){
    // TODO:  (Re DRILL-1735) Check whether still needed now that
    // BootstrapContext.close() resets the metrics.
    DrillMetrics.resetMetrics();
  }

  @BeforeClass
  public static void setupOptionManager() throws Exception{
    final LocalPersistentStoreProvider provider = new LocalPersistentStoreProvider(c);
    provider.start();
    optionManager = new SystemOptionManager(PhysicalPlanReaderTestFactory.defaultLogicalPlanPersistence(c), provider,c);
    optionManager.init();
  }

  /**
   * Creates instance of local file system.
   *
   * @return local file system
   */
  public static FileSystem getLocalFileSystem() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    return FileSystem.get(configuration);
  }

  protected void mockDrillbitContext(final DrillbitContext bitContext) throws Exception {
    new NonStrictExpectations() {{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = RootAllocatorFactory.newRoot(c);
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(ClassPathScanner.fromPrescan(c));
      bitContext.getConfig(); result = c;
      bitContext.getOptionManager(); result = optionManager;
      bitContext.getCompiler(); result = CodeCompilerTestFactory.getTestCompiler(c);
    }};
  }

  protected LogicalExpression parseExpr(String expr) throws RecognitionException {
    final ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final ExprParser parser = new ExprParser(tokens);
    final ExprParser.parse_return ret = parser.parse();
    return ret.e;
  }

  /**
   * This utility is to mock the method DateTimeUtils.getDateFormatSymbols()
   * to mock the current local as US.
   */
  public static void mockUsDateFormatSymbols() {
    new MockUp<DateTimeUtils>() {
      @Mock
      public DateFormatSymbols getDateFormatSymbols(Locale locale) {
        return new DateFormatSymbols(Locale.US);
      }
    };
  }

  /**
   * This utility is to mock the method DateTimeZone.getDefault() to
   * mock current timezone as UTC.
   */
  public static void mockUtcDateTimeZone() {
    new MockUp<DateTimeZone>() {
      @Mock
      public DateTimeZone getDefault() {
        return DateTimeZone.UTC;
      }
    };
  }
}
