/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.util.Text;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class TestBuilder {

  // test query to run
  private String query;
  // the type of query for the test
  private UserBitShared.QueryType queryType;
  // should the validation enforce ordering
  private Boolean ordered;
  private boolean approximateEquality;
  private BufferAllocator allocator;
  // Used to pass the type information associated with particular column names rather than relying on the
  // ordering of the columns in the CSV file, or the default type inferences when reading JSON, this is used for the
  // case where results of the test query are adding type casts to the baseline queries, this saves a little bit of
  // setup in cases where strict type enforcement is not necessary for a given test
  protected Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap;
  // queries to run before the baseline or test queries, can be used to set options
  private String baselineOptionSettingQueries;
  private String testOptionSettingQueries;
  // two different methods are available for comparing ordered results, the default reads all of the records
  // into giant lists of objects, like one giant on-heap batch of 'vectors'
  // this flag enables the other approach which iterates through a hyper batch for the test query results and baseline
  // while this does work faster and use less memory, it can be harder to debug as all of the elements are not in a
  // single list
  private boolean highPerformanceComparison;
  // column names for use with the baseline values
  protected String[] baselineColumns;
  // In cases where we need to verify larger datasets without the risk of running the baseline data through
  // the drill engine, results can be provided in a list of maps. While this model does make a lot of sense, there is a
  // lot of work to make the type handling/casting work correctly, and making robust complex type handling work completely outside
  // of the drill engine for generating baselines would likely be more work than it would be worth. For now we will be
  // going with an approach of using this facility to validate the parts of the drill engine that could break in ways
  // that would affect the reading of baseline files (i.e. we need robust test for storage engines, project and casting that
  // use this interface) and then rely on the engine for the rest of the tests that will use the baseline queries.
  private List<Map<String, Object>> baselineRecords;

  private int expectedNumBatches = DrillTestWrapper.EXPECTED_BATCH_COUNT_NOT_SET;
  private Map<String,Float> baselineTolerances;

  public TestBuilder(BufferAllocator allocator) {
    this.allocator = allocator;
    reset();
  }

  public TestBuilder(BufferAllocator allocator, String query, UserBitShared.QueryType queryType, Boolean ordered,
                     boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap,
                     String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison,
                     int expectedNumBatches, Map<String,Float> baselineTolerances) {
    this(allocator);
    if (ordered == null) {
      throw new RuntimeException("Ordering not set, when using a baseline file or query you must explicitly call the ordered() or unOrdered() method on the " + this.getClass().getSimpleName());
    }
    this.query = query;
    this.queryType = queryType;
    this.ordered = ordered;
    this.approximateEquality = approximateEquality;
    this.baselineTypeMap = baselineTypeMap;
    this.baselineOptionSettingQueries = baselineOptionSettingQueries;
    this.testOptionSettingQueries = testOptionSettingQueries;
    this.highPerformanceComparison = highPerformanceComparison;
    this.expectedNumBatches = expectedNumBatches;
    this.baselineTolerances = baselineTolerances;
  }

  protected TestBuilder reset() {
    query = "";
    ordered = null;
    approximateEquality = false;
    highPerformanceComparison = false;
    testOptionSettingQueries = "";
    baselineOptionSettingQueries = "";
    baselineRecords = null;
    return this;
  }

  public DrillTestWrapper build() throws Exception {
    if ( ! ordered && highPerformanceComparison ) {
      throw new Exception("High performance comparison only available for ordered checks, to enforce this restriction, ordered() must be called first.");
    }
    return new DrillTestWrapper(this, allocator, query, queryType, baselineOptionSettingQueries, testOptionSettingQueries,
        getValidationQueryType(), ordered, highPerformanceComparison, baselineRecords, expectedNumBatches, baselineTolerances);
  }

  public List<Pair<SchemaPath, TypeProtos.MajorType>> getExpectedSchema() {
    return null;
  }

  public void go() throws Exception {
    build().run();
  }

  public TestBuilder sqlQuery(String query) {
    this.query = QueryTestUtil.normalizeQuery(query);
    this.queryType = UserBitShared.QueryType.SQL;
    return this;
  }

  public TestBuilder sqlQuery(String query, Object... replacements) {
    return sqlQuery(String.format(query, replacements));
  }

  public TestBuilder sqlQueryFromFile(String queryFile) throws IOException {
    String query = BaseTestQuery.getFile(queryFile);
    this.query = query;
    this.queryType = UserBitShared.QueryType.SQL;
    return this;
  }

  public TestBuilder physicalPlanFromFile(String queryFile) throws IOException {
    String query = BaseTestQuery.getFile(queryFile);
    this.query = query;
    this.queryType = UserBitShared.QueryType.PHYSICAL;
    return this;
  }

  public TestBuilder ordered() {
    this.ordered = true;
    return this;
  }

  public TestBuilder unOrdered() {
    this.ordered = false;
    return this;
  }

  // this can only be used with ordered verifications, it does run faster and use less memory but may be
  // a little harder to debug as it iterates over a hyper batch rather than reading all of the values into
  // large on-heap lists
  public TestBuilder highPerformanceComparison() throws Exception {
    this.highPerformanceComparison = true;
    return this;
  }

  // list of queries to run before the baseline query, can be used to set several options
  // list takes the form of a semi-colon separated list
  public TestBuilder optionSettingQueriesForBaseline(String queries) {
    this.baselineOptionSettingQueries = queries;
    return this;
  }

  // list of queries to run before the test query, can be used to set several options
  // list takes the form of a semi-colon separated list
  public TestBuilder optionSettingQueriesForTestQuery(String queries) {
    this.testOptionSettingQueries = queries;
    return this;
  }
  public TestBuilder approximateEquality() {
    this.approximateEquality = true;
    return this;
  }

  // modified code from SchemaPath.De class. This should be used sparingly and only in tests if absolutely needed.
  public static SchemaPath parsePath(String path) {
    try {
      // logger.debug("Parsing expression string '{}'", expr);
      ExprLexer lexer = new ExprLexer(new ANTLRStringStream(path));
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      ExprParser parser = new ExprParser(tokens);

      //TODO: move functionregistry and error collector to injectables.
      //ctxt.findInjectableValue(valueId, forProperty, beanInstance)
      ExprParser.parse_return ret = parser.parse();

      // ret.e.resolveAndValidate(expr, errorCollector);
      if (ret.e instanceof SchemaPath) {
        return (SchemaPath) ret.e;
      } else {
        throw new IllegalStateException("Schema path is not a valid format.");
      }
    } catch (RecognitionException e) {
      throw new RuntimeException(e);
    }
  }

  String getValidationQuery() throws Exception {
    throw new RuntimeException("Must provide some kind of baseline, either a baseline file or another query");
  }

  protected UserBitShared.QueryType getValidationQueryType() throws Exception {
    if (singleExplicitBaselineRecord()) {
      return null;
    }
    throw new RuntimeException("Must provide some kind of baseline, either a baseline file or another query");
  }

  public JSONTestBuilder jsonBaselineFile(String filePath) {
    return new JSONTestBuilder(filePath, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison,
        expectedNumBatches, baselineTolerances);
  }

  public CSVTestBuilder csvBaselineFile(String filePath) {
    return new CSVTestBuilder(filePath, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison,
        expectedNumBatches, baselineTolerances);
  }

  public SchemaTestBuilder schemaBaseLine(List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema) {
    assert expectedSchema != null : "The expected schema can be provided once";
    assert baselineColumns == null : "The column information should be captured in expected schema, not baselineColumns";

    return new SchemaTestBuilder(
        allocator,
        query,
        queryType,
        baselineOptionSettingQueries,
        testOptionSettingQueries,
        expectedSchema);
  }

  public TestBuilder baselineTypes(Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap) {
    this.baselineTypeMap = baselineTypeMap;
    return this;
  }

  boolean typeInfoSet() {
    if (baselineTypeMap != null) {
      return true;
    } else {
      return false;
    }
  }

  // indicate that the tests query should be checked for an empty result set
  public TestBuilder expectsEmptyResultSet() {
    unOrdered();
    baselineRecords = new ArrayList<>();
    return this;
  }

  /**
   * Sets the number of expected batch count for this query. The test will fail if the query returns a different number
   * of batches
   *
   * @param expectedNumBatches expected batch count
   * @return this test builder
   */
  public TestBuilder expectsNumBatches(int expectedNumBatches) {
    this.expectedNumBatches = expectedNumBatches;
    return this;
  }
  /**
   * This method is used to pass in a simple list of values for a single record verification without
   * the need to create a CSV or JSON file to store the baseline.
   *
   * This can be called repeatedly to pass a list of records to verify. It works for both ordered and unordered
   * checks.
   *
   * @param baselineValues - the baseline values to validate
   * @return
   */
  public TestBuilder baselineValues(Object ... baselineValues) {
    assert getExpectedSchema() == null : "The expected schema is not needed when baselineValues are provided ";
    if (ordered == null) {
      throw new RuntimeException("Ordering not set, before specifying baseline data you must explicitly call the ordered() or unOrdered() method on the " + this.getClass().getSimpleName());
    }
    if (baselineRecords == null) {
      baselineRecords = new ArrayList<>();
    }
    Map<String, Object> ret = new HashMap<>();
    int i = 0;
    assertEquals("Must supply the same number of baseline values as columns.", baselineValues.length, baselineColumns.length);
    for (String s : baselineColumns) {
      ret.put(s, baselineValues[i]);
      i++;
    }
    this.baselineRecords.add(ret);
    return this;
  }

  /**
   * This can be used in cases where we want to avoid issues with the assumptions made by the test framework.
   * Most of the methods for verification in the framework run drill queries to generate the read baseline files or
   * execute alternative baseline queries. This model relies on basic functionality of reading files with storage
   * plugins and applying casts/projects to be stable.
   *
   * This method can be used to verify the engine for these cases and any other future execution paths that would
   * be used by both the test query and baseline. Without tests like this it is possible that some tests
   * could falsely report as passing, as both the test query and baseline query could run into the same problem
   * with an assumed stable code path and produce the same erroneous result.
   *
   * @param materializedRecords - a list of maps representing materialized results
   * @return
   */
  public TestBuilder baselineRecords(List<Map<String, Object>> materializedRecords) {
    this.baselineRecords = materializedRecords;
    return this;
  }

  /**
   * This setting has a slightly different impact on the test depending on some of the other
   * configuration options are set.
   *
   * If a JSON baseline file is given, this list will act as a project list to verify the
   * test query against a subset of the columns in the file.
   *
   * For a CSV baseline file, these will act as aliases for columns [0 .. n] in the repeated
   * varchar column that is read out of CSV.
   *
   * For a baseline sql query, this currently has no effect.
   *
   * For explicit baseline values given in java code with the baselineValues() method, these will
   * be used to create a map for the one record verification.
   */
  public TestBuilder baselineColumns(String... columns) {
    assert getExpectedSchema() == null : "The expected schema is not needed when baselineColumns are provided ";
    for (int i = 0; i < columns.length; i++) {
      columns[i] = parsePath(columns[i]).toExpr();
    }
    this.baselineColumns = columns;
    return this;
  }

  public TestBuilder baselineTolerances(Map<String,Float> baselineTolerances) {
    this.baselineTolerances = baselineTolerances;
    return this;
  }

  private boolean singleExplicitBaselineRecord() {
    return baselineRecords != null;
  }

  // provide a SQL query to validate against
  public BaselineQueryTestBuilder sqlBaselineQuery(String baselineQuery) {
    return new BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.SQL, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison, expectedNumBatches, baselineTolerances);
  }

  public BaselineQueryTestBuilder sqlBaselineQuery(String query, String ...replacements) {
    return sqlBaselineQuery(String.format(query, replacements));
  }

  // provide a path to a file containing a SQL query to use as a baseline
  public BaselineQueryTestBuilder sqlBaselineQueryFromFile(String baselineQueryFilename) throws IOException {
    String baselineQuery = BaseTestQuery.getFile(baselineQueryFilename);
    return new BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.SQL, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison, expectedNumBatches, baselineTolerances);
  }

  // as physical plans are verbose, this is the only option provided for specifying them, we should enforce
  // that physical plans, or any large JSON strings do not live in the Java source as literals
  public BaselineQueryTestBuilder physicalPlanBaselineQueryFromFile(String baselinePhysicalPlanPath) throws IOException {
    String baselineQuery = BaseTestQuery.getFile(baselinePhysicalPlanPath);
    return new BaselineQueryTestBuilder(baselineQuery, UserBitShared.QueryType.PHYSICAL, allocator, query, queryType, ordered, approximateEquality,
        baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries, highPerformanceComparison, expectedNumBatches, baselineTolerances);
  }

  private String getDecimalPrecisionScaleInfo(TypeProtos.MajorType type) {
    String precision = "";
    switch(type.getMinorType()) {
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL38SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL28DENSE:
      case DECIMAL9:
        precision = String.format("(%d,%d)", type.getPrecision(), type.getScale());
        break;
      default:
        ; // do nothing empty string set above
    }
    return precision;
  }

  public class CSVTestBuilder extends TestBuilder {

    // path to the baseline file that will be inserted into the validation query
    private String baselineFilePath;
    // use to cast the baseline file columns, if not set the types
    // that come out of the test query drive interpretation of baseline
    private TypeProtos.MajorType[] baselineTypes;

    CSVTestBuilder(String baselineFile, BufferAllocator allocator, String query, UserBitShared.QueryType queryType, Boolean ordered,
                   boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap,
                   String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison,
                   int expectedNumBatches, Map<String,Float> baselineTolerances) {
      super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries,
          highPerformanceComparison, expectedNumBatches, baselineTolerances);
      this.baselineFilePath = baselineFile;
    }

    public CSVTestBuilder baselineTypes(TypeProtos.MajorType... baselineTypes) {
      this.baselineTypes = baselineTypes;
      this.baselineTypeMap = null;
      return this;
    }

    // convenience method to convert minor types to major types if no decimals with precisions are needed
    public CSVTestBuilder baselineTypes(TypeProtos.MinorType ... baselineTypes) {
      TypeProtos.MajorType[] majorTypes = new TypeProtos.MajorType[baselineTypes.length];
      int i = 0;
      for(TypeProtos.MinorType minorType : baselineTypes) {
        majorTypes[i] = Types.required(minorType);
        i++;
      }
      this.baselineTypes = majorTypes;
      this.baselineTypeMap = null;
      return this;
    }

    @Override
    protected TestBuilder reset() {
      super.reset();
      baselineTypeMap = null;
      baselineTypes = null;
      baselineFilePath = null;
      return this;
    }

    @Override
    boolean typeInfoSet() {
      if (super.typeInfoSet() || baselineTypes != null) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    String getValidationQuery() throws Exception {
      if (baselineColumns.length == 0) {
        throw new Exception("Baseline CSV files require passing column names, please call the baselineColumns() method on the test builder.");
      }

      if (baselineTypes != null) {
        assertEquals("Must pass the same number of types as column names if types are provided.", baselineTypes.length, baselineColumns.length);
      }

      String[] aliasedExpectedColumns = new String[baselineColumns.length];
      for (int i = 0; i < baselineColumns.length; i++) {
        aliasedExpectedColumns[i] = "columns[" + i + "] ";
        TypeProtos.MajorType majorType;
        if (baselineTypes != null) {
          majorType = baselineTypes[i];
        } else if (baselineTypeMap != null) {
          majorType = baselineTypeMap.get(parsePath(baselineColumns[i]));
        } else {
          throw new Exception("Type information not set for interpreting csv baseline file.");
        }
        String precision = getDecimalPrecisionScaleInfo(majorType);
        // TODO - determine if there is a better behavior here, if we do not specify a length the default behavior is
        // to cast to varchar with length 1
        // set default cast size for varchar, the cast function will take the lesser of this passed value and the
        // length of the incoming data when choosing the length for the outgoing data
        if (majorType.getMinorType() == TypeProtos.MinorType.VARCHAR ||
            majorType.getMinorType() == TypeProtos.MinorType.VARBINARY) {
          precision = "(65000)";
        }
        aliasedExpectedColumns[i] = "cast(" + aliasedExpectedColumns[i] + " as " +
            Types.getNameOfMinorType(majorType.getMinorType()) + precision +  " ) " + baselineColumns[i];
      }
      String query = "select " + Joiner.on(", ").join(aliasedExpectedColumns) + " from cp.`" + baselineFilePath + "`";
      return query;
    }

    @Override
    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
      return UserBitShared.QueryType.SQL;
    }
  }

  public class SchemaTestBuilder extends TestBuilder {
    private List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema;
    SchemaTestBuilder(BufferAllocator allocator, String query, UserBitShared.QueryType queryType,
        String baselineOptionSettingQueries, String testOptionSettingQueries, List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema) {
      super(allocator, query, queryType, false, false, null, baselineOptionSettingQueries, testOptionSettingQueries, false, -1, null);
      expectsEmptyResultSet();
      this.expectedSchema = expectedSchema;
    }

    @Override
    public TestBuilder baselineColumns(String... columns) {
      assert false : "The column information should be captured in expected scheme, not baselineColumns";
      return this;
    }

    @Override
    public TestBuilder baselineRecords(List<Map<String, Object>> materializedRecords) {
      assert false : "Since only schema will be compared in this test, no record is expected";
      return this;
    }

    @Override
    public TestBuilder baselineValues(Object... objects) {
      assert false : "Since only schema will be compared in this test, no record is expected";
      return this;
    }

    @Override
    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
      return null;
    }

    @Override
    public List<Pair<SchemaPath, TypeProtos.MajorType>> getExpectedSchema() {
      return expectedSchema;
    }
  }

  public class JSONTestBuilder extends TestBuilder {

    // path to the baseline file that will be inserted into the validation query
    private String baselineFilePath;

    JSONTestBuilder(String baselineFile, BufferAllocator allocator, String query, UserBitShared.QueryType queryType, Boolean ordered,
                    boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap,
                    String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison,
                    int expectedNumBatches, Map<String,Float> baselineTolerances) {
      super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries,
          highPerformanceComparison, expectedNumBatches, baselineTolerances);
      this.baselineFilePath = baselineFile;
      this.baselineColumns = new String[] {"*"};
    }

    @Override
    String getValidationQuery() {
      return "select " + Joiner.on(", ").join(baselineColumns) + " from cp.`" + baselineFilePath + "`";
    }

    @Override
    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
      return UserBitShared.QueryType.SQL;
    }

  }

  public class BaselineQueryTestBuilder extends TestBuilder {

    private String baselineQuery;
    private UserBitShared.QueryType baselineQueryType;

    BaselineQueryTestBuilder(String baselineQuery, UserBitShared.QueryType baselineQueryType, BufferAllocator allocator,
                             String query, UserBitShared.QueryType queryType, Boolean ordered,
                             boolean approximateEquality, Map<SchemaPath, TypeProtos.MajorType> baselineTypeMap,
                             String baselineOptionSettingQueries, String testOptionSettingQueries, boolean highPerformanceComparison,
                             int expectedNumBatches, Map<String,Float> baselineTolerances) {
      super(allocator, query, queryType, ordered, approximateEquality, baselineTypeMap, baselineOptionSettingQueries, testOptionSettingQueries,
          highPerformanceComparison, expectedNumBatches, baselineTolerances);
      this.baselineQuery = baselineQuery;
      this.baselineQueryType = baselineQueryType;
    }

    @Override
    String getValidationQuery() {
      return baselineQuery;
    }

    @Override
    protected UserBitShared.QueryType getValidationQueryType() throws Exception {
      return baselineQueryType;
    }

    // This currently assumes that all explicit baseline queries will have fully qualified type information
    // if this changes, the baseline query can be run in a sub query with the implicit or explicit type passing
    // added on top of it, as is currently when done when reading a baseline file
    @Override
    boolean typeInfoSet() {
      return true;
    }

  }

  /**
   * Convenience method to create a {@link JsonStringArrayList list} from the given values.
   */
  public static JsonStringArrayList<Object> listOf(Object... values) {
    final JsonStringArrayList<Object> list = new JsonStringArrayList<>();
    for (Object value:values) {
      if (value instanceof CharSequence) {
        list.add(new Text(value.toString()));
      } else {
        list.add(value);
      }
    }
    return list;
  }

  /**
   * Convenience method to create a {@link JsonStringHashMap<String, Object> map} instance with the given key value sequence.
   *
   * Key value sequence consists of key - value pairs such that a key precedes its value. For instance:
   *
   * mapOf("name", "Adam", "age", 41) corresponds to {"name": "Adam", "age": 41} in JSON.
   */
  public static JsonStringHashMap<String, Object> mapOf(Object... keyValueSequence) {
    Preconditions.checkArgument(keyValueSequence.length%2==0, "Length of key value sequence must be even");
    final JsonStringHashMap<String, Object> map = new JsonStringHashMap<>();
    for (int i=0; i<keyValueSequence.length; i+=2) {
      Object value = keyValueSequence[i+1];
      if (value instanceof CharSequence) {
        value = new Text(value.toString());
      }
      map.put(String.class.cast(keyValueSequence[i]), value);
    }
    return map;
  }
}
