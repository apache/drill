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
package org.apache.drill.exec.udfs;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.registry.FunctionHolder;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the {@code desc} attribute on {@code @FunctionTemplate} survives the trip
 * into the function registry, where the metadata REST API and the AI assistant read it.
 *
 * <p>These UDFs are the main consumers of the field: a model has no priors for
 * {@code punctuation_pattern} or {@code st_dwithin} the way it does for {@code upper}, so a
 * missing description here is the difference between the assistant using the function and
 * inventing something else.
 */
@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestUdfDescriptions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  /** Maps every registered name to its holder, across all jars. */
  private static Map<String, DrillFuncHolder> registeredFunctions() {
    Map<String, DrillFuncHolder> byName = new HashMap<>();
    Map<String, List<FunctionHolder>> jars = cluster.drillbit().getContext()
        .getFunctionImplementationRegistry()
        .getLocalFunctionRegistry()
        .getAllJarsWithFunctionsHolders();
    for (List<FunctionHolder> holders : jars.values()) {
      if (holders == null) {
        continue;
      }
      for (FunctionHolder holder : holders) {
        // Keep the first holder that carries a description: overloads of one name share
        // a meaning and only one of them needs to declare it.
        DrillFuncHolder existing = byName.get(holder.getName());
        if (existing == null || existing.getDesc().isEmpty()) {
          byName.put(holder.getName(), holder.getHolder());
        }
      }
    }
    return byName;
  }

  @Test
  public void testDescriptionsReachTheRegistry() {
    Map<String, DrillFuncHolder> functions = registeredFunctions();

    assertTrue("punctuation_pattern should be registered",
        functions.containsKey("punctuation_pattern"));
    assertEquals(
        "Extracts only the punctuation from a string, with spaces as underscores. Used to "
            + "cluster log lines that share a structure.",
        functions.get("punctuation_pattern").getDesc());
  }

  /**
   * Descriptions may not contain parentheses or apostrophes.
   *
   * <p>This is not a style rule. {@code FunctionInitializer.convertToCompilationUnit} strips
   * annotations with the regex {@code @\w+(?:\([^\\]*?\))?} before handing the source to
   * Janino for code generation. That match is non-greedy and ends at the FIRST close paren,
   * so a parenthesis inside a desc string truncates it and leaves the remainder as stray
   * tokens; Janino then fails the whole compilation unit with "Failure reading Function
   * class" at query time, not at build time. An apostrophe trips Janino's scanner separately,
   * which reads it as the start of a character literal.
   *
   * <p>Nothing else in the build catches this: the code compiles, the annotation is valid
   * Java, and the failure only appears when a query actually calls a function declared in the
   * affected file — including functions that have no description of their own, since Janino
   * parses the entire file.
   */
  @Test
  public void testDescriptionsAvoidCharactersThatBreakJanino() {
    List<String> offenders = new ArrayList<>();
    for (Map.Entry<String, DrillFuncHolder> entry : registeredFunctions().entrySet()) {
      String desc = entry.getValue().getDesc();
      if (desc.indexOf('(') >= 0 || desc.indexOf(')') >= 0 || desc.indexOf('\'') >= 0) {
        offenders.add(entry.getKey() + ": " + desc);
      }
    }
    assertTrue("Function descriptions must not contain parentheses or apostrophes; see this "
        + "test's javadoc for why: " + offenders, offenders.isEmpty());
  }

  /**
   * Every UDF in this module carries a description. If a new one is added without one, this
   * fails and names it, rather than the gap being noticed only when the assistant misuses it.
   */
  @Test
  public void testEveryContribUdfHasADescription() {
    Map<String, DrillFuncHolder> functions = registeredFunctions();

    // A representative name from each source file in this module. Aliases of the same
    // function share a description, so one name per function is enough. Use the snake_case
    // alias where a function has one: the registry lowercases names, so a camelCase alias
    // like "nearestDate" is not a registry key.
    String[] udfNames = {
        "get_map_schema", "md5", "sha256", "aes_encrypt", "aes_decrypt",
        "get_host_name", "dns_lookup", "whois", "nearest_date", "yearweek",
        "width_bucket", "kendall_correlation", "regr_slope", "regr_intercept",
        "percent_change", "in_network", "address_count", "inet_aton", "is_private_ip",
        "url_encode", "caverphone1", "cologne_phonetic", "dm_soundex", "soundex",
        "metaphone", "double_metaphone", "cosine_distance", "fuzzy_score",
        "hamming_distance", "jaccard_distance", "jaro_distance", "levenshtein_distance",
        "punctuation_pattern", "entropy", "entropy_per_byte", "time_bucket",
        "time_bucket_ns", "parse_user_agent", "st_astext", "st_buffer", "st_contains",
        "st_distance", "st_dwithin", "st_envelope", "st_geomfromtext", "st_intersects",
        "st_point", "st_relate", "st_transform", "st_union", "st_within", "st_x", "st_ymax",
    };

    List<String> missing = new ArrayList<>();
    for (String name : udfNames) {
      DrillFuncHolder holder = functions.get(name);
      if (holder == null) {
        missing.add(name + " (not registered)");
      } else if (holder.getDesc().isEmpty()) {
        missing.add(name + " (no desc)");
      }
    }
    assertTrue("UDFs missing a description: " + missing, missing.isEmpty());
  }
}
