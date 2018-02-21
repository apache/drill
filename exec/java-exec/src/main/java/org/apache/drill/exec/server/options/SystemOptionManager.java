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
package org.apache.drill.exec.server.options;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.IteratorUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassCompilerSelector;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.store.sys.store.provider.InMemoryStoreProvider;
import org.apache.drill.exec.util.AssertionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * {@link OptionManager} that holds options within {@link org.apache.drill.exec.server.DrillbitContext}.
 * Only one instance of this class exists per drillbit. Options set at the system level affect the entire system and
 * persist between restarts.
 */

/**
 *  <p> All the system options are externalized into conf file. While adding a new system option
 *  a validator should be added and the default value for the option should be set in
 *  the conf files(example : drill-module.conf) under the namespace drill.exec.options.
 *  </p>
 *
 *  <p>
 *  The SystemOptionManager loads all the validators and the default values for the options are
 *  fetched from the config. The validators are populated with the default values fetched from
 *  the config. If the option is not set in the conf files config option is missing exception
 *  will be thrown.
 *  </p>
 *
 *  <p>
 *  If the option is set using ALTER, the value that is set will be returned. Else the default value
 *  that is loaded into validator from the config will be returned.
 *  </p>
 */
public class SystemOptionManager extends BaseOptionManager implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

  /**
   * Creates all the OptionDefinitions to be registered with the {@link SystemOptionManager}.
   * @return A map
   */
  public static CaseInsensitiveMap<OptionDefinition> createDefaultOptionDefinitions() {
    final OptionDefinition[] definitions = new OptionDefinition[]{
      new OptionDefinition(PlannerSettings.CONSTANT_FOLDING),
      new OptionDefinition(PlannerSettings.EXCHANGE),
      new OptionDefinition(PlannerSettings.HASHAGG),
      new OptionDefinition(PlannerSettings.STREAMAGG),
      new OptionDefinition(PlannerSettings.HASHJOIN),
      new OptionDefinition(PlannerSettings.MERGEJOIN),
      new OptionDefinition(PlannerSettings.NESTEDLOOPJOIN),
      new OptionDefinition(PlannerSettings.MULTIPHASE),
      new OptionDefinition(PlannerSettings.BROADCAST),
      new OptionDefinition(PlannerSettings.BROADCAST_THRESHOLD),
      new OptionDefinition(PlannerSettings.BROADCAST_FACTOR),
      new OptionDefinition(PlannerSettings.NESTEDLOOPJOIN_FACTOR),
      new OptionDefinition(PlannerSettings.NLJOIN_FOR_SCALAR),
      new OptionDefinition(PlannerSettings.JOIN_ROW_COUNT_ESTIMATE_FACTOR),
      new OptionDefinition(PlannerSettings.MUX_EXCHANGE),
      new OptionDefinition(PlannerSettings.ORDERED_MUX_EXCHANGE),
      new OptionDefinition(PlannerSettings.DEMUX_EXCHANGE),
      new OptionDefinition(PlannerSettings.PRODUCER_CONSUMER),
      new OptionDefinition(PlannerSettings.PRODUCER_CONSUMER_QUEUE_SIZE),
      new OptionDefinition(PlannerSettings.HASH_SINGLE_KEY),
      new OptionDefinition(PlannerSettings.IDENTIFIER_MAX_LENGTH),
      new OptionDefinition(PlannerSettings.HASH_JOIN_SWAP),
      new OptionDefinition(PlannerSettings.HASH_JOIN_SWAP_MARGIN_FACTOR),
      new OptionDefinition(PlannerSettings.PARTITION_SENDER_THREADS_FACTOR),
      new OptionDefinition(PlannerSettings.PARTITION_SENDER_MAX_THREADS),
      new OptionDefinition(PlannerSettings.PARTITION_SENDER_SET_THREADS),
      new OptionDefinition(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE),
      new OptionDefinition(PlannerSettings.HEP_OPT),
      new OptionDefinition(PlannerSettings.PLANNER_MEMORY_LIMIT),
      new OptionDefinition(PlannerSettings.HEP_PARTITION_PRUNING),
      new OptionDefinition(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR),
      new OptionDefinition(PlannerSettings.FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR),
      new OptionDefinition(PlannerSettings.TYPE_INFERENCE),
      new OptionDefinition(PlannerSettings.IN_SUBQUERY_THRESHOLD),
      new OptionDefinition(PlannerSettings.UNIONALL_DISTRIBUTE),
      new OptionDefinition(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING),
      new OptionDefinition(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD),
      new OptionDefinition(PlannerSettings.QUOTING_IDENTIFIERS),
      new OptionDefinition(PlannerSettings.JOIN_OPTIMIZATION),
      new OptionDefinition(PlannerSettings.FORCE_2PHASE_AGGR), // for testing
      new OptionDefinition(ExecConstants.HASHAGG_NUM_PARTITIONS_VALIDATOR),
      new OptionDefinition(ExecConstants.HASHAGG_MAX_MEMORY_VALIDATOR),
      new OptionDefinition(ExecConstants.HASHAGG_MIN_BATCHES_PER_PARTITION_VALIDATOR), // for tuning
      new OptionDefinition(ExecConstants.HASHAGG_USE_MEMORY_PREDICTION_VALIDATOR), // for testing
      new OptionDefinition(ExecConstants.HASHAGG_FALLBACK_ENABLED_VALIDATOR), // for enable/disable unbounded HashAgg
      new OptionDefinition(ExecConstants.CAST_TO_NULLABLE_NUMERIC_OPTION),
      new OptionDefinition(ExecConstants.OUTPUT_FORMAT_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_WRITER_USE_SINGLE_FS_BLOCK_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_PAGE_SIZE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_DICT_PAGE_SIZE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_PAGEREADER_ASYNC_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_PAGEREADER_QUEUE_SIZE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_PAGEREADER_ENFORCETOTALSIZE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_COLUMNREADER_ASYNC_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_PAGEREADER_BUFFER_SIZE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_PAGEREADER_USE_FADVISE_VALIDATOR),
      new OptionDefinition(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR),
      new OptionDefinition(ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR),
      new OptionDefinition(ExecConstants.JSON_WRITER_NAN_INF_NUMBERS_VALIDATOR),
      new OptionDefinition(ExecConstants.JSON_READER_NAN_INF_NUMBERS_VALIDATOR),
      new OptionDefinition(ExecConstants.ENABLE_UNION_TYPE),
      new OptionDefinition(ExecConstants.TEXT_ESTIMATED_ROW_SIZE),
      new OptionDefinition(ExecConstants.JSON_EXTENDED_TYPES),
      new OptionDefinition(ExecConstants.JSON_WRITER_UGLIFY),
      new OptionDefinition(ExecConstants.JSON_WRITER_SKIPNULLFIELDS),
      new OptionDefinition(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
      new OptionDefinition(ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR),
      new OptionDefinition(ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR),
      new OptionDefinition(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR),
      new OptionDefinition(ExecConstants.MONGO_READER_ALL_TEXT_MODE_VALIDATOR),
      new OptionDefinition(ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
      new OptionDefinition(ExecConstants.MONGO_BSON_RECORD_READER_VALIDATOR),
      new OptionDefinition(ExecConstants.KAFKA_READER_ALL_TEXT_MODE_VALIDATOR),
      new OptionDefinition(ExecConstants.KAFKA_RECORD_READER_VALIDATOR),
      new OptionDefinition(ExecConstants.KAFKA_POLL_TIMEOUT_VALIDATOR),
      new OptionDefinition(ExecConstants.KAFKA_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR),
      new OptionDefinition(ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR),
      new OptionDefinition(ExecConstants.SLICE_TARGET_OPTION),
      new OptionDefinition(ExecConstants.AFFINITY_FACTOR),
      new OptionDefinition(ExecConstants.MAX_WIDTH_GLOBAL),
      new OptionDefinition(ExecConstants.MAX_WIDTH_PER_NODE),
      new OptionDefinition(ExecConstants.ENABLE_QUEUE),
      new OptionDefinition(ExecConstants.LARGE_QUEUE_SIZE),
      new OptionDefinition(ExecConstants.QUEUE_THRESHOLD_SIZE),
      new OptionDefinition(ExecConstants.QUEUE_TIMEOUT),
      new OptionDefinition(ExecConstants.SMALL_QUEUE_SIZE),
      new OptionDefinition(ExecConstants.QUEUE_MEMORY_RESERVE, new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, false)),
      new OptionDefinition(ExecConstants.QUEUE_MEMORY_RATIO, new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, false)),
      new OptionDefinition(ExecConstants.MIN_HASH_TABLE_SIZE),
      new OptionDefinition(ExecConstants.MAX_HASH_TABLE_SIZE),
      new OptionDefinition(ExecConstants.EARLY_LIMIT0_OPT),
      new OptionDefinition(ExecConstants.ENABLE_MEMORY_ESTIMATION),
      new OptionDefinition(ExecConstants.MAX_QUERY_MEMORY_PER_NODE),
      new OptionDefinition(ExecConstants.PERCENT_MEMORY_PER_QUERY),
      new OptionDefinition(ExecConstants.MIN_MEMORY_PER_BUFFERED_OP),
      new OptionDefinition(ExecConstants.NON_BLOCKING_OPERATORS_MEMORY),
      new OptionDefinition(ExecConstants.HASH_JOIN_TABLE_FACTOR),
      new OptionDefinition(ExecConstants.HASH_AGG_TABLE_FACTOR),
      new OptionDefinition(ExecConstants.AVERAGE_FIELD_WIDTH),
      new OptionDefinition(ExecConstants.NEW_VIEW_DEFAULT_PERMS_VALIDATOR),
      new OptionDefinition(ExecConstants.CTAS_PARTITIONING_HASH_DISTRIBUTE_VALIDATOR),
      new OptionDefinition(ExecConstants.ADMIN_USERS_VALIDATOR, new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, false)),
      new OptionDefinition(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR, new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, false)),
      new OptionDefinition(ExecConstants.IMPERSONATION_POLICY_VALIDATOR, new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, false)),
      new OptionDefinition(ClassCompilerSelector.JAVA_COMPILER_VALIDATOR),
      new OptionDefinition(ClassCompilerSelector.JAVA_COMPILER_JANINO_MAXSIZE),
      new OptionDefinition(ClassCompilerSelector.JAVA_COMPILER_DEBUG),
      new OptionDefinition(ExecConstants.ENABLE_VERBOSE_ERRORS),
      new OptionDefinition(ExecConstants.ENABLE_WINDOW_FUNCTIONS_VALIDATOR),
      new OptionDefinition(ClassTransformer.SCALAR_REPLACEMENT_VALIDATOR),
      new OptionDefinition(ExecConstants.ENABLE_NEW_TEXT_READER),
      new OptionDefinition(ExecConstants.ENABLE_BULK_LOAD_TABLE_LIST),
      new OptionDefinition(ExecConstants.BULK_LOAD_TABLE_LIST_BULK_SIZE),
      new OptionDefinition(ExecConstants.WEB_LOGS_MAX_LINES_VALIDATOR),
      new OptionDefinition(ExecConstants.IMPLICIT_FILENAME_COLUMN_LABEL_VALIDATOR),
      new OptionDefinition(ExecConstants.IMPLICIT_SUFFIX_COLUMN_LABEL_VALIDATOR),
      new OptionDefinition(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL_VALIDATOR),
      new OptionDefinition(ExecConstants.IMPLICIT_FILEPATH_COLUMN_LABEL_VALIDATOR),
      new OptionDefinition(ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR),
      new OptionDefinition(ExecConstants.CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS_VALIDATOR),
      new OptionDefinition(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED_VALIDATOR,  new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, false)),
      new OptionDefinition(ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION),
      new OptionDefinition(ExecConstants.ENABLE_QUERY_PROFILE_VALIDATOR),
      new OptionDefinition(ExecConstants.QUERY_PROFILE_DEBUG_VALIDATOR),
      new OptionDefinition(ExecConstants.USE_DYNAMIC_UDFS),
      new OptionDefinition(ExecConstants.QUERY_TRANSIENT_STATE_UPDATE),
      new OptionDefinition(ExecConstants.PERSISTENT_TABLE_UMASK_VALIDATOR),
      new OptionDefinition(ExecConstants.CPU_LOAD_AVERAGE),
      new OptionDefinition(ExecConstants.ENABLE_VECTOR_VALIDATOR),
      new OptionDefinition(ExecConstants.ENABLE_ITERATOR_VALIDATOR),
      new OptionDefinition(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR, new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, false)),
      new OptionDefinition(ExecConstants.FRAG_RUNNER_RPC_TIMEOUT_VALIDATOR, new OptionMetaData(OptionValue.AccessibleScopes.SYSTEM, true, true)),
    };

    final CaseInsensitiveMap<OptionDefinition> map = CaseInsensitiveMap.newHashMap();

    for (final OptionDefinition definition: definitions) {
      map.put(definition.getValidator().getOptionName(), definition);
    }

    if (AssertionUtil.isAssertionsEnabled()) {
      map.put(ExecConstants.DRILLBIT_CONTROL_INJECTIONS, new OptionDefinition(ExecConstants.DRILLBIT_CONTROLS_VALIDATOR));
    }

    return map;
  }

  private final PersistentStoreConfig<PersistedOptionValue> config;

  private final PersistentStoreProvider provider;

  /**
   * Persistent store for options that have been changed from default.
   * NOTE: CRUD operations must use lowercase keys.
   */
  private PersistentStore<PersistedOptionValue> options;
  private CaseInsensitiveMap<OptionDefinition> definitions;
  private CaseInsensitiveMap<OptionValue> defaults;

  public SystemOptionManager(LogicalPlanPersistence lpPersistence, final PersistentStoreProvider provider,
                             final DrillConfig bootConfig) {
    this(lpPersistence, provider, bootConfig, SystemOptionManager.createDefaultOptionDefinitions());
  }

  public SystemOptionManager(final LogicalPlanPersistence lpPersistence, final PersistentStoreProvider provider,
                             final DrillConfig bootConfig, final CaseInsensitiveMap<OptionDefinition> definitions) {
    this.provider = provider;
    this.config = PersistentStoreConfig.newJacksonBuilder(lpPersistence.getMapper(), PersistedOptionValue.class)
          .name("sys.options")
          .build();
    this.definitions = definitions;
    this.defaults = populateDefaultValues(definitions, bootConfig);
  }

  /**
   * Test-only, in-memory version of the system option manager.
   *
   * @param bootConfig Drill config
   */

  @VisibleForTesting
  public SystemOptionManager(final DrillConfig bootConfig) {
    this.provider = new InMemoryStoreProvider(100);
    this.config = null;
    this.definitions = SystemOptionManager.createDefaultOptionDefinitions();
    this.defaults = populateDefaultValues(definitions, bootConfig);
  }

  /**
   * Initializes this option manager.
   *
   * @return this option manager
   * @throws IOException
   */
  public SystemOptionManager init() throws Exception {
    options = provider.getOrCreateStore(config);
    // if necessary, deprecate and replace options from persistent store
    for (final Entry<String, PersistedOptionValue> option : Lists.newArrayList(options.getAll())) {
      final String name = option.getKey();
      final OptionDefinition definition = definitions.get(name);
      if (definition == null) {
        // deprecated option, delete.
        options.delete(name);
        logger.warn("Deleting deprecated option `{}`", name);
      } else {
        OptionValidator validator = definition.getValidator();
        final String canonicalName = validator.getOptionName().toLowerCase();
        if (!name.equals(canonicalName)) {
          // for backwards compatibility <= 1.1, rename to lower case.
          logger.warn("Changing option name to lower case `{}`", name);
          final PersistedOptionValue value = option.getValue();
          options.delete(name);
          options.put(canonicalName, value);
        }
      }
    }

    return this;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    final Map<String, OptionValue> buildList = CaseInsensitiveMap.newHashMap();
    // populate the default options
    for (final Map.Entry<String, OptionValue> entry : defaults.entrySet()) {
      buildList.put(entry.getKey(), entry.getValue());
    }
    // override if changed
    for (final Map.Entry<String, PersistedOptionValue> entry : Lists.newArrayList(options.getAll())) {
      final String name = entry.getKey();
      final OptionDefinition optionDefinition = getOptionDefinition(name);
      final PersistedOptionValue persistedOptionValue = entry.getValue();
      final OptionValue optionValue = persistedOptionValue
        .toOptionValue(optionDefinition, OptionValue.OptionScope.SYSTEM);
      buildList.put(name, optionValue);
    }
    return buildList.values().iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    // check local space (persistent store)
    final PersistedOptionValue persistedValue = options.get(name.toLowerCase());

    if (persistedValue != null) {
      final OptionDefinition optionDefinition = getOptionDefinition(name);
      return persistedValue.toOptionValue(optionDefinition, OptionValue.OptionScope.SYSTEM);
    }

    // otherwise, return default set in the validator.
    return defaults.get(name);
  }

  @Override
  public OptionValue getDefault(String optionName) {
    OptionValue value = defaults.get(optionName);
    if (value == null) {
      throw UserException.systemError(null)
        .addContext("Undefined default value for option: " + optionName)
        .build(logger);
    }
    return value;
  }

  @Override
  protected void setLocalOptionHelper(final OptionValue value) {
    final String name = value.name.toLowerCase();
    final OptionDefinition definition = getOptionDefinition(name);
    final OptionValidator validator = definition.getValidator();

    validator.validate(value, definition.getMetaData(), this); // validate the option

    if (options.get(name) == null && value.equals(getDefault(name))) {
      return; // if the option is not overridden, ignore setting option to default
    }

    options.put(name, value.toPersisted());
  }

  @Override
  protected OptionValue.OptionScope getScope() {
    return OptionValue.OptionScope.SYSTEM;
  }

  @Override
  public void deleteLocalOption(final String name) {
    getOptionDefinition(name); // ensure option exists
    options.delete(name.toLowerCase());
  }

  @Override
  public void deleteAllLocalOptions() {
    final Set<String> names = Sets.newHashSet();
    for (final Map.Entry<String, PersistedOptionValue> entry : Lists.newArrayList(options.getAll())) {
      names.add(entry.getKey());
    }
    for (final String name : names) {
      options.delete(name); // should be lowercase
    }
  }

  public static CaseInsensitiveMap<OptionValue> populateDefaultValues(Map<String, OptionDefinition> definitions, DrillConfig bootConfig) {
    // populate the options from the config
    final Map<String, OptionValue> defaults = new HashMap<>();

    for (final Map.Entry<String, OptionDefinition> entry : definitions.entrySet()) {
      final OptionDefinition definition = entry.getValue();
      final OptionMetaData metaData = definition.getMetaData();
      final OptionValue.AccessibleScopes type = metaData.getAccessibleScopes();
      final OptionValidator validator = definition.getValidator();
      final String name = validator.getOptionName();
      final String configName = validator.getConfigProperty();
      final OptionValue.Kind kind = validator.getKind();
      OptionValue optionValue;

      switch (kind) {
        case BOOLEAN:
          optionValue = OptionValue.create(type, name,
            bootConfig.getBoolean(configName), OptionValue.OptionScope.BOOT);
          break;
        case LONG:
          optionValue = OptionValue.create(type, name,
            bootConfig.getLong(configName), OptionValue.OptionScope.BOOT);
          break;
        case STRING:
          optionValue = OptionValue.create(type, name,
            bootConfig.getString(configName), OptionValue.OptionScope.BOOT);
          break;
        case DOUBLE:
          optionValue = OptionValue.create(type, name,
            bootConfig.getDouble(configName), OptionValue.OptionScope.BOOT);
          break;
        default:
          throw new UnsupportedOperationException();
      }

      defaults.put(name, optionValue);
    }

    return CaseInsensitiveMap.newImmutableMap(defaults);
  }

  /**
   * Gets the {@link OptionDefinition} associated with the name.
   *
   * @param name name of the option
   * @return the associated option definition
   * @throws UserException - if the definition is not found
   */
  @Override
  public OptionDefinition getOptionDefinition(String name) {
    final OptionDefinition definition = definitions.get(name);
    if (definition == null) {
      throw UserException.validationError()
        .message(String.format("The option '%s' does not exist.", name.toLowerCase()))
        .build(logger);
    }
    return definition;
  }

  @Override
  public OptionList getOptionList() {
    return (OptionList) IteratorUtils.toList(iterator());
  }

  @Override
  public void close() throws Exception {
    // If the server exits very early, the options may not yet have
    // been created. Gracefully handle that case.

    if (options != null) {
      options.close();
    }
  }
}
