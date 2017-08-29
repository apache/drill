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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.typesafe.config.ConfigException;
import org.apache.commons.collections.IteratorUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassCompilerSelector;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.util.AssertionUtil;

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

public class SystemOptionManager extends BaseOptionManager implements OptionManager, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

  private CaseInsensitiveMap<OptionValidator> VALIDATORS;
  public void populateValidators() {
    final OptionValidator[] validators = new OptionValidator[]{
      PlannerSettings.CONSTANT_FOLDING,
      PlannerSettings.EXCHANGE,
      PlannerSettings.HASHAGG,
      PlannerSettings.STREAMAGG,
      PlannerSettings.HASHJOIN,
      PlannerSettings.MERGEJOIN,
      PlannerSettings.NESTEDLOOPJOIN,
      PlannerSettings.MULTIPHASE,
      PlannerSettings.BROADCAST,
      PlannerSettings.BROADCAST_THRESHOLD,
      PlannerSettings.BROADCAST_FACTOR,
      PlannerSettings.NESTEDLOOPJOIN_FACTOR,
      PlannerSettings.NLJOIN_FOR_SCALAR,
      PlannerSettings.JOIN_ROW_COUNT_ESTIMATE_FACTOR,
      PlannerSettings.MUX_EXCHANGE,
      PlannerSettings.DEMUX_EXCHANGE,
      PlannerSettings.PRODUCER_CONSUMER,
      PlannerSettings.PRODUCER_CONSUMER_QUEUE_SIZE,
      PlannerSettings.HASH_SINGLE_KEY,
      PlannerSettings.IDENTIFIER_MAX_LENGTH,
      PlannerSettings.HASH_JOIN_SWAP,
      PlannerSettings.HASH_JOIN_SWAP_MARGIN_FACTOR,
      PlannerSettings.PARTITION_SENDER_THREADS_FACTOR,
      PlannerSettings.PARTITION_SENDER_MAX_THREADS,
      PlannerSettings.PARTITION_SENDER_SET_THREADS,
      PlannerSettings.ENABLE_DECIMAL_DATA_TYPE,
      PlannerSettings.HEP_OPT,
      PlannerSettings.PLANNER_MEMORY_LIMIT,
      PlannerSettings.HEP_PARTITION_PRUNING,
      PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR,
      PlannerSettings.FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR,
      PlannerSettings.TYPE_INFERENCE,
      PlannerSettings.IN_SUBQUERY_THRESHOLD,
      PlannerSettings.UNIONALL_DISTRIBUTE,
      PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING,
      PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD,
      PlannerSettings.QUOTING_IDENTIFIERS,
      PlannerSettings.JOIN_OPTIMIZATION,
      PlannerSettings.FORCE_2PHASE_AGGR, // for testing
      ExecConstants.HASHAGG_NUM_PARTITIONS_VALIDATOR,
      ExecConstants.HASHAGG_MAX_MEMORY_VALIDATOR,
      ExecConstants.HASHAGG_MIN_BATCHES_PER_PARTITION_VALIDATOR, // for tuning
      ExecConstants.CAST_TO_NULLABLE_NUMERIC_OPTION,
      ExecConstants.OUTPUT_FORMAT_VALIDATOR,
      ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR,
      ExecConstants.PARQUET_WRITER_USE_SINGLE_FS_BLOCK_VALIDATOR,
      ExecConstants.PARQUET_PAGE_SIZE_VALIDATOR,
      ExecConstants.PARQUET_DICT_PAGE_SIZE_VALIDATOR,
      ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR,
      ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR,
      ExecConstants.PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR,
      ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR,
      ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR,
      ExecConstants.PARQUET_PAGEREADER_ASYNC_VALIDATOR,
      ExecConstants.PARQUET_PAGEREADER_QUEUE_SIZE_VALIDATOR,
      ExecConstants.PARQUET_PAGEREADER_ENFORCETOTALSIZE_VALIDATOR,
      ExecConstants.PARQUET_COLUMNREADER_ASYNC_VALIDATOR,
      ExecConstants.PARQUET_PAGEREADER_USE_BUFFERED_READ_VALIDATOR,
      ExecConstants.PARQUET_PAGEREADER_BUFFER_SIZE_VALIDATOR,
      ExecConstants.PARQUET_PAGEREADER_USE_FADVISE_VALIDATOR,
      ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP_VALIDATOR,
      ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR,
      ExecConstants.ENABLE_UNION_TYPE,
      ExecConstants.TEXT_ESTIMATED_ROW_SIZE,
      ExecConstants.JSON_EXTENDED_TYPES,
      ExecConstants.JSON_WRITER_UGLIFY,
      ExecConstants.JSON_WRITER_SKIPNULLFIELDS,
      ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR,
      ExecConstants.JSON_SKIP_MALFORMED_RECORDS_VALIDATOR,
      ExecConstants.JSON_READER_PRINT_INVALID_RECORDS_LINE_NOS_FLAG_VALIDATOR,
      ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR,
      ExecConstants.MONGO_READER_ALL_TEXT_MODE_VALIDATOR,
      ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR,
      ExecConstants.MONGO_BSON_RECORD_READER_VALIDATOR,
      ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS_VALIDATOR,
      ExecConstants.SLICE_TARGET_OPTION,
      ExecConstants.AFFINITY_FACTOR,
      ExecConstants.MAX_WIDTH_GLOBAL,
      ExecConstants.MAX_WIDTH_PER_NODE,
      ExecConstants.ENABLE_QUEUE,
      ExecConstants.LARGE_QUEUE_SIZE,
      ExecConstants.QUEUE_THRESHOLD_SIZE,
      ExecConstants.QUEUE_TIMEOUT,
      ExecConstants.SMALL_QUEUE_SIZE,
      ExecConstants.MIN_HASH_TABLE_SIZE,
      ExecConstants.MAX_HASH_TABLE_SIZE,
      ExecConstants.EARLY_LIMIT0_OPT,
      ExecConstants.ENABLE_MEMORY_ESTIMATION,
      ExecConstants.MAX_QUERY_MEMORY_PER_NODE,
      ExecConstants.MIN_MEMORY_PER_BUFFERED_OP,
      ExecConstants.NON_BLOCKING_OPERATORS_MEMORY,
      ExecConstants.HASH_JOIN_TABLE_FACTOR,
      ExecConstants.HASH_AGG_TABLE_FACTOR,
      ExecConstants.AVERAGE_FIELD_WIDTH,
      ExecConstants.NEW_VIEW_DEFAULT_PERMS_VALIDATOR,
      ExecConstants.CTAS_PARTITIONING_HASH_DISTRIBUTE_VALIDATOR,
      ExecConstants.ADMIN_USERS_VALIDATOR,
      ExecConstants.ADMIN_USER_GROUPS_VALIDATOR,
      ExecConstants.IMPERSONATION_POLICY_VALIDATOR,
      ClassCompilerSelector.JAVA_COMPILER_VALIDATOR,
      ClassCompilerSelector.JAVA_COMPILER_JANINO_MAXSIZE,
      ClassCompilerSelector.JAVA_COMPILER_DEBUG,
      ExecConstants.ENABLE_VERBOSE_ERRORS,
      ExecConstants.ENABLE_WINDOW_FUNCTIONS_VALIDATOR,
      ClassTransformer.SCALAR_REPLACEMENT_VALIDATOR,
      ExecConstants.ENABLE_NEW_TEXT_READER,
      ExecConstants.ENABLE_BULK_LOAD_TABLE_LIST,
      ExecConstants.BULK_LOAD_TABLE_LIST_BULK_SIZE,
      ExecConstants.WEB_LOGS_MAX_LINES_VALIDATOR,
      ExecConstants.IMPLICIT_FILENAME_COLUMN_LABEL_VALIDATOR,
      ExecConstants.IMPLICIT_SUFFIX_COLUMN_LABEL_VALIDATOR,
      ExecConstants.IMPLICIT_FQN_COLUMN_LABEL_VALIDATOR,
      ExecConstants.IMPLICIT_FILEPATH_COLUMN_LABEL_VALIDATOR,
      ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR,
      ExecConstants.CREATE_PREPARE_STATEMENT_TIMEOUT_MILLIS_VALIDATOR,
      ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED_VALIDATOR,
      ExecConstants.EXTERNAL_SORT_DISABLE_MANAGED_OPTION,
      ExecConstants.ENABLE_QUERY_PROFILE_VALIDATOR,
      ExecConstants.QUERY_PROFILE_DEBUG_VALIDATOR,
      ExecConstants.USE_DYNAMIC_UDFS,
      ExecConstants.QUERY_TRANSIENT_STATE_UPDATE,
      ExecConstants.PERSISTENT_TABLE_UMASK_VALIDATOR,
      ExecConstants.CPU_LOAD_AVERAGE,
      ExecConstants.ENABLE_VECTOR_VALIDATOR,
      ExecConstants.ENABLE_ITERATOR_VALIDATOR
    };
    final Map<String, OptionValidator> tmp = new HashMap<>();
    for (final OptionValidator validator : validators) {
      tmp.put(validator.getOptionName(), validator);
    }
    if (AssertionUtil.isAssertionsEnabled()) {
      tmp.put(ExecConstants.DRILLBIT_CONTROL_INJECTIONS, ExecConstants.DRILLBIT_CONTROLS_VALIDATOR);
    }
    VALIDATORS = CaseInsensitiveMap.newImmutableMap(tmp);
  }

  private final PersistentStoreConfig<OptionValue> config;

  private final PersistentStoreProvider provider;

  private final DrillConfig bootConfig;
  /**
   * Persistent store for options that have been changed from default.
   * NOTE: CRUD operations must use lowercase keys.
   */
  private PersistentStore<OptionValue> options;

  public SystemOptionManager(LogicalPlanPersistence lpPersistence, final PersistentStoreProvider provider) {
    this(lpPersistence,provider,null);
  }

  public SystemOptionManager(LogicalPlanPersistence lpPersistence, final PersistentStoreProvider provider, final DrillConfig bootConfig) {
    this.provider = provider;
    this.config = PersistentStoreConfig.newJacksonBuilder(lpPersistence.getMapper(), OptionValue.class)
          .name("sys.options")
          .build();
    this.bootConfig = bootConfig;
    populateValidators();
    populateDefaultValues();

  }

  /**
   * Gets the {@link OptionValidator} associated with the name.
   *
   * @param name name of the option
   * @return the associated validator
   * @throws UserException - if the validator is not found
   */
  public OptionValidator getValidator(final String name) {
    final OptionValidator validator = VALIDATORS.get(name);
    if (validator == null) {
      throw UserException.validationError()
              .message(String.format("The option '%s' does not exist.", name.toLowerCase()))
              .build(logger);
    }
    return validator;
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
    for (final Entry<String, OptionValue> option : Lists.newArrayList(options.getAll())) {
      final String name = option.getKey();
      final OptionValidator validator = VALIDATORS.get(name);
      if (validator == null) {
        // deprecated option, delete.
        options.delete(name);
        logger.warn("Deleting deprecated option `{}`", name);
      } else {
        final String canonicalName = validator.getOptionName().toLowerCase();
        if (!name.equals(canonicalName)) {
          // for backwards compatibility <= 1.1, rename to lower case.
          logger.warn("Changing option name to lower case `{}`", name);
          final OptionValue value = option.getValue();
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
    for (final Map.Entry<String, OptionValidator> entry : VALIDATORS.entrySet()) {
      buildList.put(entry.getKey(), entry.getValue().getDefault());
    }
    // override if changed
    for (final Map.Entry<String, OptionValue> entry : Lists.newArrayList(options.getAll())) {
      buildList.put(entry.getKey(), entry.getValue());
    }
    return buildList.values().iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    // check local space (persistent store)
    final OptionValue value = options.get(name.toLowerCase());
    if (value != null) {
      return value;
    }

    // If option is not set return the default set in the validator.
    final OptionValidator validator = getValidator(name);
    return validator.getDefault();
  }

  @Override
  public void setOption(final OptionValue value) {
    checkArgument(value.type == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final String name = value.name.toLowerCase();
    final OptionValidator validator = getValidator(name);

    validator.validate(value, this); // validate the option
    if (options.get(name) == null && value.equals(validator.getDefault())) {
      return; // if the option is not overridden, ignore setting option to default
    }
    options.put(name, value);
  }

  @Override
  public void deleteOption(final String name, OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");

    getValidator(name); // ensure option exists
    options.delete(name.toLowerCase());
  }

  @Override
  public void deleteAllOptions(OptionType type) {
    checkArgument(type == OptionType.SYSTEM, "OptionType must be SYSTEM.");
    final Set<String> names = Sets.newHashSet();
    for (final Map.Entry<String, OptionValue> entry : Lists.newArrayList(options.getAll())) {
      names.add(entry.getKey());
    }
    for (final String name : names) {
      options.delete(name); // should be lowercase
    }
  }

  public void populateDefaultValues() {

    // populate the options from the config
    final Map<String, OptionValidator> tmp = new HashMap<>();
    for (OptionValidator validator: VALIDATORS.values()) {
      try {
         validator.loadDefault(bootConfig);
      } catch (ConfigException.Missing e) {
        throw new IllegalStateException("Config Option is missing"+ validator.getOptionName());
      }
    }
  }

  @Override
  public OptionList getOptionList() {
    return (OptionList) IteratorUtils.toList(iterator());
  }

  @Override
  public void close() throws Exception {
    options.close();
  }
}
