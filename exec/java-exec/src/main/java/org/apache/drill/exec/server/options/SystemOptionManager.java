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
package org.apache.drill.exec.server.options;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.commons.collections.IteratorUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.util.AssertionUtil;

public class SystemOptionManager extends BaseOptionManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

  private static final ImmutableList<OptionValidator> VALIDATORS;

  static {
    final ImmutableList.Builder<OptionValidator> builder = ImmutableList.<OptionValidator>builder().add(
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
      PlannerSettings.HEP_JOIN_OPT,
      ExecConstants.CAST_TO_NULLABLE_NUMERIC_OPTION,
      ExecConstants.OUTPUT_FORMAT_VALIDATOR,
      ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR,
      ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR,
      ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR,
      ExecConstants.PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR,
      ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR,
      ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR,
      ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR,
      ExecConstants.TEXT_ESTIMATED_ROW_SIZE,
      ExecConstants.JSON_EXTENDED_TYPES,
      ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE_VALIDATOR,
      ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR,
      ExecConstants.MONGO_READER_ALL_TEXT_MODE_VALIDATOR,
      ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE_VALIDATOR,
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
      ExecConstants.ENABLE_MEMORY_ESTIMATION,
      ExecConstants.MAX_QUERY_MEMORY_PER_NODE,
      ExecConstants.NON_BLOCKING_OPERATORS_MEMORY,
      ExecConstants.HASH_JOIN_TABLE_FACTOR,
      ExecConstants.HASH_AGG_TABLE_FACTOR,
      ExecConstants.AVERAGE_FIELD_WIDTH,
      ExecConstants.NEW_VIEW_DEFAULT_PERMS_VALIDATOR,
      ExecConstants.USE_OLD_ASSIGNMENT_CREATOR_VALIDATOR,
      ExecConstants.CTAS_PARTITIONING_HASH_DISTRIBUTE_VALIDATOR,
      QueryClassLoader.JAVA_COMPILER_VALIDATOR,
      QueryClassLoader.JAVA_COMPILER_JANINO_MAXSIZE,
      QueryClassLoader.JAVA_COMPILER_DEBUG,
      ExecConstants.ENABLE_VERBOSE_ERRORS,
      ExecConstants.ENABLE_WINDOW_FUNCTIONS_VALIDATOR,
      ClassTransformer.SCALAR_REPLACEMENT_VALIDATOR,
      ExecConstants.ENABLE_NEW_TEXT_READER
    );
    if (AssertionUtil.isAssertionsEnabled()) {
      builder.add(ExecConstants.DRILLBIT_CONTROLS_VALIDATOR);
    }
    VALIDATORS = builder.build();
  }

  private final PStoreConfig<OptionValue> config;

  // persistent store for options that have been changed from default
  // NOTE: CRUD operations must use lowercase keys
  private PStore<OptionValue> options;

  private SystemOptionAdmin admin;

  // all known options; also contains default values
  // NOTE: CRUD operations must use lowercase keys
  private final ConcurrentMap<String, OptionValidator> knownOptions = Maps.newConcurrentMap();

  private final PStoreProvider provider;

  public SystemOptionManager(final DrillConfig config, final PStoreProvider provider) {
    this.provider = provider;
    this.config =  PStoreConfig.newJacksonBuilder(config.getMapper(), OptionValue.class)
        .name("sys.options")
        .build();
  }

  public SystemOptionManager init() throws IOException {
    options = provider.getStore(config);
    admin = new SystemOptionAdmin();
    return this;
  }

  /**
   * Get the {@link OptionValue} from the persistent store.
   *
   * @param name name of the option
   * @return the option value of present; null otherwise
   */
  private OptionValue getFromOptions(final String name) {
    return options.get(name.toLowerCase());
  }

  /**
   * Gets the {@link OptionValidator} from the known validators.
   *
   * @param name name of the option
   * @return the associated validator
   * @throws UserException - if the validator is not found
   */
  private OptionValidator getFromKnown(final String name) {
    final OptionValidator validator = knownOptions.get(name.toLowerCase());
    if (validator == null) {
      throw UserException.validationError()
        .message("Unknown option %s", name)
        .build(logger);
    }
    return validator;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    final Map<String, OptionValue> buildList = Maps.newHashMap();
    // populate the default options
    for (final Map.Entry<String, OptionValidator> v : knownOptions.entrySet()) {
      buildList.put(v.getKey(), v.getValue().getDefault());
    }
    // override if changed
    for (final Map.Entry<String, OptionValue> v : options) {
      buildList.put(v.getKey(), v.getValue());
    }
    return buildList.values().iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    // check local space
    final OptionValue value = getFromOptions(name);
    if (value != null) {
      return value;
    }

    // otherwise, return default.
    final OptionValidator validator = getFromKnown(name);
    return validator.getDefault();
  }

  @Override
  public OptionValue getDefault(final String name) {
    return getFromKnown(name).getDefault();
  }

  @Override
  public void setOption(final OptionValue value) {
    assert value.type == OptionType.SYSTEM;
    admin.validate(value);
    setOptionInternal(value);
  }

  private void setOptionInternal(final OptionValue value) {
    final String name = value.name;
    if (getFromOptions(name) == null && value.equals(getFromKnown(name).getDefault())) {
      return; // if the option is not overridden, ignore setting option to default
    }
    options.put(name.toLowerCase(), value);
  }

  @Override
  public void setOption(final String name, final SqlLiteral literal, final OptionType type) {
    assert type == OptionValue.OptionType.SYSTEM;
    final OptionValue v = admin.validate(name, literal, type);
    setOptionInternal(v);
  }

  @Override
  public OptionList getOptionList() {
    return (OptionList) IteratorUtils.toList(iterator());
  }

  @Override
  public OptionManager getSystemManager() {
    return this;
  }

  @Override
  public OptionAdmin getAdmin() {
    return admin;
  }

  private class SystemOptionAdmin implements OptionAdmin {

    public SystemOptionAdmin() {
      // register all options with a lower case name
      for (final OptionValidator v : VALIDATORS) {
        final String name = v.getOptionName().toLowerCase();
        if (!name.equals(v.getOptionName())) {
          logger.warn("Registering option with a lower case name `{}`", name);
        }
        knownOptions.put(name, v);
      }

      // if necessary, deprecate and replace options from persistent store
      for (final Entry<String, OptionValue> v : options) {
        final String name = v.getKey();
        final OptionValidator defaultValidator = knownOptions.get(name.toLowerCase());
        if (defaultValidator == null) {
          // deprecated option, delete.
          options.delete(name);
          logger.warn("Deleting deprecated option `{}`", name);
        } else {
          if (!name.equals(defaultValidator.getOptionName().toLowerCase())) {
            // for backwards compatibility <= 1.1, rename to lower case
            final OptionValue value = v.getValue();
            options.delete(name);
            options.put(name.toLowerCase(), value);
            logger.warn("Changing option name to lower case `{}`", name);
          }
        }
      }
    }

    @Override
    public OptionValidator getValidator(final String name) {
      return getFromKnown(name);
    }

    @Override
    public void validate(final OptionValue v) {
      final OptionValidator validator = getFromKnown(v.name);
      validator.validate(v);
    }

    @Override
    public OptionValue validate(final String name, final SqlLiteral value, final OptionType optionType) {
      final OptionValidator validator = getFromKnown(name);
      return validator.validate(value, optionType);
    }
  }
}
