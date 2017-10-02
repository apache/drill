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

import org.apache.drill.common.exceptions.UserException;

import java.util.Iterator;

/**
 * This {@link OptionManager} implements some the basic methods and should be extended by concrete implementations.
 */
public abstract class BaseOptionManager extends BaseOptionSet implements OptionManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseOptionManager.class);

  @Override
  public OptionList getInternalOptionList() {
    return getAllOptionList(true);
  }

  @Override
  public OptionList getPublicOptionList() {
    return getAllOptionList(false);
  }

  @Override
  public void setLocalOption(final String name, final boolean value) {
    setLocalOption(name, Boolean.valueOf(value));
  }

  @Override
  public void setLocalOption(final String name, final long value) {
    setLocalOption(name, Long.valueOf(value));
  }

  @Override
  public void setLocalOption(final String name, final double value) {
    setLocalOption(name, Double.valueOf(value));
  }

  @Override
  public void setLocalOption(final String name, final String value) {
    setLocalOption(name, (Object) value);
  }

  @Override
  public void setLocalOption(final String name, final Object value) {
    final OptionDefinition definition = getOptionDefinition(name);
    final OptionValidator validator = definition.getValidator();
    final OptionMetaData metaData = definition.getMetaData();
    final OptionValue.AccessibleScopes type = definition.getMetaData().getAccessibleScopes();
    final OptionValue.OptionScope scope = getScope();
    checkOptionPermissions(name, type, scope);
    final OptionValue optionValue = OptionValue.create(type, name, value, scope);
    validator.validate(optionValue, metaData, this);
    setLocalOptionHelper(optionValue);
  }

  @Override
  public void setLocalOption(final OptionValue.Kind kind, final String name, final String valueStr) {
    Object value;

    switch (kind) {
      case LONG:
        value = Long.valueOf(valueStr);
        break;
      case DOUBLE:
        value = Double.valueOf(valueStr);
        break;
      case STRING:
        value = valueStr;
        break;
      case BOOLEAN:
        value = Boolean.valueOf(valueStr);
        break;
      default:
        throw new IllegalArgumentException(String.format("Unsupported kind %s", kind));
    }

    setLocalOption(name, value);
  }

  private static void checkOptionPermissions(String name, OptionValue.AccessibleScopes type, OptionValue.OptionScope scope) {
    if (!type.inScopeOf(scope)) {
      throw UserException.permissionError()
        .message(String.format("Cannot change option %s in scope %s", name, scope))
        .build(logger);
    }
  }

  abstract protected void setLocalOptionHelper(OptionValue optionValue);

  abstract protected OptionValue.OptionScope getScope();

  private OptionList getAllOptionList(boolean internal)
  {
    Iterator<OptionValue> values = this.iterator();
    OptionList optionList = new OptionList();

    while (values.hasNext()) {
      OptionValue value = values.next();

      if (getOptionDefinition(value.getName()).getMetaData().isInternal() == internal) {
        optionList.add(value);
      }
    }

    return optionList;
  }
}
