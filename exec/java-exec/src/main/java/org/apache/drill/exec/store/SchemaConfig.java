/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.ops.ViewExpansionContext;
import org.apache.drill.exec.server.options.OptionValue;

/**
 * Contains information needed by {@link org.apache.drill.exec.store.AbstractSchema} implementations.
 */
public class SchemaConfig {
  private final String userName;
  private final QueryContext queryContext;
  private final boolean ignoreAuthErrors;

  private SchemaConfig(final String userName, final QueryContext queryContext, final boolean ignoreAuthErrors) {
    this.userName = userName;
    this.queryContext = queryContext;
    this.ignoreAuthErrors = ignoreAuthErrors;
  }

  public static Builder newBuilder(final String userName, final QueryContext queryContext) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(userName), "A valid userName is expected");
    Preconditions.checkNotNull(queryContext, "Non-null QueryContext is expected");
    return new Builder(userName, queryContext);
  }

  public static class Builder {
    final String userName;
    final QueryContext queryContext;
    boolean ignoreAuthErrors;

    private Builder(final String userName, final QueryContext queryContext) {
      this.userName = userName;
      this.queryContext = queryContext;
    }

    public Builder setIgnoreAuthErrors(boolean ignoreAuthErrors) {
      this.ignoreAuthErrors = ignoreAuthErrors;
      return this;
    }

    public SchemaConfig build() {
      return new SchemaConfig(userName, queryContext, ignoreAuthErrors);
    }
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  /**
   * @return User whom to impersonate as while {@link net.hydromatic.optiq.SchemaPlus} instances
   * interact with the underlying storage.
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @return Should ignore if authorization errors are reported while {@link net.hydromatic.optiq.SchemaPlus}
   * instances interact with the underlying storage.
   */
  public boolean getIgnoreAuthErrors() {
    return ignoreAuthErrors;
  }

  public OptionValue getOption(String optionKey) {
    return queryContext.getOptions().getOption(optionKey);
  }

  public ViewExpansionContext getViewExpansionContext() {
    return queryContext.getViewExpansionContext();
  }
}
