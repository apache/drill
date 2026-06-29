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
package org.apache.drill.exec.server.rest.ai;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Static registry of available LLM providers.
 */
public final class LlmProviderRegistry {

  private static final Map<String, LlmProvider> PROVIDERS = new LinkedHashMap<>();

  static {
    register(new OpenAiCompatibleProvider());
    register(new AnthropicProvider());
  }

  private LlmProviderRegistry() {
  }

  public static void register(LlmProvider provider) {
    PROVIDERS.put(provider.getId(), provider);
  }

  public static LlmProvider get(String id) {
    return PROVIDERS.get(id);
  }

  public static Collection<LlmProvider> getAll() {
    return PROVIDERS.values();
  }
}
