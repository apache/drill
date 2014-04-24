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
package org.apache.drill.exec.rpc.user;

import com.google.common.collect.*;
import net.hydromatic.optiq.impl.ViewTable;

import java.util.Collections;
import java.util.Map;

/**
 * ViewStore for holding and managing the views created in a user session.
 *
 * Note: Currently these views are for session only and doesn't persist across session.
 */
public class ViewStore {
  // Map of views associated in a schema
  private Map<String, Map<String, ViewTable>> viewMap = Maps.newHashMap();

  /**
   * Get all views belonging to the given schema.
   * @param schemaPath
   * @return Map of (viewName, ViewTable) entries.
   */
  public Map<String, ViewTable> getViews(String schemaPath) {
    if (viewMap.containsKey(schemaPath))
      return viewMap.get(schemaPath);

    return Collections.emptyMap();
  }

  /**
   * Add the given view to store.
   *
   * @param schemaPath
   * @param viewName Name of the view.
   * @param view {@link ViewTable} object.
   * @param replace If a view with given name already exists, replace it.
   * @return True if a view is replaced.
   */
  public boolean addView(String schemaPath, String viewName, ViewTable view, boolean replace) throws ViewAlreadyExistsException {
    if (!viewMap.containsKey(schemaPath)) {
      Map<String, ViewTable> viewNameMap = Maps.newHashMap();
      viewMap.put(schemaPath, viewNameMap);
    }

    boolean replaced = false;
    if (viewMap.get(schemaPath).get(viewName) != null) {
      if (replace)
        replaced = true;
      else
        throw new ViewAlreadyExistsException(schemaPath, viewName);
    }
    viewMap.get(schemaPath).put(viewName, view);

    return replaced;
  }

  /** Delete the ViewTable with given parameters from store. */
  public void dropView(String schemaPath, String viewName) throws NoSuchViewExistsException {
    if (viewMap.containsKey(schemaPath) && viewMap.get(schemaPath).containsKey(viewName))
      viewMap.get(schemaPath).remove(viewName);
    else
      throw new NoSuchViewExistsException(schemaPath, viewName);
  }

  public static class ViewAlreadyExistsException extends Exception {
    public ViewAlreadyExistsException(String schemaPath, String viewName){
      super(String.format("Schema '%s' already contains a view view name '%s'.", schemaPath, viewName));
    }
  }

  public static class NoSuchViewExistsException extends Exception {
    public NoSuchViewExistsException(String schemaPath, String viewName){
      super(String.format("Schema '%s' has no view named '%s'", schemaPath, viewName));
    }
  }
}
