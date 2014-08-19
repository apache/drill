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

import org.eigenbase.sql.SqlLiteral;

public interface OptionManager extends Iterable<OptionValue>{
  public OptionValue getOption(String name);
  public void setOption(OptionValue value) throws SetOptionException;
  public void setOption(String name, SqlLiteral literal, OptionValue.OptionType type) throws SetOptionException;
  public OptionAdmin getAdmin();
  public OptionManager getSystemManager();
  public OptionList getOptionList();

  public interface OptionAdmin{
    public void registerOptionType(OptionValidator validator);
    public void validate(OptionValue v) throws SetOptionException;
    public OptionValue validate(String name, SqlLiteral value) throws SetOptionException;
  }


}
