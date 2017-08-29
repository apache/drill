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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.store.sys.PersistentStore;
import com.google.common.base.Preconditions;

/**
 * <p>
 * An {@link OptionValue option value} is used by an {@link OptionManager} to store a run-time setting. This setting,
 * for example, could affect a query in execution stage. Instances of this class are JSON serializable and can be stored
 * in a {@link PersistentStore persistent store} (see {@link SystemOptionManager#options}), or
 * in memory (see {@link InMemoryOptionManager#options}).
 * </p>
 *<p>  OptionType defines the permission level of the option.If it can be set at System level or Session level or so on.
 * Whereas OptionScope defines the scope at which the option is being set. If the option is being set at the BOOT time
 * the scope of the option is BOOT. If it is set at SYSTEM level the scope is SYSTEM. Although they look similar there
 * is a fine level which differentiates both of them which is at which level of hierarchy they can be set and
 * at what at level of hierarchy they were actually set.
 * </p>
 */
@JsonInclude(Include.NON_NULL)
public class OptionValue implements Comparable<OptionValue> {

  public enum OptionType {
    BOOT, SYSTEM, SESSION, QUERY
  }

  public enum Kind {
    BOOLEAN, LONG, STRING, DOUBLE
  }

  public enum OptionScope {
    BOOT, SYSTEM, SESSION, QUERY
  }

  public final String name;
  public final Kind kind;
  public final OptionType type;
  public final Long num_val;
  public final String string_val;
  public final Boolean bool_val;
  public final Double float_val;
  public final OptionScope scope;

  public static OptionValue createLong(OptionType type, String name, long val, OptionScope scope) {
    return new OptionValue(Kind.LONG, type, name, val, null, null, null, scope);
  }

  public static OptionValue createBoolean(OptionType type, String name, boolean bool, OptionScope scope) {
    return new OptionValue(Kind.BOOLEAN, type, name, null, null, bool, null, scope);
  }

  public static OptionValue createString(OptionType type, String name, String val, OptionScope scope) {
    return new OptionValue(Kind.STRING, type, name, null, val, null, null, scope);
  }

  public static OptionValue createDouble(OptionType type, String name, double val, OptionScope scope) {
    return new OptionValue(Kind.DOUBLE, type, name, null, null, null, val, scope);
  }

  public static OptionValue createOption(Kind kind, OptionType type, String name, String val, OptionScope scope) {
    switch (kind) {
      case BOOLEAN:
        return createBoolean(type, name, Boolean.valueOf(val), scope);
      case LONG:
        return createLong(type, name, Long.valueOf(val), scope);
      case STRING:
        return createString(type, name, val, scope);
      case DOUBLE:
        return createDouble(type, name, Double.valueOf(val), scope);
      default:
        return null;
    }
  }

  @JsonCreator
  private OptionValue(@JsonProperty("kind") Kind kind,
                      @JsonProperty("type") OptionType type,
                      @JsonProperty("name") String name,
                      @JsonProperty("num_val") Long num_val,
                      @JsonProperty("string_val") String string_val,
                      @JsonProperty("bool_val") Boolean bool_val,
                      @JsonProperty("float_val") Double float_val,
                      @JsonProperty("scope") OptionScope scope) {
    Preconditions.checkArgument(num_val != null || string_val != null || bool_val != null || float_val != null);
    this.kind = kind;
    this.type = type;
    this.name = name;
    this.float_val = float_val;
    this.num_val = num_val;
    this.string_val = string_val;
    this.bool_val = bool_val;
    this.scope = scope;
  }

  public String getName() {
    return name;
  }

  @JsonIgnore
  public Object getValue() {
    switch (kind) {
      case BOOLEAN:
        return bool_val;
      case LONG:
        return num_val;
      case STRING:
        return string_val;
      case DOUBLE:
        return float_val;
      default:
        return null;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((bool_val == null) ? 0 : bool_val.hashCode());
    result = prime * result + ((float_val == null) ? 0 : float_val.hashCode());
    result = prime * result + ((kind == null) ? 0 : kind.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((num_val == null) ? 0 : num_val.hashCode());
    result = prime * result + ((string_val == null) ? 0 : string_val.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  public boolean equalsIgnoreType(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OptionValue other = (OptionValue) obj;
    if (bool_val == null) {
      if (other.bool_val != null) {
        return false;
      }
    } else if (!bool_val.equals(other.bool_val)) {
      return false;
    }
    if (float_val == null) {
      if (other.float_val != null) {
        return false;
      }
    } else if (!float_val.equals(other.float_val)) {
      return false;
    }
    if (kind != other.kind) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (num_val == null) {
      if (other.num_val != null) {
        return false;
      }
    } else if (!num_val.equals(other.num_val)) {
      return false;
    }
    if (string_val == null) {
      if (other.string_val != null) {
        return false;
      }
    } else if (!string_val.equals(other.string_val)) {
      return false;
    }
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (!equalsIgnoreType(obj)) {
      return false;
    }
    final OptionValue other = (OptionValue) obj;
    return type == other.type;
  }

  @Override
  public int compareTo(OptionValue o) {
    return this.name.compareTo(o.name);
  }

  @Override
  public String toString() {
    return "OptionValue [ type=" + type + ", name=" + name + ", value=" + getValue() + " ]";
  }
}
