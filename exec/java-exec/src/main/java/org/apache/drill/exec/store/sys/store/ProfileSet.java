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
package org.apache.drill.exec.store.sys.store;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wrapper around TreeSet to mimic a size-bound set ordered by name (implicitly the profiles' age)
 */
public class ProfileSet implements Iterable<String> {
  private TreeSet<String> store;
  private int maxCapacity;
  //Using a dedicated counter to avoid
  private AtomicInteger size;

  @SuppressWarnings("unused")
  @Deprecated
  private ProfileSet() {}

  public ProfileSet(int capacity) {
    this.store = new TreeSet<String>();
    this.maxCapacity = capacity;
    this.size = new AtomicInteger();
  }

  public int size() {
    return size.get();
  }

  /**
   * Get max capacity of the profile set
   * @return max capacity
   */
  public int capacity() {
    return maxCapacity;
  }

  /**
   * Add a profile name to the set, while removing the oldest, if exceeding capacity
   * @param profile
   * @return oldest profile
   */
  public String add(String profile) {
    return add(profile, false);
  }

  /**
   * Add a profile name to the set, while removing the oldest or youngest, based on flag
   * @param profile
   * @param retainOldest indicate retaining policy as oldest
   * @return youngest/oldest profile
   */
  public String add(String profile, boolean retainOldest) {
    store.add(profile);
    if (size.incrementAndGet() > maxCapacity) {
      if (retainOldest) {
        return removeYoungest();
      } else {
        return removeOldest();
      }
    }
    return null;
  }

  /**
   * Remove the oldest profile
   * @return oldest profile
   */
  public String removeOldest() {
    size.decrementAndGet();
    return store.pollLast();
  }

  /**
   * Remove the youngest profile
   * @return youngest profile
   */
  public String removeYoungest() {
    size.decrementAndGet();
    return store.pollFirst();
  }

  /**
   * Retrieve the oldest profile without removal
   * @return oldest profile
   */
  public String getOldest() {
    return store.last();
  }

  /**
   * Retrieve the youngest profile without removal
   * @return youngest profile
   */
  public String getYoungest() {
    return store.first();
  }

  /**
   * Clear the set
   */
  public void clear() {
    size.set(0);
    store.clear();
  }

  /**
   * Clear the set with the initial capacity
   * @param capacity
   */
  public void clear(int capacity) {
    clear(maxCapacity, false);
  }

  /**
   * Clear the set with the initial capacity
   * @param capacity
   * @param forceResize
   */
  public void clear(int capacity, boolean forceResize) {
    clear();
    if (forceResize || capacity > maxCapacity) {
      maxCapacity = capacity;
    }
  }

  public boolean isEmpty() {
    return store.isEmpty();
  }

  @Override
  public Iterator<String> iterator() {
    return store.iterator();
  }
}