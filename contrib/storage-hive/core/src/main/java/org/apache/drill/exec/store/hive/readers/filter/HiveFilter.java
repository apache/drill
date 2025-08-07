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
package org.apache.drill.exec.store.hive.readers.filter;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;
/**
 * Primary interface for <a href="http://en.wikipedia.org/wiki/Sargable">
 *   SearchArgument</a>, which are the subset of predicates
 * that can be pushed down to the RecordReader. Each SearchArgument consists
 * of a series of SearchClauses that must each be true for the row to be
 * accepted by the filter.
 *
 * This requires that the filter be normalized into conjunctive normal form
 * (<a href="http://en.wikipedia.org/wiki/Conjunctive_normal_form">CNF</a>).
 */
public class HiveFilter {

  private final SearchArgument searchArgument;
  private final static ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
     protected Kryo initialValue() { return new Kryo(); }
  };

  public HiveFilter(SearchArgument searchArgument) {
    this.searchArgument = searchArgument;
  }

  private static String toKryo(SearchArgument sarg) {
    Output out = new Output(4 * 1024, 10 * 1024 * 1024);
    new Kryo().writeObject(out, sarg);
    out.close();
    return Base64.encodeBase64String(out.toBytes());
  }

  @VisibleForTesting
  public static SearchArgument create(String kryo) {
    return create(Base64.decodeBase64(kryo));
  }

  private static SearchArgument create(byte[] kryoBytes) {
    return kryo.get().readObject(new Input(kryoBytes), SearchArgumentImpl.class);
  }

  public SearchArgument getSearchArgument() {
    return searchArgument;
  }

  public String getSearchArgumentString() {
    return toKryo(searchArgument);
  }
}
