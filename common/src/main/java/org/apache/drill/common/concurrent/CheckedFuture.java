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
package org.apache.drill.common.concurrent;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@code CheckedFuture} is a {@link ListenableFuture} that includes versions of the {@code get}
 * methods that can throw a checked exception. This makes it easier to create a future that executes
 * logic which can throw an exception.
 *
 * <p>Implementations of this interface must adapt the exceptions thrown by {@code Future#get()}:
 * {@link CancellationException}, {@link ExecutionException} and {@link InterruptedException} into
 * the type specified by the {@code X} type parameter.
 *
 * <p>This interface also extends the ListenableFuture interface to allow listeners to be added.
 * This allows the future to be used as a normal {@link Future} or as an asynchronous callback
 * mechanism as needed. This allows multiple callbacks to be registered for a particular task, and
 * the future will guarantee execution of all listeners when the task completes.
 *
 * <p>For a simpler alternative to CheckedFuture, consider accessing Future values with {@link
 * Futures#getChecked(Future, Class) Futures.getChecked()}.
 *
 * This interface is moved from Guava, since there it was deleted.
 *
 */
public interface CheckedFuture<V, X extends Exception> extends ListenableFuture<V> {

  /**
   * Exception checking version of {@link Future#get()} that will translate {@link
   * InterruptedException}, {@link CancellationException} and {@link ExecutionException} into
   * application-specific exceptions.
   *
   * @return the result of executing the future.
   * @throws X on interruption, cancellation or execution exceptions.
   */
  V checkedGet() throws X;

  /**
   * Exception checking version of {@link Future#get(long, TimeUnit)} that will translate {@link
   * InterruptedException}, {@link CancellationException} and {@link ExecutionException} into
   * application-specific exceptions.  On timeout this method throws a normal {@link
   * TimeoutException}.
   *
   * @return the result of executing the future.
   * @throws TimeoutException if retrieving the result timed out.
   * @throws X on interruption, cancellation or execution exceptions.
   */
  V checkedGet(long timeout, TimeUnit unit) throws TimeoutException, X;
}
