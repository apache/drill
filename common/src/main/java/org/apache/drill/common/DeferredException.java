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
package org.apache.drill.common;

import com.google.common.base.Preconditions;

/**
 * Collects one or more exceptions that may occur, using
 * <a href="http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html#suppressed-exceptions">
 * suppressed exceptions</a>.
 * When this AutoCloseable is closed, if there was an exception added, it will be thrown. If more than one
 * exception was added, then all but the first will be added to the first as suppressed
 * exceptions.
 *
 * <p>This class is thread safe.
 */
public class DeferredException implements AutoCloseable {
  private Exception exception = null;
  private boolean isClosed = false;

  /**
   * Add an exception. If this is the first exception added, it will be the one
   * that is thrown when this is closed. If not the first exception, then it will
   * be added to the suppressed exceptions on the first exception.
   *
   * @param exception the exception to add
   */
  public void addException(final Exception exception) {
    Preconditions.checkNotNull(exception);

    synchronized(this) {
      Preconditions.checkState(!isClosed);

      if (this.exception == null) {
        this.exception = exception;
      } else {
        this.exception.addSuppressed(exception);
      }
    }
  }

  public void addThrowable(final Throwable throwable) {
    Preconditions.checkNotNull(throwable);

    if (throwable instanceof Exception) {
      addException((Exception) throwable);
      return;
    }

    addException(new RuntimeException(throwable));
  }

  /**
   * Get the deferred exception, if there is one. Note that if this returns null,
   * the result could change at any time.
   *
   * @return the deferred exception, or null
   */
  public synchronized Exception getException() {
    return exception;
  }

  public synchronized Exception getAndClear() {
    Preconditions.checkState(!isClosed);

    if (exception != null) {
      final Exception local = exception;
      exception = null;
      return local;
    }

    return null;
  }

  /**
   * If an exception exists, will throw the exception and then clear it. This is so in cases where want to reuse
   * DeferredException, we don't double report the same exception.
   *
   * @throws Exception
   */
  public synchronized void throwAndClear() throws Exception{
    final Exception e = getAndClear();
    if (e != null) {
      throw e;
    }
  }

  /**
   * Close the given AutoCloseable, suppressing any exceptions that are thrown.
   * If an exception is thrown, the rules for {@link #addException(Exception)}
   * are followed.
   *
   * @param autoCloseable the AutoCloseable to close; may be null
   */
  public void suppressingClose(final AutoCloseable autoCloseable) {
    synchronized(this) {
      /*
       * For the sake of detecting code that doesn't follow the conventions,
       * we want this to complain whether the closeable exists or not.
       */
      Preconditions.checkState(!isClosed);

      if (autoCloseable == null) {
        return;
      }

      try {
        autoCloseable.close();
      } catch(final Exception e) {
        addException(e);
      }
    }
  }

  @Override
  public synchronized void close() throws Exception {
    try {
      throwAndClear();
    } finally {
      isClosed = true;
    }
  }
}
