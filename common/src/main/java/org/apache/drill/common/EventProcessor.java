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

import java.util.LinkedList;

/**
 * Process events serially.
 *
 * <p>Our use of listeners that deliver events directly can sometimes
 * cause problems when events are delivered recursively in the middle of
 * event handling by the same object. This helper class can be used to
 * serialize events in such cases. If an event is being processed, arriving
 * events are queued. Once the current event handling is completed, the
 * next event on the queue is processed; this continues until the event
 * queue is empty. The first thread to arrive will process its own event
 * and all other events that arrive during that processing. Other threads
 * will just enqueue their events.</p>
 *
 * @param <T> the event class
 */
public abstract class EventProcessor<T> {
  private final LinkedList<T> queuedEvents = new LinkedList<>();
  private volatile boolean isProcessing = false;

  /**
   * Constructor.
   */
  public EventProcessor() {
  }

  /**
   * Send an event to the processor. If the processor is not busy, the event
   * will be processed. If busy, the event will be queued to be processed after
   * any prior events are processed.
   *
   * <p>If an event's processing causes an exception, it will be added to any
   * previous exceptions as a suppressed exception. Once all the currently queued
   * events have been processed, a single exception will be thrown.</p>
   *
   * @param newEvent the new event
   */
  public void sendEvent(final T newEvent) {
    synchronized (queuedEvents) {
      if (isProcessing) {
        queuedEvents.addLast(newEvent);
        return;
      }

      isProcessing = true;
    }

    @SuppressWarnings("resource")
    final DeferredException deferredException = new DeferredException();
    T event = newEvent;
    while (true) {
      try {
        processEvent(event);
      } catch (Exception e) {
        deferredException.addException(e);
      } catch (AssertionError ae) {
        deferredException.addException(new RuntimeException("Caught an assertion", ae));
      }

      synchronized (queuedEvents) {
        if (queuedEvents.isEmpty()) {
          isProcessing = false;
          break;
        }

        event = queuedEvents.removeFirst();
      }
    }

    try {
      deferredException.close();
    } catch(Exception e) {
      throw new RuntimeException("Exceptions caught during event processing", e);
    }
  }

  /**
   * Process a single event. Derived classes provide the implementation of this
   * to process events.
   *
   * @param event the event to process
   */
  protected abstract void processEvent(T event);
}
