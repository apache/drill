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

import org.slf4j.Logger;

/**
 * Utility class that can be used to log activity within a class
 * for later logging and debugging. Supports recording events and
 * recording the stack at the time they occur.
 */
public class HistoricalLog {
  private static class Event {
    private final String note; // the event text
    private final StackTrace stackTrace; // where the event occurred

    public Event(final String note) {
      this.note = note;
      stackTrace = new StackTrace();
    }
  }

  private final LinkedList<Event> history = new LinkedList<>();
  private final String idString; // the formatted id string
  private Event firstEvent; // the first stack trace recorded
  private final int limit; // the limit on the number of events kept
  private final Logger logger; // the logger to use when direct logging

  private final static boolean DIRECT_LOGGING = false;

  /**
   * Constructor. The format string will be formatted and have its arguments
   * substituted at the time this is called.
   *
   * @param idStringFormat {@link String#format} format string that can be used
   *     to identify this object in a log. Including some kind of unique identifier
   *     that can be associated with the object instance is best.
   * @param args for the format string, or nothing if none are required
   */
  public HistoricalLog(final Logger logger, final String idStringFormat, Object... args) {
    this(logger, Integer.MAX_VALUE, idStringFormat, args);
  }

  /**
   * Constructor. The format string will be formatted and have its arguments
   * substituted at the time this is called.
   *
   * <p>This form supports the specification of a limit that will limit the
   * number of historical entries kept (which keeps down the amount of memory
   * used). With the limit, the first entry made is always kept (under the
   * assumption that this is the creation site of the object, which is usually
   * interesting), and then up to the limit number of entries are kept after that.
   * Each time a new entry is made, the oldest that is not the first is dropped.
   * </p>
   *
   * @param limit the maximum number of historical entries that will be kept,
   *   not including the first entry made
   * @param idStringFormat {@link String#format} format string that can be used
   *     to identify this object in a log. Including some kind of unique identifier
   *     that can be associated with the object instance is best.
   * @param args for the format string, or nothing if none are required
   */
  public HistoricalLog(final Logger logger, final int limit,
      final String idStringFormat, Object... args) {
    this.limit = limit;
    this.idString = String.format(idStringFormat, args);
    this.logger = logger;
  }

  /**
   * Record an event. Automatically captures the stack trace at the time this is
   * called. The format string will be formatted and have its arguments substituted
   * at the time this is called.
   *
   * @param noteFormat {@link String#format} format string that describes the event
   * @param args for the format string, or nothing if none are required
   */
  public synchronized void recordEvent(final String noteFormat, Object... args) {
    final String note = String.format(noteFormat, args);
    addEvent(new Event(note));
  }

  private void addEvent(final Event event) {
    if (DIRECT_LOGGING) {
      final StringBuilder sb = new StringBuilder();
      sb.append(idString);
      sb.append(": ");
      sb.append(event.note);
      sb.append('\n');
      event.stackTrace.writeToBuilder(sb, 4, 12);
      logger.debug(sb.toString());
      return;
    }

    if (firstEvent == null) {
      firstEvent = event;
    }
    if (history.size() == limit) {
      history.removeFirst();
    }
    history.add(event);
  }

  public synchronized void append(final HistoricalLog otherLog) {
    synchronized(otherLog) {
      for(Event event : otherLog.history) {
        addEvent(event);
      }
    }
  }

  /**
   * Write the history of this object to the given {@link StringBuilder}. The history
   * includes the identifying string provided at construction time, and all the recorded
   * events with their stack traces.
   *
   * @param sb {@link StringBuilder} to write to
   */
  public void buildHistory(final StringBuilder sb) {
    buildHistory(sb, null);
  }

  /**
   * Write the history of this object to the given {@link StringBuilder}. The history
   * includes the identifying string provided at construction time, and all the recorded
   * events with their stack traces.
   *
   * @param sb {@link StringBuilder} to write to
   * @param additional an extra string that will be written between the identifying
   *     information and the history; often used for a current piece of state
   */
  public synchronized void buildHistory(final StringBuilder sb, final CharSequence additional) {
    sb.append('\n')
        .append(idString);

    if (additional != null) {
      sb.append('\n')
          .append(additional)
          .append('\n');
    }

    sb.append(" event log\n");

    if (firstEvent != null) {
      sb.append("  ")
          .append(firstEvent.note)
          .append('\n');
      firstEvent.stackTrace.writeToBuilder(sb, 4);

      for(final Event event : history) {
        if (event == firstEvent) {
          continue;
        }

        sb.append("  ")
            .append(event.note)
            .append('\n');

        event.stackTrace.writeToBuilder(sb, 4);
      }
    }
  }

  /**
   * Write the history of this object to the given {@link Logger}. The history
   * includes the identifying string provided at construction time, and all the recorded
   * events with their stack traces.
   *
   * @param logger {@link Logger} to write to
   * @param additional an extra string that will be written between the identifying
   *     information and the history; often used for a current piece of state
   */
  public void logHistory(final Logger logger, final CharSequence additional) {
    final StringBuilder sb = new StringBuilder();
    buildHistory(sb, additional);
    logger.debug(sb.toString());
  }

  /**
   * Write the history of this object to the given {@link Logger}. The history
   * includes the identifying string provided at construction time, and all the recorded
   * events with their stack traces.
   *
   * @param logger {@link Logger} to write to
   */
  public void logHistory(final Logger logger) {
    logHistory(logger, null);
  }
}
