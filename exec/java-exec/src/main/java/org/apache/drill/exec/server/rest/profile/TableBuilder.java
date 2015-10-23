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
package org.apache.drill.exec.server.rest.profile;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

class TableBuilder {
  private final NumberFormat format = NumberFormat.getInstance(Locale.US);
  private final SimpleDateFormat days = new SimpleDateFormat("DD'd'hh'h'mm'm'");
  private final SimpleDateFormat sdays = new SimpleDateFormat("DD'd'hh'h'mm'm'");
  private final SimpleDateFormat hours = new SimpleDateFormat("HH'h'mm'm'");
  private final SimpleDateFormat shours = new SimpleDateFormat("H'h'mm'm'");
  private final SimpleDateFormat mins = new SimpleDateFormat("mm'm'ss's'");
  private final SimpleDateFormat smins = new SimpleDateFormat("m'm'ss's'");

  private final SimpleDateFormat secs = new SimpleDateFormat("ss.SSS's'");
  private final SimpleDateFormat ssecs = new SimpleDateFormat("s.SSS's'");
  private final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
  private final DecimalFormat dec = new DecimalFormat("0.00");
  private final DecimalFormat intformat = new DecimalFormat("#,###");

  private StringBuilder sb;
  private int w = 0;
  private int width;

  public TableBuilder(final String[] columns) {
    sb = new StringBuilder();
    width = columns.length;

    format.setMaximumFractionDigits(3);

    sb.append("<table class=\"table table-bordered text-right\">\n<tr>");
    for (final String cn : columns) {
      sb.append("<th>" + cn + "</th>");
    }
    sb.append("</tr>\n");
  }

  public void appendCell(final String s, final String link) {
    if (w == 0) {
      sb.append("<tr>");
    }
    sb.append(String.format("<td>%s%s</td>", s, link != null ? link : ""));
    if (++w >= width) {
      sb.append("</tr>\n");
      w = 0;
    }
  }

  public void appendRepeated(final String s, final String link, final int n) {
    for (int i = 0; i < n; i++) {
      appendCell(s, link);
    }
  }

  public void appendTime(final long d, final String link) {
    appendCell(dateFormat.format(d), link);
  }

  public void appendMillis(final long p, final String link) {
    final double secs = p/1000.0;
    final double mins = secs/60;
    final double hours = mins/60;
    final double days = hours / 24;
    SimpleDateFormat timeFormat = null;
    if (days >= 10) {
      timeFormat = this.days;
    } else if (days >= 1) {
      timeFormat = this.sdays;
    } else if (hours >= 10) {
      timeFormat = this.hours;
    }else if(hours >= 1){
      timeFormat = this.shours;
    }else if (mins >= 10){
      timeFormat = this.mins;
    }else if (mins >= 1){
      timeFormat = this.smins;
    }else if (secs >= 10){
      timeFormat = this.secs;
    }else {
      timeFormat = this.ssecs;
    }
    appendCell(timeFormat.format(new Date(p)), null);
  }

  public void appendNanos(final long p, final String link) {
    appendMillis(Math.round(p / 1000.0 / 1000.0), link);
  }

  public void appendFormattedNumber(final Number n, final String link) {
    appendCell(format.format(n), link);
  }

  public void appendFormattedInteger(final long n, final String link) {
    appendCell(intformat.format(n), link);
  }

  public void appendInteger(final long l, final String link) {
    appendCell(Long.toString(l), link);
  }

  public void appendBytes(final long l, final String link){
    appendCell(bytePrint(l), link);
  }

  private String bytePrint(final long size) {
    final double t = size / Math.pow(1024, 4);
    if (t > 1) {
      return dec.format(t).concat("TB");
    }

    final double g = size / Math.pow(1024, 3);
    if (g > 1) {
      return dec.format(g).concat("GB");
    }

    final double m = size / Math.pow(1024, 2);
    if (m > 1) {
      return intformat.format(m).concat("MB");
    }

    final double k = size / 1024;
    if (k >= 1) {
      return intformat.format(k).concat("KB");
    }

    // size < 1 KB
    return "-";
  }

  public String build() {
    String rv;
    rv = sb.append("\n</table>").toString();
    sb = null;
    return rv;
  }
}