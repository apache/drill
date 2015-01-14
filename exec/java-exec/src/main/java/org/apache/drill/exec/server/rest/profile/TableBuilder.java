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
  NumberFormat format = NumberFormat.getInstance(Locale.US);
  SimpleDateFormat hours = new SimpleDateFormat("HH:mm");
  SimpleDateFormat shours = new SimpleDateFormat("H:mm");
  SimpleDateFormat mins = new SimpleDateFormat("mm:ss");
  SimpleDateFormat smins = new SimpleDateFormat("m:ss");

  SimpleDateFormat secs = new SimpleDateFormat("ss.SSS");
  SimpleDateFormat ssecs = new SimpleDateFormat("s.SSS");
  DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
  DecimalFormat dec = new DecimalFormat("0.00");
  DecimalFormat intformat = new DecimalFormat("#,###");

  StringBuilder sb;
  int w = 0;
  int width;

  public TableBuilder(String[] columns) {
    sb = new StringBuilder();
    width = columns.length;

    format.setMaximumFractionDigits(3);
    format.setMinimumFractionDigits(3);

    sb.append("<table class=\"table table-bordered text-right\">\n<tr>");
    for (String cn : columns) {
      sb.append("<th>" + cn + "</th>");
    }
    sb.append("</tr>\n");
  }

  public void appendCell(String s, String link) {
    if (w == 0) {
      sb.append("<tr>");
    }
    sb.append(String.format("<td>%s%s</td>", s, link != null ? link : ""));
    if (++w >= width) {
      sb.append("</tr>\n");
      w = 0;
    }
  }

  public void appendRepeated(String s, String link, int n) {
    for (int i = 0; i < n; i++) {
      appendCell(s, link);
    }
  }

  public void appendTime(long d, String link) {
    appendCell(dateFormat.format(d), link);
  }

  public void appendMillis(long p, String link) {
    double secs = p/1000.0;
    double mins = secs/60;
    double hours = mins/60;
    SimpleDateFormat timeFormat = null;
    if(hours >= 10){
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

  public void appendNanos(long p, String link) {
    appendMillis((long) (p / 1000.0 / 1000.0), link);
  }

  public void appendFormattedNumber(Number n, String link) {
    appendCell(format.format(n), link);
  }

  public void appendFormattedInteger(long n, String link) {
    appendCell(intformat.format(n), link);
  }

  public void appendInteger(long l, String link) {
    appendCell(Long.toString(l), link);
  }

  public void appendBytes(long l, String link){
    appendCell(bytePrint(l), link);
  }

  private String bytePrint(long size){
    double m = size/Math.pow(1024, 2);
    double g = size/Math.pow(1024, 3);
    double t = size/Math.pow(1024, 4);
    if (t > 1) {
      return dec.format(t).concat("TB");
    } else if (g > 1) {
      return dec.format(g).concat("GB");
    } else if (m > 1){
      return intformat.format(m).concat("MB");
    } else {
      return "-";
    }
  }

  @Override
  public String toString() {
    String rv;
    rv = sb.append("\n</table>").toString();
    sb = null;
    return rv;
  }
}