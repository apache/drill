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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class TableBuilder {
  private final NumberFormat format = NumberFormat.getInstance(Locale.US);
  private final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
  private final DecimalFormat dec = new DecimalFormat("0.00");
  private final DecimalFormat intformat = new DecimalFormat("#,###");

  private StringBuilder sb;
  private int w = 0;
  private int width;

  public TableBuilder(final String[] columns, final String[] columnTooltip) {
    this(columns, columnTooltip, false);
  }

  public TableBuilder(final String[] columns, final String[] columnTooltip, final boolean isSortable) {
    sb = new StringBuilder();
    width = columns.length;

    format.setMaximumFractionDigits(3);

    sb.append("<table class=\"table table-bordered text-right"+(isSortable? " sortable" : "")+"\">\n<thead><tr>");
    for (int i = 0; i < columns.length; i++) {
      String cn = columns[i];
      String ctt = "";
      if (columnTooltip != null) {
        String tooltip = columnTooltip[i];
        if (tooltip != null) {
          ctt = " title=\""+tooltip+"\"";
        }
      }
      sb.append("<th" + ctt + ">" + cn + "</th>");
    }
    sb.append("</tr></thead>\n<tbody>\n");
  }

  public void appendCell(final String s) {
    appendCell(s, null, null, null);
  }

  public void appendCell(final String s, final String link) {
    appendCell(s, link, null, null);
  }

  public void appendCell(final String s, final String link, final String titleText) {
    appendCell(s, link, titleText, null);
  }

  public void appendCell(final String s, final String link, final String titleText, final String backgroundColor) {
    appendCell(s, link, titleText, backgroundColor, null);
  }

  public void appendCell(final String s, final Map<String, String> kvPairs) {
    appendCell(s, null, null, null, kvPairs);
  }

  public void appendCell(final String s, final String link, final String titleText, final String backgroundColor,
      final Map<String, String> kvPairs) {
    if (w == 0) {
      sb.append("<tr"
          + (backgroundColor == null ? "" : " style=\"background-color:"+backgroundColor+"\"")
          + ">");
    }
    StringBuilder tdElemSB = new StringBuilder("<td");
    //Injecting title if specified (legacy impl)
    if (titleText != null && titleText.length() > 0) {
      tdElemSB.append(" title=\""+titleText+"\"");
    }
    //Extract other attributes for injection into element
    if (kvPairs != null) {
      for (String attributeName : kvPairs.keySet()) {
        String attributeText = " " + attributeName + "=\"" + kvPairs.get(attributeName) + "\"";
        tdElemSB.append(attributeText);
      }
    }
    //Closing <td>
    tdElemSB.append(String.format(">%s%s</td>", s, link != null ? link : ""));
    sb.append(tdElemSB);
    if (++w >= width) {
      sb.append("</tr>\n");
      w = 0;
    }
  }

  public void appendRepeated(final String s, final String link, final int n) {
    appendRepeated(s, link, n, null);
  }

  public void appendRepeated(final String s, final String link, final int n, final String tooltip) {
    for (int i = 0; i < n; i++) {
      appendCell(s, link, tooltip);
    }
  }

  public void appendTime(final long d) {
    appendTime(d, null);
  }

  public void appendTime(final long d, final String link) {
    appendTime(d, link, null);
  }

  public void appendTime(final long d, final String link, final String tooltip) {
    //Embedding dataTable's data-order attribute
    Map<String, String> attributeMap = new HashMap<String, String>();
    attributeMap.put("data-order", String.valueOf(d));
    appendCell(dateFormat.format(d), link, tooltip, null, attributeMap);
  }

  public void appendMillis(final long p) {
    appendMillis(p, null);
  }

  public void appendMillis(final long p, final String link) {
    appendMillis(p, link, null);
  }

  public void appendMillis(final long p, final String link, final String tooltip) {
    //Embedding dataTable's data-order attribute
    Map<String, String> attributeMap = new HashMap<String, String>();
    attributeMap.put("data-order", String.valueOf(p));
    appendCell((new SimpleDurationFormat(0, p)).compact(), link, tooltip, null, attributeMap);
  }

  public void appendNanos(final long p) {
    appendNanos(p, null, null);
  }

  public void appendNanos(final long p, final String link) {
    appendNanos(p, link, null);
  }

  public void appendNanos(final long p, final String link, final String tooltip) {
    appendMillis(Math.round(p / 1000.0 / 1000.0), link, tooltip);
  }

  public void appendPercent(final double percentAsFraction) {
    appendCell(dec.format(100*percentAsFraction).concat("%"), null, null);
  }

  public void appendPercent(final double percentAsFraction, final String link) {
    appendCell(dec.format(100*percentAsFraction).concat("%"), link, null);
  }

  public void appendPercent(final double percentAsFraction, final String link, final String tooltip) {
    appendCell(dec.format(100*percentAsFraction).concat("%"), link, tooltip);
  }

  public void appendFormattedNumber(final Number n) {
    appendCell(format.format(n), null, null);
  }

  public void appendFormattedNumber(final Number n, final String link) {
    appendCell(format.format(n), link, null);
  }

  public void appendFormattedNumber(final Number n, final String link, final String tooltip) {
    appendCell(format.format(n), link, tooltip);
  }

  public void appendFormattedInteger(final long n) {
    appendCell(intformat.format(n), null, null);
  }

  public void appendFormattedInteger(final long n, final String link) {
    appendCell(intformat.format(n), link, null);
  }

  public void appendFormattedInteger(final long n, final String link, final String tooltip) {
    appendCell(intformat.format(n), link, tooltip);
  }

  public void appendInteger(final long l, final String link, final String tooltip) {
    appendCell(Long.toString(l), link, tooltip);
  }

  public void appendBytes(final long l) {
    appendBytes(l, null);
  }

  public void appendBytes(final long l, final String link) {
    appendBytes(l, link, null);
  }

  public void appendBytes(final long l, final String link, final String tooltip) {
    //Embedding dataTable's data-order attribute
    Map<String, String> attributeMap = new HashMap<String, String>();
    attributeMap.put("data-order", String.valueOf(l));
    appendCell(bytePrint(l), link, tooltip, null, attributeMap);
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
    rv = sb.append("\n</tbody>\n</table>").toString();
    sb = null;
    return rv;
  }
}