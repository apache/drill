/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.serialization.field;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.elasticsearch.hadoop.util.Constants;
import org.elasticsearch.hadoop.util.DateUtils;
import org.elasticsearch.hadoop.util.StringUtils;


public class DateIndexFormatter implements IndexFormatter {

    private String format;
    private SimpleDateFormat dateFormat;

    @Override
    public void configure(String format) {
        this.format = format;
        format = fixDateForJdk(format);
        this.dateFormat = new SimpleDateFormat(format);
    }

    /**
     * Work-around for year formatting in JDK 6 vs 7+.
     * JDK6: does not know about 'YYYY' (only 'yyyy'). Considers 'y' and 'yyy' as 'yy'.
     * JDK7: understands both 'YYYY' and 'yyyy'. Considers 'y' and 'yyy' as 'yyyy'
     *
     * This method checks the pattern and converts it into JDK7 when running on JDK6.
     */
    private String fixDateForJdk(String format) {
        if (Constants.JRE_IS_MINIMUM_JAVA7) {
            return format;
        }
        // JDK 6 - fix year formatting

        // a. lower case Y to y
        if (format.contains("Y")) {
            format = format.replace("Y", "y");
        }

        // gotta love regex
        // use lookahead to match isolated y/yyy with yyyy
        format = format.replaceAll("((?<!y)(?:y|yyy)(?!y))", "yyyy");

        return format;
    }

    @Override
    public String format(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }

        Calendar calendar = DateUtils.parseDate(value);
        dateFormat.setCalendar(calendar);
        return dateFormat.format(calendar.getTime());
    }
}
