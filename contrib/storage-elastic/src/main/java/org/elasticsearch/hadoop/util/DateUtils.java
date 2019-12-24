package org.elasticsearch.hadoop.util;

/**
 * Created by Administrator on 2016/4/11.
 */

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Calendar;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utility used for parsing date ISO8601.
 * Morphed into a runtime bridge over possible ISO8601 (simply because the spec is too large, especially when considering the various optional formats).
 */
public abstract class DateUtils {

    public static boolean printed = false;

    private final static boolean jodaTimeAvailable = ObjectUtils.isClassPresent("org.joda.time.format.ISODateTimeFormat", DateUtils.class.getClassLoader());

    private static abstract class Jdk6 {
        // Parses ISO date through the JDK XML bind class. However the spec doesn't support all ISO8601 formats which this class tries to address
        // in particular Time offsets from UTC are available in 3 forms:
        // The offset from UTC is appended to the time in the same way that 'Z' was above, in the form 盵hh]:[mm], 盵hh][mm], or 盵hh].
        //
        // XML Bind supports only the first one.
        public static Calendar parseDate(String value) {
            // check for colon in the time offset
            int timeZoneIndex = value.indexOf("T");
            if (timeZoneIndex > 0) {
                int sign = value.indexOf("+", timeZoneIndex);
                if (sign < 0) {
                    sign = value.indexOf("-", timeZoneIndex);
                }

                // +4 means it's either hh:mm or hhmm
                if (sign > 0) {
                    // +3 points to either : or m
                    int colonIndex = sign + 3;
                    // +hh - need to add :mm
                    if (colonIndex >= value.length()) {
                        value = value + ":00";
                    }
                    else if (value.charAt(colonIndex) != ':') {
                        value = value.substring(0, colonIndex) + ":" + value.substring(colonIndex);
                    }
                }
            }

            return DatatypeConverter.parseDateTime(value);
        }
    }

    private static abstract class JodaTime {

        private static final Object DATE_OPTIONAL_TIME_FORMATTER;
        private static final Method PARSE_DATE_TIME;
        private static final Method TO_CALENDAR;

        private static final boolean INITIALIZED;

        static {
            boolean init = false;
            Method parseDateTime = null, toCalendar = null;
            Object dotf = null;
            try {
                ClassLoader cl = JodaTime.class.getClassLoader();

                Class<?> FORMAT_CLASS = ObjectUtils.loadClass("org.joda.time.format.ISODateTimeFormat", cl);
                Method DATE_OPTIONAL_TIME = ReflectionUtils.findMethod(FORMAT_CLASS, "dateOptionalTimeParser");
                Object dotf0 = ReflectionUtils.invoke(DATE_OPTIONAL_TIME, null);
                
                //rewrite to support more format, start
                Class<?> FORMAT_BUILDER_CLASS = ObjectUtils.loadClass("org.joda.time.format.DateTimeFormatterBuilder", cl);
                Class<?> DATETIME_FORMATTER_CLASS = ObjectUtils.loadClass("org.joda.time.format.DateTimeFormatter", cl);
                Class<?> DATETIME_PARSER_CLASS = ObjectUtils.loadClass("org.joda.time.format.DateTimeParser", cl);
                Class<?> DATETIME_PRINTER_CLASS = ObjectUtils.loadClass("org.joda.time.format.DateTimePrinter", cl);
                Class<?> DATETIME_FORMAT_CLASS = ObjectUtils.loadClass("org.joda.time.format.DateTimeFormat", cl);
                
                Method forPattern = ReflectionUtils.findMethod(DATETIME_FORMAT_CLASS, "forPattern", String.class);
                Method getParser = ReflectionUtils.findMethod(DATETIME_FORMATTER_CLASS, "getParser");
                
                Object dotf1 = ReflectionUtils.invoke(forPattern, null, "yyyy-MM-dd HH:mm:ss"); 
                Object parser1 = ReflectionUtils.invoke(getParser, dotf1);   
                
                Object dotf2 = ReflectionUtils.invoke(forPattern, null, "yyyyMMdd");
                Object parser2 = ReflectionUtils.invoke(getParser, dotf2);
                
                Object dotf3 = ReflectionUtils.invoke(forPattern, null, "yyyyMMddHHmmZ");
                Object parser3 = ReflectionUtils.invoke(getParser, dotf3);
                
                Object parsers = Array.newInstance(DATETIME_PARSER_CLASS, 4);
                Array.set(parsers, 0, parser1);
                Array.set(parsers, 1, parser2);
                Array.set(parsers, 2, parser3);
                Array.set(parsers, 3, ReflectionUtils.invoke(getParser,dotf0));
                
                Object dateTimeFormatterBuilder = ObjectUtils.instantiate("org.joda.time.format.DateTimeFormatterBuilder", cl);
                Method append = ReflectionUtils.findMethod(FORMAT_BUILDER_CLASS, "append", DATETIME_PRINTER_CLASS, parsers.getClass());
                Method toFormatter = ReflectionUtils.findMethod(FORMAT_BUILDER_CLASS, "toFormatter");
                
                ReflectionUtils.invoke(append, dateTimeFormatterBuilder, null, parsers);
                dotf = ReflectionUtils.invoke(toFormatter, dateTimeFormatterBuilder);                
                //rewrite to support more format, end

                parseDateTime = ReflectionUtils.findMethod(dotf.getClass(), "parseDateTime", String.class);
                Class<?> DATE_TIME_CLASS = ObjectUtils.loadClass("org.joda.time.DateTime", cl);
                toCalendar = ReflectionUtils.findMethod(DATE_TIME_CLASS, "toGregorianCalendar");
                init = true;
            } catch (Exception ex) {
                // log exception
                ex.printStackTrace();
            }

            DATE_OPTIONAL_TIME_FORMATTER = dotf;
            PARSE_DATE_TIME = parseDateTime;
            TO_CALENDAR = toCalendar;
            INITIALIZED = init;
        }

        public static Calendar parseDate(String value) {
            Object dt = ReflectionUtils.invoke(PARSE_DATE_TIME, DATE_OPTIONAL_TIME_FORMATTER, value);
            return ReflectionUtils.invoke(TO_CALENDAR, dt);
        }
    }

    public static Calendar parseDate(String value) {
        if (!printed) {
            printed = true;
            Log log = LogFactory.getLog(DateUtils.class);
            if (jodaTimeAvailable && JodaTime.INITIALIZED) {
                log.info("Joda library available in the classpath; using it for date/time handling...");
            }
            else {
                // be silent otherwise
            }
        }

        return (jodaTimeAvailable && JodaTime.INITIALIZED) ? JodaTime.parseDate(value) : Jdk6.parseDate(value);
    }
}