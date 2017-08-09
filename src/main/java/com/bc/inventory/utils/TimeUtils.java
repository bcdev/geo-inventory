/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.inventory.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

public class TimeUtils {

    private static final int MINUTES_PER_MILLI = 60 * 1000;
    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    public static Date parse(String text, String pattern) throws ParseException {
        final DateFormat dateFormat = createDateFormat(pattern);
        String noFractionString = getNoFractionString(text);
        return dateFormat.parse(noFractionString);
    }

    public static String getNoFractionString(String text) {
        final int dotPos = text.lastIndexOf(".");
        String noFractionString = text;
        if (dotPos > 0) {
            noFractionString = text.substring(0, dotPos);
        }
        return noFractionString;
    }


    public static DateFormat createDateFormat(String pattern) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(pattern, Locale.ENGLISH);
        dateFormat.setCalendar(createCalendar());
        return dateFormat;
    }

    public static Calendar createCalendar() {
        final Calendar calendar = GregorianCalendar.getInstance(UTC_TIME_ZONE, Locale.ENGLISH);
        calendar.clear();
        calendar.set(2000, 0, 1);
        return calendar;
    }
    
    public static int startTimeInMin(long startTime) {
        if (startTime == -1) {
            return -1;
        }
        return (int) Math.floor((double)startTime / (double)MINUTES_PER_MILLI);
    }

    public static int endTimeInMin(long endTime) {
        if (endTime == -1) {
            return -1;
        }
        return (int) Math.ceil((double)endTime / (double)MINUTES_PER_MILLI);
    }
    
    public static Date minuteTimeAsDate(int minuteTime) {
        return new Date((long)minuteTime * (long)MINUTES_PER_MILLI);
    }
}
