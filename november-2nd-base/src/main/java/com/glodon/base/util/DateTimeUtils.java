package com.glodon.base.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import com.glodon.base.value.*;
import com.glodon.base.exceptions.UnificationException;

public class DateTimeUtils {

    public static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    private static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1000000;

    private static final int SHIFT_YEAR = 9;
    private static final int SHIFT_MONTH = 5;

    private static final int[] NORMAL_DAYS_PER_MONTH = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30,
            31};

    private static final int[] DAYS_OFFSET = {0, 31, 61, 92, 122, 153, 184, 214, 245, 275, 306, 337,
            366};

    private static int zoneOffset;
    private static Calendar cachedCalendar;

    private DateTimeUtils() {
    }

    static {
        getCalendar();
    }

    public static void resetCalendar() {
        cachedCalendar = null;
        getCalendar();
    }

    private static Calendar getCalendar() {
        if (cachedCalendar == null) {
            cachedCalendar = Calendar.getInstance();
            zoneOffset = cachedCalendar.get(Calendar.ZONE_OFFSET);
        }
        return cachedCalendar;
    }

    private static void convertTime(Calendar from, Calendar to) {
        to.set(Calendar.ERA, from.get(Calendar.ERA));
        to.set(Calendar.YEAR, from.get(Calendar.YEAR));
        to.set(Calendar.MONTH, from.get(Calendar.MONTH));
        to.set(Calendar.DAY_OF_MONTH, from.get(Calendar.DAY_OF_MONTH));
        to.set(Calendar.HOUR_OF_DAY, from.get(Calendar.HOUR_OF_DAY));
        to.set(Calendar.MINUTE, from.get(Calendar.MINUTE));
        to.set(Calendar.SECOND, from.get(Calendar.SECOND));
        to.set(Calendar.MILLISECOND, from.get(Calendar.MILLISECOND));
    }

    public static long parseDateValue(String s, int start, int end) {
        if (s.charAt(start) == '+') {
            start++;
        }
        int s1 = s.indexOf('-', start + 1);
        int s2 = s.indexOf('-', s1 + 1);
        if (s1 <= 0 || s2 <= s1) {
            throw new IllegalArgumentException(s);
        }
        int year = Integer.parseInt(s.substring(start, s1));
        int month = Integer.parseInt(s.substring(s1 + 1, s2));
        int day = Integer.parseInt(s.substring(s2 + 1, end));
        if (!isValidDate(year, month, day)) {
            throw new IllegalArgumentException(year + "-" + month + "-" + day);
        }
        return dateValue(year, month, day);
    }

    public static long parseTimeNanos(String s, int start, int end, boolean timeOfDay) {
        int hour = 0, minute = 0, second = 0;
        long nanos = 0;
        int s1 = s.indexOf(':', start);
        int s2 = s.indexOf(':', s1 + 1);
        int s3 = s.indexOf('.', s2 + 1);
        if (s1 <= 0 || s2 <= s1) {
            throw new IllegalArgumentException(s);
        }
        boolean negative;
        hour = Integer.parseInt(s.substring(start, s1));
        if (hour < 0) {
            if (timeOfDay) {
                throw new IllegalArgumentException(s);
            }
            negative = true;
            hour = -hour;
        } else {
            negative = false;
        }
        minute = Integer.parseInt(s.substring(s1 + 1, s2));
        if (s3 < 0) {
            second = Integer.parseInt(s.substring(s2 + 1, end));
        } else {
            second = Integer.parseInt(s.substring(s2 + 1, s3));
            String n = (s.substring(s3 + 1, end) + "000000000").substring(0, 9);
            nanos = Integer.parseInt(n);
        }
        if (hour >= 2000000 || minute < 0 || minute >= 60 || second < 0 || second >= 60) {
            throw new IllegalArgumentException(s);
        }
        if (timeOfDay && hour >= 24) {
            throw new IllegalArgumentException(s);
        }
        nanos += ((((hour * 60L) + minute) * 60) + second) * 1000000000;
        return negative ? -nanos : nanos;
    }

    public static long getMillis(TimeZone tz, int year, int month, int day, int hour, int minute,
                                 int second, int millis) {
        try {
            return getTimeTry(false, tz, year, month, day, hour, minute, second, millis);
        } catch (IllegalArgumentException e) {
            String message = e.toString();
            if (message.indexOf("HOUR_OF_DAY") > 0) {
                if (hour < 0 || hour > 23) {
                    throw e;
                }
                return getTimeTry(true, tz, year, month, day, hour, minute, second, millis);
            } else if (message.indexOf("DAY_OF_MONTH") > 0) {
                int maxDay;
                if (month == 2) {
                    maxDay = new GregorianCalendar().isLeapYear(year) ? 29 : 28;
                } else {
                    maxDay = 30 + ((month + (month > 7 ? 1 : 0)) & 1);
                }
                if (day < 1 || day > maxDay) {
                    throw e;
                }
                hour += 6;
                return getTimeTry(true, tz, year, month, day, hour, minute, second, millis);
            } else {
                return getTimeTry(true, tz, year, month, day, hour, minute, second, millis);
            }
        }
    }

    private static long getTimeTry(boolean lenient, TimeZone tz, int year, int month, int day, int hour,
                                   int minute, int second, int millis) {
        Calendar c;
        if (tz == null) {
            c = getCalendar();
        } else {
            c = Calendar.getInstance(tz);
        }
        synchronized (c) {
            c.clear();
            c.setLenient(lenient);
            setCalendarFields(c, year, month, day, hour, minute, second, millis);
            return c.getTime().getTime();
        }
    }

    private static void setCalendarFields(Calendar cal, int year, int month, int day, int hour,
                                          int minute, int second, int millis) {
        if (year <= 0) {
            cal.set(Calendar.ERA, GregorianCalendar.BC);
            cal.set(Calendar.YEAR, 1 - year);
        } else {
            cal.set(Calendar.ERA, GregorianCalendar.AD);
            cal.set(Calendar.YEAR, year);
        }
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millis);
    }

    public static int getDatePart(java.util.Date d, int field) {
        Calendar c = getCalendar();
        synchronized (c) {
            c.setTime(d);
            if (field == Calendar.YEAR) {
                return getYear(c);
            }
            int value = c.get(field);
            if (field == Calendar.MONTH) {
                return value + 1;
            }
            return value;
        }
    }

    private static int getYear(Calendar calendar) {
        int year = calendar.get(Calendar.YEAR);
        if (calendar.get(Calendar.ERA) == GregorianCalendar.BC) {
            year = 1 - year;
        }
        return year;
    }

    public static long getTimeLocalWithoutDst(java.util.Date d) {
        return d.getTime() + zoneOffset;
    }

    public static long getTimeUTCWithoutDst(long millis) {
        return millis - zoneOffset;
    }

    public static int getIsoDayOfWeek(java.util.Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date.getTime());
        int val = cal.get(Calendar.DAY_OF_WEEK) - 1;
        return val == 0 ? 7 : val;
    }

    public static int getIsoWeek(java.util.Date date) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(date.getTime());
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setMinimalDaysInFirstWeek(4);
        return c.get(Calendar.WEEK_OF_YEAR);
    }

    public static int getIsoYear(java.util.Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date.getTime());
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setMinimalDaysInFirstWeek(4);
        int year = getYear(cal);
        int month = cal.get(Calendar.MONTH);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        if (month == 0 && week > 51) {
            year--;
        } else if (month == 11 && week == 1) {
            year++;
        }
        return year;
    }

    public static String formatDateTime(java.util.Date date, String format, String locale,
                                        String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    public static java.util.Date parseDateTime(String date, String format, String locale,
                                               String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        try {
            synchronized (dateFormat) {
                return dateFormat.parse(date);
            }
        } catch (Exception e) {
            throw UnificationException.get("%s parse error.", e, date);
        }
    }

    private static SimpleDateFormat getDateFormat(String format, String locale, String timeZone) {
        try {
            SimpleDateFormat df;
            if (locale == null) {
                df = new SimpleDateFormat(format);
            } else {
                Locale l = new Locale(locale);
                df = new SimpleDateFormat(format, l);
            }
            if (timeZone != null) {
                df.setTimeZone(TimeZone.getTimeZone(timeZone));
            }
            return df;
        } catch (Exception e) {
            throw UnificationException.get("%s parse error.", e, format + "/" + locale + "/" + timeZone);
        }
    }

    public static boolean isValidDate(int year, int month, int day) {
        if (month < 1 || month > 12 || day < 1) {
            return false;
        }
        if (year > 1582) {
            if (month != 2) {
                return day <= NORMAL_DAYS_PER_MONTH[month];
            }
            if ((year & 3) != 0) {
                return day <= 28;
            }
            return day <= ((year % 100 != 0) || (year % 400 == 0) ? 29 : 28);
        } else if (year == 1582 && month == 10) {
            return day <= 31 && (day < 5 || day > 14);
        }
        if (month != 2 && day <= NORMAL_DAYS_PER_MONTH[month]) {
            return true;
        }
        return day <= ((year & 3) != 0 ? 28 : 29);
    }

    public static Date convertDateValueToDate(long dateValue) {
        long millis = getMillis(TimeZone.getDefault(), yearFromDateValue(dateValue),
                monthFromDateValue(dateValue), dayFromDateValue(dateValue), 0, 0, 0, 0);
        return new Date(millis);
    }

    public static Timestamp convertDateValueToTimestamp(long dateValue, long nanos) {
        long millis = nanos / 1000000;
        nanos -= millis * 1000000;
        long s = millis / 1000;
        millis -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        long ms = getMillis(TimeZone.getDefault(), yearFromDateValue(dateValue),
                monthFromDateValue(dateValue), dayFromDateValue(dateValue), (int) h, (int) m, (int) s,
                0);
        Timestamp ts = new Timestamp(ms);
        ts.setNanos((int) (nanos + millis * 1000000));
        return ts;
    }

    public static Time convertNanoToTime(long nanos) {
        long millis = nanos / 1000000;
        long s = millis / 1000;
        millis -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        long ms = getMillis(TimeZone.getDefault(), 1970, 1, 1, (int) (h % 24), (int) m, (int) s,
                (int) millis);
        return new Time(ms);
    }

    public static int yearFromDateValue(long x) {
        return (int) (x >>> SHIFT_YEAR);
    }

    public static int monthFromDateValue(long x) {
        return (int) (x >>> SHIFT_MONTH) & 15;
    }

    public static int dayFromDateValue(long x) {
        return (int) (x & 31);
    }

    public static long dateValue(long year, int month, int day) {
        return (year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    public static long dateValueFromDate(long ms) {
        Calendar cal = getCalendar();
        synchronized (cal) {
            cal.clear();
            cal.setTimeInMillis(ms);
            return dateValueFromCalendar(cal);
        }
    }

    private static long dateValueFromCalendar(Calendar cal) {
        int year, month, day;
        year = getYear(cal);
        month = cal.get(Calendar.MONTH) + 1;
        day = cal.get(Calendar.DAY_OF_MONTH);
        return ((long) year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    public static long nanosFromDate(long ms) {
        Calendar cal = getCalendar();
        synchronized (cal) {
            cal.clear();
            cal.setTimeInMillis(ms);
            return nanosFromCalendar(cal);
        }
    }

    private static long nanosFromCalendar(Calendar cal) {
        int h = cal.get(Calendar.HOUR_OF_DAY);
        int m = cal.get(Calendar.MINUTE);
        int s = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);
        return ((((((h * 60L) + m) * 60) + s) * 1000) + millis) * 1000000;
    }

    public static ValueTimestamp normalizeTimestamp(long absoluteDay, long nanos) {
        if (nanos > NANOS_PER_DAY || nanos < 0) {
            long d;
            if (nanos > NANOS_PER_DAY) {
                d = nanos / NANOS_PER_DAY;
            } else {
                d = (nanos - NANOS_PER_DAY + 1) / NANOS_PER_DAY;
            }
            nanos -= d * NANOS_PER_DAY;
            absoluteDay += d;
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValueFromAbsoluteDay(absoluteDay), nanos);
    }

    public static long absoluteDayFromDateValue(long dateValue) {
        long y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        if (m <= 2) {
            y--;
            m += 12;
        }
        long a = ((y * 2922L) >> 3) + DAYS_OFFSET[m - 3] + d - 719484;
        if (y <= 1582 && ((y < 1582) || (m * 100 + d < 1005))) {
            a += 13;
        } else if (y < 1901 || y > 2099) {
            a += (y / 400) - (y / 100) + 15;
        }
        return a;
    }

    public static long dateValueFromAbsoluteDay(long absoluteDay) {
        long d = absoluteDay + 719468;
        long y100 = 0, offset;
        if (d > 578040) {
            long y400 = d / 146097;
            d -= y400 * 146097;
            y100 = d / 36524;
            d -= y100 * 36524;
            offset = y400 * 400 + y100 * 100;
        } else {
            d += 292200000002L;
            offset = -800000000;
        }
        long y4 = d / 1461;
        d -= y4 * 1461;
        long y = d / 365;
        d -= y * 365;
        if (d == 0 && (y == 4 || y100 == 4)) {
            y--;
            d += 365;
        }
        y += offset + y4 * 4;
        int m = ((int) d * 2 + 1) * 5 / 306;
        d -= DAYS_OFFSET[m] - 1;
        if (m >= 10) {
            y++;
            m -= 12;
        }
        return dateValue(y, m + 3, (int) d);
    }
}
