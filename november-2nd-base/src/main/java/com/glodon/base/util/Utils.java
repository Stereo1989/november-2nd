package com.glodon.base.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;

import com.glodon.base.exceptions.UnificationException;
import com.glodon.base.exceptions.ConfigException;

public class Utils {

    private static final int GC_DELAY = 50;
    private static final int MAX_GC = 8;
    private static long lastGC;
    private static boolean allowAllClasses;
    private static HashSet<String> allowedClassNames;
    private static String[] allowedClassNamePrefixes;

    protected Utils() {
    }

    private static int readInt(byte[] buff, int pos) {
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8)
                + (buff[pos] & 0xff);
    }

    public static void writeLong(byte[] buff, int pos, long x) {
        writeInt(buff, pos, (int) (x >> 32));
        writeInt(buff, pos + 4, (int) x);
    }

    private static void writeInt(byte[] buff, int pos, int x) {
        buff[pos++] = (byte) (x >> 24);
        buff[pos++] = (byte) (x >> 16);
        buff[pos++] = (byte) (x >> 8);
        buff[pos++] = (byte) x;
    }

    public static long readLong(byte[] buff, int pos) {
        return (((long) readInt(buff, pos)) << 32) + (readInt(buff, pos + 4) & 0xffffffffL);
    }

    public static int indexOf(byte[] bytes, byte[] pattern, int start) {
        if (pattern.length == 0) {
            return start;
        }
        if (start > bytes.length) {
            return -1;
        }
        int last = bytes.length - pattern.length + 1;
        int patternLen = pattern.length;
        next:
        for (; start < last; start++) {
            for (int i = 0; i < patternLen; i++) {
                if (bytes[start + i] != pattern[i]) {
                    continue next;
                }
            }
            return start;
        }
        return -1;
    }

    public static byte[] copy(byte[] source, byte[] target) {
        int len = source.length;
        if (len > target.length) {
            target = new byte[len];
        }
        System.arraycopy(source, 0, target, 0, len);
        return target;
    }

    public static int hashCode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    public static int getMemoryUsed() {
        collectGarbage();
        Runtime rt = Runtime.getRuntime();
        long mem = rt.totalMemory() - rt.freeMemory();
        return (int) (mem >> 10);
    }

    public static int getMemoryFree() {
        collectGarbage();
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        return (int) (mem >> 10);
    }

    public static long getMemoryMax() {
        long max = Runtime.getRuntime().maxMemory();
        return max / 1024;
    }

    private static synchronized void collectGarbage() {
        Runtime runtime = Runtime.getRuntime();
        long total = runtime.totalMemory();
        long time = System.currentTimeMillis();
        if (lastGC + GC_DELAY < time) {
            for (int i = 0; i < MAX_GC; i++) {
                runtime.gc();
                long now = runtime.totalMemory();
                if (now == total) {
                    lastGC = System.currentTimeMillis();
                    break;
                }
                total = now;
            }
        }
    }

    public static <T> ArrayList<T> newSmallArrayList() {
        return new ArrayList<>(4);
    }

    public static Class<?> loadUserClass(String className) {
        if (allowedClassNames == null) {
            String s = "*";
            ArrayList<String> prefixes = new ArrayList<>();
            boolean allowAll = false;
            HashSet<String> classNames = new HashSet<>();
            for (String p : StringUtils.arraySplit(s, ',')) {
                if (p.equals("*")) {
                    allowAll = true;
                } else if (p.endsWith("*")) {
                    prefixes.add(p.substring(0, p.length() - 1));
                } else {
                    classNames.add(p);
                }
            }
            allowedClassNamePrefixes = new String[prefixes.size()];
            prefixes.toArray(allowedClassNamePrefixes);
            allowAllClasses = allowAll;
            allowedClassNames = classNames;
        }
        if (!allowAllClasses && !allowedClassNames.contains(className)) {
            boolean allowed = false;
            for (String s : allowedClassNamePrefixes) {
                if (className.startsWith(s)) {
                    allowed = true;
                }
            }
            if (!allowed) {
                throw UnificationException.get("access denied_to class %s.", className);
            }
        }
        /*
        try {
            return Utils.class.getClassLoader().loadClass(className);
        } catch (Throwable e) {
            try {
                return Thread.currentThread().getContextClassLoader().loadClass(className);
            } catch (Throwable e2) {
                DbException.get(ErrorCode.CLASS_NOT_FOUND_1, e, className);
            }
        }
        */
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            try {
                return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
            } catch (Exception e2) {
                throw UnificationException.get("%s class not found.", e, className);
            }
        } catch (NoClassDefFoundError e) {
            throw UnificationException.get("%s class not found.", e, className);
        } catch (Error e) {
            // UnsupportedClassVersionError
            throw UnificationException.get("%s general error.", e, className);
        }
    }

    public static Object callStaticMethod(String classAndMethod, Object... params) throws Exception {
        int lastDot = classAndMethod.lastIndexOf('.');
        String className = classAndMethod.substring(0, lastDot);
        String methodName = classAndMethod.substring(lastDot + 1);
        return callMethod(null, Class.forName(className), methodName, params);
    }

    public static Object callMethod(Object instance, String methodName, Object... params)
            throws Exception {
        return callMethod(instance, instance.getClass(), methodName, params);
    }

    private static Object callMethod(Object instance, Class<?> clazz, String methodName,
                                     Object... params) throws Exception {
        Method best = null;
        int bestMatch = 0;
        boolean isStatic = instance == null;
        for (Method m : clazz.getMethods()) {
            if (Modifier.isStatic(m.getModifiers()) == isStatic && m.getName().equals(methodName)) {
                int p = match(m.getParameterTypes(), params);
                if (p > bestMatch) {
                    bestMatch = p;
                    best = m;
                }
            }
        }
        if (best == null) {
            throw new NoSuchMethodException(methodName);
        }
        return best.invoke(instance, params);
    }

    public static Object newInstance(String className, Object... params) throws Exception {
        Constructor<?> best = null;
        int bestMatch = 0;
        for (Constructor<?> c : Class.forName(className).getConstructors()) {
            int p = match(c.getParameterTypes(), params);
            if (p > bestMatch) {
                bestMatch = p;
                best = c;
            }
        }
        if (best == null) {
            throw new NoSuchMethodException(className);
        }
        return best.newInstance(params);
    }

    public static <T> T newInstance(String className) {
        Class<?> clz = loadUserClass(className);
        return newInstance(clz);
    }

    public static <T> T newInstance(Class<?> clz) {
        try {
            return (T) clz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw UnificationException.convert(e);
        }
    }

    private static int match(Class<?>[] params, Object[] values) {
        int len = params.length;
        if (len == values.length) {
            int points = 1;
            for (int i = 0; i < len; i++) {
                Class<?> pc = getNonPrimitiveClass(params[i]);
                Object v = values[i];
                Class<?> vc = v == null ? null : v.getClass();
                if (pc == vc) {
                    points++;
                } else if (vc == null) {
                } else if (!pc.isAssignableFrom(vc)) {
                    return 0;
                }
            }
            return points;
        }
        return 0;
    }

    public static Object getStaticField(String classAndField) throws Exception {
        int lastDot = classAndField.lastIndexOf('.');
        String className = classAndField.substring(0, lastDot);
        String fieldName = classAndField.substring(lastDot + 1);
        return Class.forName(className).getField(fieldName).get(null);
    }

    public static Object getField(Object instance, String fieldName) throws Exception {
        return instance.getClass().getField(fieldName).get(instance);
    }

    public static boolean isClassPresent(String fullyQualifiedClassName) {
        try {
            Class.forName(fullyQualifiedClassName);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static Class<?> getNonPrimitiveClass(Class<?> clazz) {
        if (!clazz.isPrimitive()) {
            return clazz;
        } else if (clazz == boolean.class) {
            return Boolean.class;
        } else if (clazz == byte.class) {
            return Byte.class;
        } else if (clazz == char.class) {
            return Character.class;
        } else if (clazz == double.class) {
            return Double.class;
        } else if (clazz == float.class) {
            return Float.class;
        } else if (clazz == int.class) {
            return Integer.class;
        } else if (clazz == long.class) {
            return Long.class;
        } else if (clazz == short.class) {
            return Short.class;
        } else if (clazz == void.class) {
            return Void.class;
        }
        return clazz;
    }

    public static String getProperty(String key, String defaultValue) {
        try {
            return System.getProperty(key, defaultValue);
        } catch (SecurityException se) {
            return defaultValue;
        }
    }

    public static int getProperty(String key, int defaultValue) {
        String s = getProperty(key, null);
        return toInt(s, defaultValue);
    }

    public static boolean getProperty(String key, boolean defaultValue) {
        String s = getProperty(key, null);
        return toBoolean(s, defaultValue);
    }

    public static <T> Class<T> classForName(String classname, String readable) throws ConfigException {
        try {
            return (Class<T>) Class.forName(classname);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new ConfigException(String.format("Unable to find %s class '%s'", readable, classname),
                    e);
        }
    }

    public static <T> T construct(String classname, String readable) throws ConfigException {
        Class<T> cls = Utils.classForName(classname, readable);
        return construct(cls, classname, readable);
    }

    public static <T> T construct(Class<T> cls, String classname, String readable)
            throws ConfigException {
        try {
            return cls.getDeclaredConstructor().newInstance();
        } catch (IllegalAccessException e) {
            throw new ConfigException(String.format(
                    "Default constructor for %s class '%s' is inaccessible.", readable, classname));
        } catch (InstantiationException e) {
            throw new ConfigException(
                    String.format("Cannot use abstract class '%s' as %s.", classname, readable));
        } catch (Exception e) {
            if (e.getCause() instanceof ConfigException)
                throw (ConfigException) e.getCause();
            throw new ConfigException(
                    String.format("Error instantiating %s class '%s'.", readable, classname), e);
        }
    }

    public static int toInt(String value, int def) {
        if (value == null)
            return def;
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            UnificationException.traceThrowable(e);
            return def;
        }
    }

    public static int toIntMB(String value, int def) {
        if (value == null)
            return def;
        try {
            return Integer.parseInt(value) * 1024 * 1024;
        } catch (Exception e) {
            UnificationException.traceThrowable(e);
            return def;
        }
    }

    public static long toLong(String value, long def) {
        if (value == null)
            return def;
        try {
            return Long.parseLong(value);
        } catch (Exception e) {
            UnificationException.traceThrowable(e);
            return def;
        }
    }

    public static boolean toBoolean(String value, boolean def) {
        if (value == null)
            return def;
        try {
            return Boolean.parseBoolean(value);
        } catch (Exception e) {
            UnificationException.traceThrowable(e);
            return def;
        }
    }
}
