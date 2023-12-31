package com.glodon.servingsphere.serialization.org.msgpack.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Title:一些零散的公共方法<br>
 * <p/>
 * Description: <br>
 * <p/>
 */
public class CommonUtils {

    private final static Logger logger = LoggerFactory.getLogger(CommonUtils.class);


    public static boolean isWindows() {
        boolean flag = Boolean.FALSE;
        String osName = System.getProperty("os.name").toLowerCase();
        logger.debug("osName::{}", osName);
        //System.out.println(" osName:"+osName);
        if (osName.indexOf("windows") >= 0) {
            flag = Boolean.TRUE;
        }
        return flag;
    }

    public static Boolean isLinux() {
        boolean flag = Boolean.FALSE;
        String osName = System.getProperty("os.name").toLowerCase();
        logger.debug("osName::{}", osName);

        if (osName.indexOf("linux") >= 0) {
            flag = Boolean.TRUE;
        }
        return flag;
    }

    /**
     * byte array copy.
     *
     * @param src    src.
     * @param length new length.
     * @return new byte array.
     */
    public static byte[] copyOf(byte[] src, int length) {
        byte[] dest = new byte[length];
        System.arraycopy(src, 0, dest, 0, Math.min(src.length, length));
        return dest;
    }

    /**
     * 将值放入ConcurrentMap，已经考虑第一次并发问题
     *
     * @param map   ConcurrentMap
     * @param key   关键字
     * @param value 值
     * @param <K>   关键字类型
     * @param <V>   值类型
     * @return
     */
    public static <K, V> V putToConcurrentMap(ConcurrentMap<K, V> map, K key, V value) {
        V old = map.putIfAbsent(key, value);
        return old != null ? old : value;
    }

    /**
     * 不为空，且为“true”
     *
     * @param b Boolean对象
     * @return 不为空，且为true
     */
    public static boolean isTrue(String b) {
        return b != null && "true".equalsIgnoreCase(b);
    }

    /**
     * 不为空，且为true
     *
     * @param b Boolean对象
     * @return 不为空，且为true
     */
    public static boolean isTrue(Boolean b) {
        return b != null && b;
    }

    /**
     * 不为空，且为false
     *
     * @param b Boolean对象
     * @return 不为空，且为true
     */
    public static boolean isFalse(Boolean b) {
        return b != null && !b;
    }

    /**
     * 不为空，且为“false”
     *
     * @param b Boolean对象
     * @return 不为空，且为true
     */
    public static boolean isFalse(String b) {
        return b != null && "false".equalsIgnoreCase(b);
    }

    /**
     * 判断一个集合是否为空
     *
     * @param collection 集合
     * @return 是否为空
     */
    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    /**
     * 判断一个集合是否为非空
     *
     * @param collection 集合
     * @return 是否为非空
     */
    public static boolean isNotEmpty(Collection collection) {
        return collection != null && !collection.isEmpty();
    }

    /**
     * 判断一个Map是否为空
     *
     * @param map Map
     * @return 是否为空
     */
    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    /**
     * 判断一个Map是否为非空
     *
     * @param map Map
     * @return 是否为非空
     */
    public static boolean isNotEmpty(Map map) {
        return map != null && !map.isEmpty();
    }

    /**
     * 判断一个Array是否为空
     *
     * @param array 数组
     * @return 是否为空
     */
    public static boolean isEmpty(Object[] array) {
        return array == null || array.length == 0;
    }

    /**
     * 判断一个Array是否为非空
     *
     * @param array 数组
     * @return 是否为非空
     */
    public static boolean isNotEmpty(Object[] array) {
        return array != null && array.length > 0;
    }

    /**
     * 字符串转值
     *
     * @param num        数字
     * @param defaultInt 默认值
     * @return int
     */
    public static int parseInt(String num, int defaultInt) {
        if (num == null) {
            return defaultInt;
        } else {
            try {
                return Integer.parseInt(num);
            } catch (Exception e) {
                return defaultInt;
            }
        }
    }

    /**
     * 字符串转值
     *
     * @param nums     多个数字
     * @param sperator 分隔符
     * @return int[]
     */
    public static int[] parseInts(String nums, String sperator) {
        String[] ss = StringUtils.split(nums, sperator);
        int[] ints = new int[ss.length];
        for (int i = 0; i < ss.length; i++) {
            ints[i] = Integer.parseInt(ss[i]);
        }
        return ints;
    }

    /**
     * 比较list元素是否一致，忽略顺序
     *
     * @param left  左边List
     * @param right 右边List
     * @param <T>   元素类型
     * @return 是否一致
     */
    public static <T> boolean listEquals(List<T> left, List<T> right) {
        if (left == null) {
            return right == null;
        } else {
            if (right == null) {
                return false;
            }
            if (left.size() != right.size()) {
                return false;
            }

            List<T> ltmp = new ArrayList<T>(left);
            List<T> rtmp = new ArrayList<T>(right);
            for (T t : ltmp) {
                if (rtmp.contains(t)) {
                    rtmp.remove(t);
                }
            }
            return rtmp.isEmpty();
        }
    }

    /**
     * 连接集合类为字符串
     *
     * @param collection 集合
     * @param separator  分隔符
     * @return 分隔符连接的字符串
     */
    public static String join(Collection collection, String separator) {
        if (collection == null || collection.size() == 0) {
            return StringUtils.EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        for (Object object : collection) {
            if (object != null) {
                String string = StringUtils.toString(object);
                if (string != null) {
                    sb.append(string).append(separator);
                }
            }
        }
        return sb.length() > 0 ? sb.substring(0, sb.length() - separator.length()) : StringUtils.EMPTY;
    }
}
