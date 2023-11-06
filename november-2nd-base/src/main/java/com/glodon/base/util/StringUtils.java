package com.glodon.base.util;

import java.lang.ref.SoftReference;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Locale;

import com.glodon.base.Constants;
import com.glodon.base.exceptions.UnificationException;

public class StringUtils {

    private static SoftReference<String[]> softCache = new SoftReference<String[]>(null);
    private static long softCacheCreated;
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final int[] HEX_DECODE = new int['f' + 1];

    static {
        for (int i = 0; i < HEX_DECODE.length; i++) {
            HEX_DECODE[i] = -1;
        }
        for (int i = 0; i <= 9; i++) {
            HEX_DECODE[i + '0'] = i;
        }
        for (int i = 0; i <= 5; i++) {
            HEX_DECODE[i + 'a'] = HEX_DECODE[i + 'A'] = i + 10;
        }
    }

    private StringUtils() {
    }

    private static String[] getCache() {
        String[] cache;
        if (softCache != null) {
            cache = softCache.get();
            if (cache != null) {
                return cache;
            }
        }
        long time = System.currentTimeMillis();
        if (softCacheCreated != 0 && time - softCacheCreated < 5000) {
            return null;
        }
        try {
            cache = new String[Constants.OBJECT_CACHE_SIZE];
            softCache = new SoftReference<String[]>(cache);
            return cache;
        } finally {
            softCacheCreated = System.currentTimeMillis();
        }
    }

    public static boolean equals(String a, String b) {
        if (a == null) {
            return b == null;
        }
        return a.equals(b);
    }

    public static String toUpperEnglish(String s) {
        return s.toUpperCase(Locale.ENGLISH);
    }

    public static String toLowerEnglish(String s) {
        return s.toLowerCase(Locale.ENGLISH);
    }

    public static boolean startsWithIgnoreCase(String s, String start) {
        if (s.length() < start.length()) {
            return false;
        }
        return s.substring(0, start.length()).equalsIgnoreCase(start);
    }

    public static String javaEncode(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            switch (c) {
                // case '\b':
                // // BS backspace
                // // not supported in properties files
                // buff.append("\\b");
                // break;
                case '\t':
                    // HT horizontal tab
                    buff.append("\\t");
                    break;
                case '\n':
                    // LF linefeed
                    buff.append("\\n");
                    break;
                case '\f':
                    // FF form feed
                    buff.append("\\f");
                    break;
                case '\r':
                    // CR carriage return
                    buff.append("\\r");
                    break;
                case '"':
                    // double quote
                    buff.append("\\\"");
                    break;
                case '\\':
                    // backslash
                    buff.append("\\\\");
                    break;
                default:
                    int ch = c & 0xffff;
                    if (ch >= ' ' && (ch < 0x80)) {
                        buff.append(c);
                        // not supported in properties files
                        // } else if(ch < 0xff) {
                        // buff.append("\\");
                        // // make sure it's three characters (0x200 is octal 1000)
                        // buff.append(Integer.toOctalString(0x200 | ch).substring(1));
                    } else {
                        buff.append("\\u");
                        String hex = Integer.toHexString(ch);
                        // make sure it's four characters
                        for (int len = hex.length(); len < 4; len++) {
                            buff.append('0');
                        }
                        buff.append(hex);
                    }
            }
        }
        return buff.toString();
    }

    public static String addAsterisk(String s, int index) {
        if (s != null && index < s.length()) {
            s = s.substring(0, index) + "[*]" + s.substring(index);
        }
        return s;
    }

    private static UnificationException getFormatException(String s, int i) {
        return UnificationException.get("%s format_error.", addAsterisk(s, i));
    }

    public static String javaDecode(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (i + 1 >= s.length()) {
                    throw getFormatException(s, i);
                }
                c = s.charAt(++i);
                switch (c) {
                    case 't':
                        buff.append('\t');
                        break;
                    case 'r':
                        buff.append('\r');
                        break;
                    case 'n':
                        buff.append('\n');
                        break;
                    case 'b':
                        buff.append('\b');
                        break;
                    case 'f':
                        buff.append('\f');
                        break;
                    case '#':
                        buff.append('#');
                        break;
                    case '=':
                        buff.append('=');
                        break;
                    case ':':
                        buff.append(':');
                        break;
                    case '"':
                        buff.append('"');
                        break;
                    case '\\':
                        buff.append('\\');
                        break;
                    case 'u': {
                        try {
                            c = (char) (Integer.parseInt(s.substring(i + 1, i + 5), 16));
                        } catch (NumberFormatException e) {
                            throw getFormatException(s, i);
                        }
                        i += 4;
                        buff.append(c);
                        break;
                    }
                    default:
                        if (c >= '0' && c <= '9') {
                            try {
                                c = (char) (Integer.parseInt(s.substring(i, i + 3), 8));
                            } catch (NumberFormatException e) {
                                throw getFormatException(s, i);
                            }
                            i += 2;
                            buff.append(c);
                        } else {
                            throw getFormatException(s, i);
                        }
                }
            } else {
                buff.append(c);
            }
        }
        return buff.toString();
    }

    public static String quoteJavaString(String s) {
        if (s == null) {
            return "null";
        }
        return "\"" + javaEncode(s) + "\"";
    }

    public static String enclose(String s) {
        if (s.startsWith("(")) {
            return s;
        }
        return "(" + s + ")";
    }

    public static String unEnclose(String s) {
        if (s.startsWith("(") && s.endsWith(")")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    public static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (Exception e) {
            throw UnificationException.convert(e);
        }
    }

    public static String urlDecode(String encoded) {
        int length = encoded.length();
        byte[] buff = new byte[length];
        int j = 0;
        for (int i = 0; i < length; i++) {
            char ch = encoded.charAt(i);
            if (ch == '+') {
                buff[j++] = ' ';
            } else if (ch == '%') {
                buff[j++] = (byte) Integer.parseInt(encoded.substring(i + 1, i + 3), 16);
                i += 2;
            } else {
                if (Constants.CHECK) {
                    if (ch > 127 || ch < ' ') {
                        throw new IllegalArgumentException(
                                "Unexpected char " + (int) ch + " decoding " + encoded);
                    }
                }
                buff[j++] = (byte) ch;
            }
        }
        String s = new String(buff, 0, j, Constants.UTF8);
        return s;
    }

    public static String[] arraySplit(String s, char separatorChar) {
        return arraySplit(s, separatorChar, true);
    }

    public static String[] arraySplit(String s, char separatorChar, boolean trim) {
        if (s == null) {
            return null;
        }
        int length = s.length();
        if (length == 0) {
            return new String[0];
        }
        ArrayList<String> list = Utils.newSmallArrayList();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == separatorChar) {
                String e = buff.toString();
                list.add(trim ? e.trim() : e);
                buff.setLength(0);
            } else if (c == '\\' && i < length - 1) {
                buff.append(s.charAt(++i));
            } else {
                buff.append(c);
            }
        }
        String e = buff.toString();
        list.add(trim ? e.trim() : e);
        String[] array = new String[list.size()];
        list.toArray(array);
        return array;
    }

    public static int[] arraySplitAsInt(String s, char separatorChar) {
        String[] array = arraySplit(s, separatorChar);
        if (array == null)
            return null;

        int[] intArray = new int[array.length];
        for (int i = 0; i < array.length; i++) {
            intArray[i] = Integer.parseInt(array[i]);
        }
        return intArray;
    }

    public static String arrayCombine(int[] list, char separatorChar) {
        StringBuilder buff = new StringBuilder();
        for (int i : list) {
            if (buff.length() > 0)
                buff.append(',');
            buff.append(i);
        }
        return buff.toString();
    }

    public static String xmlAttr(String name, String value) {
        return " " + name + "=\"" + xmlText(value) + "\"";
    }

    public static String xmlNode(String name, String attributes, String content) {
        return xmlNode(name, attributes, content, true);
    }

    public static String xmlNode(String name, String attributes, String content, boolean indent) {
        String start = attributes == null ? name : name + attributes;
        if (content == null) {
            return "<" + start + "/>\n";
        }
        if (indent && content.indexOf('\n') >= 0) {
            content = "\n" + indent(content);
        }
        return "<" + start + ">" + content + "</" + name + ">\n";
    }

    public static String indent(String s) {
        return indent(s, 4, true);
    }

    public static String indent(String s, int spaces, boolean newline) {
        StringBuilder buff = new StringBuilder(s.length() + spaces);
        for (int i = 0; i < s.length(); ) {
            for (int j = 0; j < spaces; j++) {
                buff.append(' ');
            }
            int n = s.indexOf('\n', i);
            n = n < 0 ? s.length() : n + 1;
            buff.append(s.substring(i, n));
            i = n;
        }
        if (newline && !s.endsWith("\n")) {
            buff.append('\n');
        }
        return buff.toString();
    }

    public static String xmlComment(String data) {
        int idx = 0;
        while (true) {
            idx = data.indexOf("--", idx);
            if (idx < 0) {
                break;
            }
            data = data.substring(0, idx + 1) + " " + data.substring(idx + 1);
        }
        if (data.indexOf('\n') >= 0) {
            return "<!--\n" + indent(data) + "-->\n";
        }
        return "<!-- " + data + " -->\n";
    }

    public static String xmlCData(String data) {
        if (data.indexOf("]]>") >= 0) {
            return xmlText(data);
        }
        boolean newline = data.endsWith("\n");
        data = "<![CDATA[" + data + "]]>";
        return newline ? data + "\n" : data;
    }

    public static String xmlStartDoc() {
        return "<?xml version=\"1.0\"?>\n";
    }

    public static String xmlText(String text) {
        return xmlText(text, false);
    }

    public static String xmlText(String text, boolean escapeNewline) {
        int length = text.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '<':
                    buff.append("&lt;");
                    break;
                case '>':
                    buff.append("&gt;");
                    break;
                case '&':
                    buff.append("&amp;");
                    break;
                case '\'':
                    buff.append("&apos;");
                    break;
                case '\"':
                    buff.append("&quot;");
                    break;
                case '\r':
                case '\n':
                    if (escapeNewline) {
                        buff.append("&#x").append(Integer.toHexString(ch)).append(';');
                    } else {
                        buff.append(ch);
                    }
                    break;
                case '\t':
                    buff.append(ch);
                    break;
                default:
                    if (ch < ' ' || ch > 127) {
                        buff.append("&#x").append(Integer.toHexString(ch)).append(';');
                    } else {
                        buff.append(ch);
                    }
            }
        }
        return buff.toString();
    }

    public static String replaceAll(String s, String before, String after) {
        int next = s.indexOf(before);
        if (next < 0) {
            return s;
        }
        StringBuilder buff = new StringBuilder(s.length() - before.length() + after.length());
        int index = 0;
        while (true) {
            buff.append(s.substring(index, next)).append(after);
            index = next + before.length();
            next = s.indexOf(before, index);
            if (next < 0) {
                buff.append(s.substring(index));
                break;
            }
        }
        return buff.toString();
    }

    public static String quoteIdentifier(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(length + 2);
        buff.append('\"');
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '"') {
                buff.append(c);
            }
            buff.append(c);
        }
        return buff.append('\"').toString();
    }

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }

    public static String pad(String string, int n, String padding, boolean right) {
        if (n < 0) {
            n = 0;
        }
        if (n < string.length()) {
            return string.substring(0, n);
        } else if (n == string.length()) {
            return string;
        }
        char paddingChar;
        if (padding == null || padding.length() == 0) {
            paddingChar = ' ';
        } else {
            paddingChar = padding.charAt(0);
        }
        StringBuilder buff = new StringBuilder(n);
        n -= string.length();
        if (right) {
            buff.append(string);
        }
        for (int i = 0; i < n; i++) {
            buff.append(paddingChar);
        }
        if (!right) {
            buff.append(string);
        }
        return buff.toString();
    }

    public static char[] cloneCharArray(char[] chars) {
        if (chars == null) {
            return null;
        }
        int len = chars.length;
        if (len == 0) {
            return chars;
        }
        char[] copy = new char[len];
        System.arraycopy(chars, 0, copy, 0, len);
        return copy;
    }

    public static String trim(String s, boolean leading, boolean trailing, String sp) {
        char space = (sp == null || sp.length() < 1) ? ' ' : sp.charAt(0);
        if (leading) {
            int len = s.length(), i = 0;
            while (i < len && s.charAt(i) == space) {
                i++;
            }
            s = (i == 0) ? s : s.substring(i);
        }
        if (trailing) {
            int endIndex = s.length() - 1;
            int i = endIndex;
            while (i >= 0 && s.charAt(i) == space) {
                i--;
            }
            s = i == endIndex ? s : s.substring(0, i + 1);
        }
        return s;
    }

    public static String cache(String s) {
        if (s == null) {
            return s;
        } else if (s.length() == 0) {
            return "";
        }
        int hash = s.hashCode();
        String[] cache = getCache();
        if (cache != null) {
            int index = hash & (Constants.OBJECT_CACHE_SIZE - 1);
            String cached = cache[index];
            if (cached != null) {
                if (s.equals(cached)) {
                    return cached;
                }
            }
            cache[index] = s;
        }
        return s;
    }

    public static void clearCache() {
        softCache = new SoftReference<String[]>(null);
    }

    public static byte[] convertHexToBytes(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            throw UnificationException.get("%s hex string odd.", s);
        }
        len /= 2;
        byte[] buff = new byte[len];
        int mask = 0;
        int[] hex = HEX_DECODE;
        try {
            for (int i = 0; i < len; i++) {
                int d = hex[s.charAt(i + i)] << 4 | hex[s.charAt(i + i + 1)];
                mask |= d;
                buff[i] = (byte) d;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw UnificationException.get("%s hex_string_wrong.", s);
        }
        if ((mask & ~255) != 0) {
            throw UnificationException.get("%s hex_string_wrong.", s);
        }
        return buff;
    }

    public static String convertBytesToHex(byte[] value) {
        return convertBytesToHex(value, value.length);
    }

    public static String convertBytesToHex(byte[] value, int len) {
        char[] buff = new char[len + len];
        char[] hex = HEX;
        for (int i = 0; i < len; i++) {
            int c = value[i] & 0xff;
            buff[i + i] = hex[c >> 4];
            buff[i + i + 1] = hex[c & 0xf];
        }
        return new String(buff);
    }

    public static boolean isNumber(String s) {
        if (s.length() == 0) {
            return false;
        }
        for (char c : s.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    public static void appendZeroPadded(StringBuilder buff, int length, long positiveValue) {
        if (length == 2) {
            if (positiveValue < 10) {
                buff.append('0');
            }
            buff.append(positiveValue);
        } else {
            String s = Long.toString(positiveValue);
            length -= s.length();
            while (length > 0) {
                buff.append('0');
                length--;
            }
            buff.append(s);
        }
    }
}
