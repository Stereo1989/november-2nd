package com.glodon.base.value;

import com.glodon.base.util.StringUtils;

public class ValueStringFixed extends ValueString {

    private static final ValueStringFixed EMPTY = new ValueStringFixed("");

    protected ValueStringFixed(String value) {
        super(value);
    }

    private static String trimRight(String s) {
        int endIndex = s.length() - 1;
        int i = endIndex;
        while (i >= 0 && s.charAt(i) == ' ') {
            i--;
        }
        s = i == endIndex ? s : s.substring(0, i + 1);
        return s;
    }

    @Override
    public int getType() {
        return Value.STRING_FIXED;
    }

    public static ValueStringFixed get(String s) {
        s = trimRight(s);
        if (s.length() == 0) {
            return EMPTY;
        }
        return new ValueStringFixed(StringUtils.cache(s));
    }

    @Override
    protected ValueString getNew(String s) {
        return ValueStringFixed.get(s);
    }

}
