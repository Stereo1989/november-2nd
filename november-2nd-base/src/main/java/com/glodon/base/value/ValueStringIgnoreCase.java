package com.glodon.base.value;

import com.glodon.base.util.StringUtils;

public class ValueStringIgnoreCase extends ValueString {

    private static final ValueStringIgnoreCase EMPTY = new ValueStringIgnoreCase("");
    private int hash;

    protected ValueStringIgnoreCase(String value) {
        super(value);
    }

    @Override
    public int getType() {
        return Value.STRING_IGNORECASE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueString && value.equalsIgnoreCase(((ValueString) other).value);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = value.toUpperCase().hashCode();
        }
        return hash;
    }

    public static ValueStringIgnoreCase get(String s) {
        if (s.length() == 0) {
            return EMPTY;
        }
        return new ValueStringIgnoreCase(StringUtils.cache(s));
    }

    @Override
    protected ValueString getNew(String s) {
        return ValueStringIgnoreCase.get(s);
    }
}
