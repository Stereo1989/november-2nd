package com.glodon.container.engine;

import com.glodon.base.value.Value;
import com.glodon.servingsphere.serialization.org.msgpack.BeanMessage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by liujing on 2023/10/20.
 */
public class ElementIndices implements Serializable {

    private final Long[] ids;
    private ElementValue[] elementValues;
    private final ElementTagIndexValue[] tagIndexValues;
    private final ElementCategoryIndexValue[] categoryIndexValues;

    public ElementIndices(ElementValue[] values) {
        this.elementValues = values;
        this.ids = new Long[values.length];
        this.tagIndexValues = new ElementTagIndexValue[values.length];
        this.categoryIndexValues = new ElementCategoryIndexValue[values.length];
        for (int i = 0; i < values.length; i++) {
            ElementValue value = values[i];
            ids[i] = value.getId();
            tagIndexValues[i] = value.getElementTagIndexValue();
            categoryIndexValues[i] = value.getElementCategoryIndex();
        }
        Arrays.sort(elementValues);
        Arrays.sort(ids);
        Arrays.sort(tagIndexValues);
        Arrays.sort(categoryIndexValues);
    }

    public ElementValue[] takeElementValues() {
        try {
            ElementValue[] evArr = this.elementValues;
            return evArr;
        } finally {
            this.elementValues = null;
        }
    }

    public ElementTagIndexValue[] getTagIndexValues() {
        return tagIndexValues;
    }

    public ElementCategoryIndexValue[] getCategoryIndexValues() {
        return categoryIndexValues;
    }

    public Long[] getIds() {
        return ids;
    }
}
