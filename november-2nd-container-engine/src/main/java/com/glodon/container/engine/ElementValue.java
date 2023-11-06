package com.glodon.container.engine;

import com.glodon.base.Constants;
import com.glodon.base.fs.FileFastLoader;
import com.glodon.base.fs.FileLoader;
import com.glodon.base.util.DataUtils;
import com.glodon.base.util.MathUtils;
import com.glodon.base.util.StringUtils;
import com.glodon.base.value.Value;
import com.glodon.base.value.ValueInt;
import com.glodon.base.value.ValueLong;
import com.glodon.base.value.ValueUuid;
import com.glodon.servingsphere.serialization.org.msgpack.BeanMessage;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Created by liujing on 2023/10/16.
 */
public class ElementValue extends Value implements FileLoader, Serializable, BeanMessage, ElementCategoryIndex, ElementTagIndex {
    public static final int TEST_ELEMENT_TAG = Value.TYPE_COUNT + 4;
    public static int CAMOUFLAGE_MEMORY_SIZE = 30;
    public static final int USE_FILE_SIZE = 8 * 1024;

    private long id;
    private String category;
    private int tag;
    private String path;
    private byte[] content;

    //二级联合索引
    private transient ElementTagIndexValue elementTagIndexValue;
    private transient ElementCategoryIndexValue elementCategoryIndex;


    private static int ELEMENT_META_TAG_0_OFFSET = 0;
    private static int ELEMENT_META_TAG_1_OFFSET = 1;

    public enum ElementMetaTagMask {
        TAG_0_MASK(1 << ELEMENT_META_TAG_0_OFFSET),

        TAG_1_MASK(1 << ELEMENT_META_TAG_1_OFFSET),
        ;

        private int mask;

        ElementMetaTagMask(Integer mask) {
            this.mask = mask;
        }

        public int mark() {
            return mask;
        }
    }

    public ElementValue() {
    }

    public ElementValue(long id, String category, int tag, String path) {
        this(id, category, tag, path, null);
    }

    //insert
    public ElementValue(long id, String category, int tag, String path, byte[] content) {
        this.id = id;
        DataUtils.checkNotNull(category, "category");
        this.category = category;
        this.tag = tag;
        this.path = path;
        DataUtils.checkNotNull(category, "path");
        this.content = content;
        ValueInt tagIndex = ValueInt.get(this.tag);
        java.util.UUID uuid = java.util.UUID.fromString(this.category);
        long l = uuid.getMostSignificantBits();
        long r = uuid.getLeastSignificantBits();
        ValueUuid categoryIndex = ValueUuid.get(l, r);
        ValueLong idIndex = ValueLong.get(this.id);
        //测试程序务必初始化索引
        this.elementTagIndexValue = ElementTagIndexValue.get(tagIndex, idIndex);
        this.elementCategoryIndex = ElementCategoryIndexValue.get(categoryIndex, idIndex);
    }

    public ElementTagIndexValue getElementTagIndexValue() {
        if (elementTagIndexValue == null) {
            ValueInt tagIndex = ValueInt.get(this.tag);
            ValueLong idIndex = ValueLong.get(this.id);
            elementTagIndexValue = ElementTagIndexValue.get(tagIndex, idIndex);
        }
        return elementTagIndexValue;
    }

    public ElementCategoryIndexValue getElementCategoryIndex() {
        if (elementCategoryIndex == null) {
            java.util.UUID uuid = java.util.UUID.fromString(this.category);
            long l = uuid.getMostSignificantBits();
            long r = uuid.getLeastSignificantBits();
            ValueUuid categoryIndex = ValueUuid.get(l, r);
            ValueLong idIndex = ValueLong.get(this.id);
            elementCategoryIndex = ElementCategoryIndexValue.get(categoryIndex, idIndex);
        }
        return elementCategoryIndex;
    }

    public long getId() {
        return this.id;
    }

    public String getCategory() {
        return this.category;
    }

    public byte[] getCategoryBytes() {
        return getCategory().getBytes(Constants.UTF8);
    }

    public int getTag() {
        return this.tag;
    }

    public String getPath() {
        return this.path;
    }

    public byte[] getPathBytes() {
        return StringUtils.isNullOrEmpty(getPath()) ? null : getPath().getBytes(Constants.UTF8);
    }

    public byte[] getContent() {
        if (content == null) {
            loading();
        }
        return this.content;
    }

    @Override
    public int getType() {
        return TEST_ELEMENT_TAG;
    }

    @Override
    public String getString() {
        return String.valueOf(getId());
    }

    @Override
    public Object getObject() {
        return this;
    }

    @Override
    public int hashCode() {
        return (int) (this.id ^ (this.id >> 32));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ElementValue && this.getId() == ((ElementValue) other).getId();
    }

    @Override
    public int compareTo(Value o) {
        ElementValue value = (ElementValue) o;
        return MathUtils.compareLong(getId(), value.getId());
    }

    public int getMemory() {
        return CAMOUFLAGE_MEMORY_SIZE == 0 ? (1 +  //type
                8 + //id
                4 + category.getBytes(Constants.UTF8).length + // category
                4 + //tag
                4 + (path == null ? 0 : path.getBytes(Constants.UTF8).length) + // path
                4 + (content == null || content.length == 0 ? 0 : content.length))  //content
                : CAMOUFLAGE_MEMORY_SIZE;  //伪装成30个字节
    }

    @Override
    public String toString() {
        return "ElementValue{" +
                "id=" + this.id +
                ", category='" + this.category + '\'' +
                ", tag=" + this.tag +
                ", path='" + this.path + '\'' +
                '}';
    }

    @Override
    public void loading() {
        Loader loader = new Loader();
        loader.loading();
    }

    class Loader extends FileFastLoader {

        @Override
        protected String getPath() {
            return ElementValue.this.getPath();
        }

        @Override
        protected void completeLoading(byte[] bytes) {
            ElementValue.this.content = bytes;
        }
    }


    /**
     * 读取meta
     */
    public static ElementValue fromBinary(byte[] metaBinary, byte[] contentBinary, String filePath) {
        if (metaBinary.length != size()) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(metaBinary).order(ByteOrder.LITTLE_ENDIAN);
        long id = buffer.getLong();
        long mostSignificantBits = buffer.getLong();
        long leastSignificantBits = buffer.getLong();
        String category = new UUID(mostSignificantBits, leastSignificantBits).toString();
        int tag = buffer.getInt();

        return new ElementValue(id, category, tag, filePath, contentBinary);
    }

    /**
     * id: 8 bytes  +
     * category: 16 bytes for
     * tag: 4 bytes for
     */
    public static int size() {
        return Long.BYTES + 16 + Integer.BYTES;
    }
}
