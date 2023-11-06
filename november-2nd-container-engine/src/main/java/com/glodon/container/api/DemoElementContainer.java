package com.glodon.container.api;

import com.glodon.base.conf.Config;
import com.glodon.base.table.Tablet;
import com.glodon.base.value.ValueInt;
import com.glodon.base.value.ValueUuid;
import com.glodon.container.engine.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * @program: november-2nd
 * @description:
 * @author: hons.chang
 * @since: 2023-10-26 16:00
 **/
public class DemoElementContainer implements IElementContainer {

    private volatile boolean starting;
    protected Tablet<Long, ElementValue> tablet;

    @Override
    public synchronized void init(String workDir, String table) {
        if (!starting) {
            tablet = new ElementTabletImpl(workDir, table);
            tablet.init(Config.DEFAULT);
            starting = true;
        }

    }

    @Override
    public synchronized void close() {
        if (starting) {
            tablet.close();
            tablet.drop();
            starting = false;
        }
    }

    @Override
    public void truncate() {
        tablet.truncate();
    }

    @Override
    public ElementValue getElementById(Long id) {
        return tablet.select(id);
    }

    @Override
    public List<ElementValue> getElementsById(List<Long> ids) {
        List<ElementValue> elementValues = new ArrayList<>(ids.size());
        for (Long id : ids) {
            elementValues.add(tablet.select(id));
        }
        return elementValues;
    }

    @Override
    public Set<ElementValue> getElementsByCategory(String category) {
        UUID categoryUuid = UUID.fromString(category);
        ElementCategoryIndexValue categoryIndexValue = ElementCategoryIndexValue.get(ValueUuid.get(categoryUuid.getMostSignificantBits(), categoryUuid.getLeastSignificantBits()));
        return tablet.in(categoryIndexValue);
    }

    @Override
    public Set<ElementValue> getElementsByTags(ElementTagIndexValue tagIndexValue) {
        return tablet.in(tagIndexValue);
    }

    @Override
    public void addElement(ElementValue elementValue) {
        tablet.insert(elementValue);
    }


    @Override
    public void batchAddElement(ElementValue[] elementValues) {
        ElementTabletImpl elementTabletimpl = (ElementTabletImpl) tablet;
        try {
            elementTabletimpl.insert(elementValues);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void batchAddElementParallel(ElementValue[] elementValues, CountDownLatch latch) {
        batchAddElement(elementValues);
        latch.countDown();
    }

    @Override
    public void batchUpdateElement(ElementValue[] elementValues) {
        for (ElementValue elementValue : elementValues) {
            tablet.update(elementValue);
        }
    }

    @Override
    public boolean updateElementParallel(ElementValue[] elementValues, CountDownLatch latch) {
        batchUpdateElement(elementValues);
        latch.countDown();
        return true;
    }

    @Override
    public boolean deleteElement(Long id) {
        try {
            tablet.delete(id);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public void batchDeleteElement(Long[] ids) {
        try {
            tablet.delete(ids);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void batchDeleteElementParallel(Long[] ids, CountDownLatch latch) {
        batchDeleteElement(ids);
        latch.countDown();
    }

    @Override
    public boolean updateElement(ElementValue elementValue) {
        tablet.update(elementValue);
        return true;
    }
}
