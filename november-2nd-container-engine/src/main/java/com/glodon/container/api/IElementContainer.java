package com.glodon.container.api;

import com.glodon.container.engine.ElementTagIndexValue;
import com.glodon.container.engine.ElementValue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * @program: november-2nd
 * @description:
 * @author: hons.chang
 * @since: 2023-10-25 14:12
 **/
public interface IElementContainer {

    void init(String workDir, String table);

    void close();

    void truncate();

    ElementValue getElementById(Long id);

    List<ElementValue> getElementsById(List<Long> ids);

    Set<ElementValue> getElementsByCategory(String category);

    Set<ElementValue> getElementsByTags(ElementTagIndexValue tagIndexValue);

    void addElement(ElementValue elementValue);

    void batchAddElement(ElementValue[] ids);

    void batchAddElementParallel(ElementValue[] ids, CountDownLatch latch);

    void batchUpdateElement(ElementValue[] elementValues);

    boolean updateElementParallel(ElementValue[] elementValues, CountDownLatch latch);

    boolean deleteElement(Long id);

    void batchDeleteElement(Long[] ids);

    void batchDeleteElementParallel(Long[] ids, CountDownLatch latch);

    boolean updateElement(ElementValue elementValue);


}
