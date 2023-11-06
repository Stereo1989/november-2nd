package com.glodon.testing.test;

import com.glodon.base.util.Utils;
import com.glodon.base.value.ValueInt;
import com.glodon.container.api.DemoElementContainer;
import com.glodon.container.api.DemoThreadFactory;
import com.glodon.container.engine.ElementTagIndexValue;
import com.glodon.container.engine.ElementValue;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @program: november-2nd
 * @description:
 * @author: hons.chang
 * @since: 2023-10-25 14:08
 **/
public class Test {

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(CPU_COUNT, CPU_COUNT, 60000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new DemoThreadFactory("test"));

    public enum OperationType {
        INSERT,
        UPDATE,
        DELETE
    }

    public  void close(){
        executor.shutdown();
    }


    public boolean test1AddElement(TestContext testContext) throws IOException {
        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //loadIds这部分落库, notLoadIds这部门放到内存中
        List<Long> loadIds = new ArrayList<>();
        List<Long> notLoadIds = new ArrayList<>();
        testContext.loadHalfElements(loadIds, notLoadIds);

        //开始执行测试
        testContext.startTest("[Test01] AddElement");

        //获取数据
        TestContext.MetaAndContent metaAndContent = testContext.getMetaAndContents();
        int nShouldPass = metaAndContent.getElementValues().length;

        int loopCount = loopCount(nShouldPass);
        //开始批量保存数据,计算时间
        System.out.println("开始写入" + nShouldPass * loopCount + "数据量...");
        int memoryUsedBefore = Utils.getMemoryUsed();
        long tp1 = System.nanoTime();

        for (int i = 0; i < loopCount; i++) {
            demoElementContainer.batchAddElement(metaAndContent.getElementValues());
        }
        double elapsed = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("写入" + nShouldPass * loopCount + "数据耗时: " + elapsed + "μs");
        int memoryUsedAfter = Utils.getMemoryUsed();

        System.out.println("开始验证数据...");
        for (int i = 0; i < notLoadIds.size(); i++) {
            //加载到内存的那一部分数据
            ElementValue elementValue = demoElementContainer.getElementById(notLoadIds.get(i));
            if (Objects.isNull(elementValue)) {
                System.out.println("[Test01] AddElemen 验证失败");
                return false;
            }
        }
        System.out.println("插入数据验证成功,开始记录指标数据...");

        //文件大小
        testContext.recordTotalElementSize("AddElement", metaAndContent.getnTotalElementSize());
        testContext.recordMemoryUsageOfTestPoint("AddElement", memoryUsedBefore, memoryUsedAfter);
        testContext.recordSpeedOfTestPoint("AddElement", nShouldPass * loopCount, elapsed);
        testContext.endTest();
        System.out.println("[Test01] AddElement end\n");
        return true;
    }


    public boolean test2UpdateElement(TestContext testContext) {
        testContext.startTest("[Test02] UpdateElement");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        List<Long> loadedIds = testContext.getLoadedIds();

        int nShouldPass = loadedIds.size();
        List<ElementValue> elementsById = demoElementContainer.getElementsById(loadedIds);

        List<Boolean> updated = new ArrayList<>();
        int loopCount = loopCount(nShouldPass);
        System.out.println("开始更新" + nShouldPass * loopCount + "数据量...");
        long tp1 = System.nanoTime();

        for (int i = 0; i < loopCount; i++) {
            for (ElementValue elementValue : elementsById) {
                updated.add(demoElementContainer.updateElement(elementValue));
            }

        }
        double elapsed = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("更新" + nShouldPass * loopCount + "数据耗时: " + elapsed + "μs");

        System.out.println("开始验证数据...");
        int nPassed = 0;
        for (Boolean item : updated) {
            if (Boolean.TRUE.equals(item)) {
                nPassed++;
            }
        }

        if (nPassed != nShouldPass * loopCount) {
            System.out.println("[Test02] UpdateElement 验证失败");
            return false;
        }

        System.out.println("更新数据验证成功,开始记录指标数据...");
        testContext.recordSpeedOfTestPoint("UpdateElement", nShouldPass * loopCount, elapsed);
        testContext.endTest();
        System.out.println("[Test02] UpdateElement end\n");
        return true;
    }

    public boolean test3DeleteElement(TestContext testContext) {
        testContext.startTest("[Test03] DeleteElement");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //只删除一半
        List<Long> loadedIds = testContext.getLoadedIds();
        Long[] array = loadedIds.toArray(new Long[0]);

        int nShouldPass = loadedIds.size();

        int loopCount = loopCount(nShouldPass);
        System.out.println("开始删除" + nShouldPass * loopCount + "数据量...");
        long tp1 = System.nanoTime();
        for (int i = 0; i < loopCount; i++) {
            demoElementContainer.batchDeleteElement(array);

        }
        double elapsed = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("删除" + nShouldPass * loopCount + "数据耗时: " + elapsed + "μs");


        testContext.recordSpeedOfTestPoint("DeleteElement", nShouldPass * loopCount, elapsed);
        testContext.endTest();

        System.out.println("[Test03] DeleteElement end\n");

        return true;

    }


    public boolean test4AddElementParallelly(TestContext testContext) throws IOException, InterruptedException {
        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //loadIds这部分落库, notLoadIds这部门放到内存中
        List<Long> loadIds = new ArrayList<>();
        List<Long> notLoadIds = new ArrayList<>();
        testContext.loadHalfElements(loadIds, notLoadIds);

        //开始执行测试
        testContext.startTest("[Test04] AddElementParallelly");

        //获取数据
        TestContext.MetaAndContent metaAndContent = testContext.getMetaAndContents();
        int nShouldPass = metaAndContent.getElementValues().length;

        //开始批量保存数据,计算时间
        System.out.println("开始并行写入" + nShouldPass + "数据量...");
        List<ElementValue[]> elementValues = splitElementIndices(metaAndContent.getElementValues());
        CountDownLatch countDownLatch = new CountDownLatch(elementValues.size());
        long tp1 = System.nanoTime();
        for (ElementValue[] elementValue : elementValues) {
            executor.submit(() -> demoElementContainer.batchAddElementParallel(elementValue, countDownLatch));
        }
        double elapsed = (System.nanoTime() - tp1) / 1000.0;
        countDownLatch.await();
        System.out.println("并行写入" + nShouldPass + "数据耗时: " + elapsed + "μs");

        System.out.println("开始验证数据...");
        for (int i = 0; i < notLoadIds.size(); i++) {
            //加载到内存的那一部分数据
            ElementValue elementValue = demoElementContainer.getElementById(notLoadIds.get(i));
            if (Objects.isNull(elementValue)) {
                System.out.println("[Test04] AddElemen 验证失败");
                return false;
            }
        }
        System.out.println("插入数据验证成功,开始记录指标数据...");

        //文件大小
        testContext.recordSpeedOfTestPoint("AddElement", nShouldPass, elapsed);
        testContext.endTest();
        System.out.println("[Test04] AddElementParallelly end\n");
        return true;

    }


    public boolean test5UpdateElementParallelly(TestContext testContext) throws InterruptedException {
        testContext.startTest("[Test05] UpdateElementParallelly");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        List<Long> loadedIds = testContext.getLoadedIds();

        int nShouldPass = loadedIds.size();
        List<ElementValue> elementsById = demoElementContainer.getElementsById(loadedIds);

        List<ElementValue[]> elementValues = splitElementIndices(elementsById.toArray(new ElementValue[0]));
        CountDownLatch countDownLatch = new CountDownLatch(elementValues.size());

        System.out.println("开始并行更新" + nShouldPass + "数据量...");
        long tp1 = System.nanoTime();
        for (ElementValue[] elementValue : elementValues) {
            executor.submit(() -> demoElementContainer.updateElementParallel(elementValue, countDownLatch));
        }
        double elapsed = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("并行更新" + nShouldPass + "数据耗时: " + elapsed + "μs");
        countDownLatch.await();

        System.out.println("更新数据验证成功,开始记录指标数据...");
        testContext.recordSpeedOfTestPoint("UpdateElementParallelly", nShouldPass, elapsed);
        testContext.endTest();
        System.out.println("[Test05] UpdateElement end\n");
        return true;
    }


    public boolean test6DeleteElementParallelly(TestContext testContext) throws InterruptedException {
        testContext.startTest("[Test06] DeleteElementParallelly");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //只删除一半
        List<Long> loadedIds = testContext.getLoadedIds();

        int nShouldPass = loadedIds.size();

        List<Long[]> longs = splitDeleteLoadIds(loadedIds);
        CountDownLatch countDownLatch = new CountDownLatch(longs.size());
        System.out.println("开始并行删除" + nShouldPass + "数据量...");
        long tp1 = System.nanoTime();
        for (Long[] aLong : longs) {
            executor.submit(() -> demoElementContainer.batchDeleteElementParallel(aLong, countDownLatch));
        }
        double elapsed = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("并行删除" + nShouldPass + "数据耗时: " + elapsed + "μs");
        countDownLatch.await();

        testContext.recordSpeedOfTestPoint("DeleteElementParallelly", nShouldPass, elapsed);
        testContext.endTest();

        System.out.println("[Test06] DeleteElementParallelly end\n");
        return true;
    }


    public boolean test7QueryElementById(TestContext testContext) throws IOException {
        testContext.startTest("[Test07] QueryElementById");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //loadIds这部分落库, notLoadIds这部门放到内存中
        List<Long> loadIds = new ArrayList<>();
        List<Long> notLoadIds = new ArrayList<>();
        testContext.loadHalfElements(loadIds, notLoadIds);

        Long[] loadIdsArray = loadIds.toArray(new Long[0]);
        Long[] notloadIdsArray = notLoadIds.toArray(new Long[0]);

        int nShouldPass1 = loadIds.size();
        int nShouldPass2 = notLoadIds.size();

        // 测试点1：对于加载的对象，应该都能查询到
        int nPassed1 = 0;
        long tp1 = System.nanoTime();
        int loopCount = 10000000 / loadIdsArray.length;
        System.out.println("开始查询" + nShouldPass1 * loopCount + "数据量...");
        // 一共要执行1000万次,
        for (int i = 0; i < loopCount; i++) {
            for (Long loadId : loadIdsArray) {
                ElementValue elementValue = demoElementContainer.getElementById(loadId);
                if (Objects.nonNull(elementValue)) {
                    nPassed1++;
                }
            }
        }
        double elapsed = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("批量查询" + nShouldPass1 * loopCount + "数据耗时: " + elapsed + "μs");
        if (nShouldPass1 * loopCount == nPassed1) {
            testContext.recordSpeedOfTestPoint("QueryElementById", nShouldPass1 * loopCount, elapsed);
        }


        // 测试点2：对于没有预先加载的对象，也应该都能查询到(验证有延迟加载的能力）
        int nPassed2 = 0;
        int memoryUsedBefore = Utils.getMemoryUsed();
        int notloopCount = 10000000 / notloadIdsArray.length;
        System.out.println("开始查询" + nShouldPass2 * notloopCount + "数据量...");
        long tp2 = System.nanoTime();
        for (int i = 0; i < notloopCount; i++) {
            for (Long notLoadId : notloadIdsArray) {
                ElementValue elementValue = demoElementContainer.getElementById(notLoadId);
                if (Objects.nonNull(elementValue)) {
                    nPassed2++;
                }
            }
        }

        double elapsed2 = (System.nanoTime() - tp2) / 1000.0;
        int memoryUsedAfter = Utils.getMemoryUsed();
        System.out.println("批量查询" + nShouldPass2 * notloopCount + "数据耗时: " + elapsed2 + "μs");
        if (nShouldPass2 * notloopCount == nPassed2) {
            testContext.recordTotalElementSize("QueryElementByIdWithLazyLoad", testContext.getMetaAndContents().getnTotalElementSize());
            testContext.recordMemoryUsageOfTestPoint("QueryElementByIdWithLazyLoad", memoryUsedBefore, memoryUsedAfter);
            testContext.recordSpeedOfTestPoint("QueryElementByIdWithLazyLoad", nShouldPass2 * notloopCount, elapsed2);
        }

        if ((nShouldPass1 * loopCount + nShouldPass2 * notloopCount) == (nPassed1 + nPassed2)) {
            testContext.endTest();
        }
        System.out.println("[Test07] QueryElementById end\n");
        return true;
    }


    public boolean test8QueryElementsByIds(TestContext testContext) throws IOException {
        testContext.startTest("[Test08] QueryElementsByIds");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //loadIds这部分落库, notLoadIds这部门放到内存中
        List<Long> loadIds = new ArrayList<>();
        List<Long> notLoadIds = new ArrayList<>();
        testContext.loadHalfElements(loadIds, notLoadIds);


        int nLoaded = loadIds.size();
        int nNotLoaded = notLoadIds.size();

        int nShouldPass = nLoaded + nNotLoaded;
        int nPassed1 = 0;
        int nPassed2 = 0;
        int nPassed3 = 0;

        // 测试点1：对于加载的对象，批量应该都能查询到。取前一半用来测试
        List<Long> elemIds1 = new ArrayList<>();
        for (int i = 0; i < (nLoaded >> 1); ++i) {
            elemIds1.add((loadIds.get(i)));
        }
        long nIds1 = elemIds1.size();
        // 测试查询，并测量时间
        long tp1 = System.nanoTime();
        System.out.println("开始批量查询" + nIds1 + "数据量...");
        List<ElementValue> queried1 = demoElementContainer.getElementsById(elemIds1);
        double elapsed1 = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("批量查询" + nIds1 + "数据耗时: " + elapsed1 + "μs");
        if (elemIds1.size() == queried1.size()) {
            // 验证查询结果：2）内容
            for (int idx = 0; idx < nIds1; ++idx) {
                // 没查到
                if (null == queried1.get(idx)) {
                    continue;
                }

                // id不相等，查错了
                if (queried1.get(idx).getId() != elemIds1.get(idx)) {
                    continue;
                }
                ++nPassed1;
            }
        }

        if (nIds1 == nPassed1) {
            testContext.recordSpeedOfTestPoint("QueryElementsByIds", nPassed1, elapsed1);
        }

        // 测试点2：对于尚未加载对象，批量应该都能查询到。取前一半用来测试
        List<Long> elemIds2 = new ArrayList<>();
        for (int i = 0; i < (nNotLoaded >> 1); ++i) {
            elemIds2.add((notLoadIds.get(i)));
        }
        long nIds2 = elemIds2.size();
        // 测试查询，并测量时间
        long tp2 = System.nanoTime();
        System.out.println("开始批量查询" + nIds2 + "数据量...");
        List<ElementValue> queried2 = demoElementContainer.getElementsById(elemIds2);
        double elapsed2 = (System.nanoTime() - tp2) / 1000.0;
        System.out.println("批量查询" + nIds2 + "数据耗时: " + elapsed2 + "μs");
        if (elemIds2.size() == queried2.size()) {
            // 验证查询结果：2）内容
            for (int idx = 0; idx < nIds2; ++idx) {
                // 没查到
                if (null == queried2.get(idx)) {
                    continue;
                }

                // id不相等，查错了
                if (queried2.get(idx).getId() != elemIds2.get(idx)) {
                    continue;
                }
                ++nPassed2;
            }
        }

        if (nIds2 == nPassed2) {
            testContext.recordSpeedOfTestPoint("QueryElementByIdsWithLazyLoad", nPassed2, elapsed2);
        }

        // 测试点3：对于混合查询加载的/尚未加载的，批量应该都能查询到。
        List<Long> elemIds3 = new ArrayList<>();
        for (int i = (nLoaded >> 1); i < nLoaded; ++i) {
            elemIds3.add((loadIds.get(i)));
        }
        for (int i = (nNotLoaded >> 1); i < nNotLoaded; ++i) {
            elemIds3.add((notLoadIds.get(i)));
        }
        long nIds3 = elemIds3.size();

        long tp3 = System.nanoTime();
        System.out.println("开始批量查询" + nIds3 + "数据量...");
        List<ElementValue> queried3 = demoElementContainer.getElementsById(elemIds3);
        double elapsed3 = (System.nanoTime() - tp3) / 1000.0;
        System.out.println("批量查询" + nIds3 + "数据耗时: " + elapsed3 + "μs");
        if (elemIds3.size() == queried3.size()) {
            // 验证查询结果：2）内容
            for (int idx = 0; idx < nIds3; ++idx) {
                // 没查到
                if (null == queried3.get(idx)) {
                    continue;
                }

                // id不相等，查错了
                if (queried3.get(idx).getId() != elemIds3.get(idx)) {
                    continue;
                }
                ++nPassed3;
            }
        }

        if (nIds3 == nPassed3) {
            testContext.recordSpeedOfTestPoint("QueryElementByIdsMixPreLoadAndLazyLoad", nPassed3, elapsed3);
        }

        if (nShouldPass == (nPassed1 + nPassed2 + nPassed3)) {
            testContext.endTest();
        }

        System.out.println("[Test08] QueryElementsByIds end\n");
        return true;
    }


    private void parseMeta2ElementIds(TestContext.MetaAndContent metaAndContent,
                                      Map<String, Set<Long>> categoryToElementIds,
                                      Map<Integer, Set<Long>> tagToElementIds) {
        ElementValue[] elementValues = metaAndContent.getElementValues();

        for (ElementValue value : elementValues) {
            // 处理类别到元素ID的映射
            String categoryGuid = value.getCategory();
            categoryToElementIds.putIfAbsent(categoryGuid, new HashSet<>());
            categoryToElementIds.get(categoryGuid).add(value.getId());

            // 处理标签到元素ID的映射
            int tag = value.getTag();
            if (0 != tag && ElementValue.ElementMetaTagMask.TAG_0_MASK.mark() == tag) {
                tagToElementIds.putIfAbsent(ElementValue.ElementMetaTagMask.TAG_0_MASK.mark(), new HashSet<>());
                tagToElementIds.get(ElementValue.ElementMetaTagMask.TAG_0_MASK.mark()).add(value.getId());
            }
            if (0 != tag && ElementValue.ElementMetaTagMask.TAG_1_MASK.mark() == tag) {
                tagToElementIds.putIfAbsent(ElementValue.ElementMetaTagMask.TAG_1_MASK.mark(), new HashSet<>());
                tagToElementIds.get(ElementValue.ElementMetaTagMask.TAG_1_MASK.mark()).add(value.getId());
            }
        }
    }


    public boolean test9QueryElementsByTag(TestContext testContext) throws IOException {
        testContext.startTest("[Test09] QueryElementsByTag");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //把所有文件加载上来
        List<Long> ids = testContext.getAllIds();
        testContext.loadMetaAndContentIntoMemory(ids);

        Map<String, Set<Long>> categoryToElementIds = new HashMap<>();
        Map<Integer, Set<Long>> tagToElementIds = new HashMap<>();
        parseMeta2ElementIds(testContext.getMetaAndContents(), categoryToElementIds, tagToElementIds);


        int nShouldPass1 = 0;
        int nShouldPass2 = 0;
        if (tagToElementIds.get(ElementValue.ElementMetaTagMask.TAG_0_MASK.mark()) != null) {
            nShouldPass1 = tagToElementIds.get(ElementValue.ElementMetaTagMask.TAG_0_MASK.mark()).size();
        }

        if (tagToElementIds.get(ElementValue.ElementMetaTagMask.TAG_1_MASK.mark()) != null) {
            nShouldPass2 = tagToElementIds.get(ElementValue.ElementMetaTagMask.TAG_1_MASK.mark()).size();
        }

        // 测试点1: 按Tag0查询对象

        // 测试GetElementsByTags()，并测量时间
        ElementTagIndexValue tagIndexValue = ElementTagIndexValue.get(ValueInt.get(ElementValue.ElementMetaTagMask.TAG_0_MASK.mark()));
        long tp1 = System.nanoTime();
        System.out.println("开始按Tag0查询数据...");
        Set<ElementValue> elementsByTags = demoElementContainer.getElementsByTags(tagIndexValue);
        double elapsed1 = (System.nanoTime() - tp1) / 1000.0;
        System.out.println("按Tag0查询数据耗时: " + elapsed1 + "μs");

        int nPassed1 = 0;
        if (elementsByTags.size() == nShouldPass1) {
            // 验证查询结果：1）数量
            for (ElementValue elementValue : elementsByTags) {
                // 没查到
                if (null == elementValue) {
                    continue;
                }

                // id不相等，查错了
                if (ElementValue.ElementMetaTagMask.TAG_0_MASK.mark() != elementValue.getTag()) {
                    continue;
                }
                ++nPassed1;
            }
        }

        if (nShouldPass1 == nPassed1) {
            testContext.recordSpeedOfTestPoint("QueryElementsByTag1", nPassed1, elapsed1);
        }

        // 测试点2: 按Tag2查询对象

        // 测试GetElementsByTags()，并测量时间
        ElementTagIndexValue tagIndexValue1 = ElementTagIndexValue.get(ValueInt.get(ElementValue.ElementMetaTagMask.TAG_1_MASK.mark()));
        long tp2 = System.nanoTime();
        System.out.println("开始按Tag1查询数据...");
        Set<ElementValue> elementsByTags2 = demoElementContainer.getElementsByTags(tagIndexValue1);
        double elapsed2 = (System.nanoTime() - tp2) / 1000.0;
        System.out.println("按Tag1查询数据耗时: " + elapsed2 + "μs");

        int nPassed2 = 0;
        if (elementsByTags2.size() == nShouldPass2) {
            // 验证查询结果：1）数量
            for (ElementValue elementValue : elementsByTags2) {
                // 没查到
                if (null == elementValue) {
                    continue;
                }

                // id不相等，查错了
                if (ElementValue.ElementMetaTagMask.TAG_1_MASK.mark() != elementValue.getTag()) {
                    continue;
                }
                ++nPassed2;
            }
        }

        if (nShouldPass2 == nPassed2) {
            testContext.recordSpeedOfTestPoint("QueryElementsByTag2", nPassed2, elapsed2);
        }

        if ((nShouldPass1 + nShouldPass2) == (nPassed1 + nPassed2)) {
            testContext.endTest();
        }

        System.out.println("[Test09] QueryElementsByTag end\n");
        return true;
    }


    public boolean test10QueryElementsByCategory(TestContext testContext) throws IOException {
        testContext.startTest("[Test10] QueryElementsByCategory");

        DemoElementContainer demoElementContainer = testContext.getDemoElementContainer();
        if (Objects.isNull(demoElementContainer)) {
            return false;
        }

        //把所有文件加载上来
        List<Long> allIds = testContext.getAllIds();
        testContext.loadMetaAndContentIntoMemory(allIds);

        Map<String, Set<Long>> categoryToElementIds = new HashMap<>();
        Map<Integer, Set<Long>> tagToElementIds = new HashMap<>();
        parseMeta2ElementIds(testContext.getMetaAndContents(), categoryToElementIds, tagToElementIds);

        int nShouldPass1 = 0;
        int nPassed1 = 0;
        double elapsed1 = 0;
        for (Map.Entry<String, Set<Long>> integerSetEntry : categoryToElementIds.entrySet()) {
            String guid = integerSetEntry.getKey();
            Set<Long> ids = integerSetEntry.getValue();
            nShouldPass1 += ids.size();

            long tp = System.nanoTime();
            Set<ElementValue> elementsByCategory = demoElementContainer.getElementsByCategory(guid);
            elapsed1 += (System.nanoTime() - tp) / 1000.0;

            if (elementsByCategory.size() == ids.size()) {
                // 验证查询结果：1）数量
                for (ElementValue elementValue : elementsByCategory) {
                    // 没查到
                    if (null == elementValue) {
                        continue;
                    }

                    // id不相等，查错了
                    if (!ids.contains(elementValue.getId())) {
                        continue;
                    }
                    ++nPassed1;
                }
            }
        }

        if (nShouldPass1 == nPassed1) {
            testContext.recordSpeedOfTestPoint("QueryElementsByCategoryWithLazyLoad", nPassed1, elapsed1);
        }


        int nShouldPass2 = 0;
        int nPassed2 = 0;
        double elapsed2 = 0;
        for (Map.Entry<String, Set<Long>> integerSetEntry : categoryToElementIds.entrySet()) {
            String guid = integerSetEntry.getKey();
            Set<Long> ids = integerSetEntry.getValue();
            nShouldPass2 += ids.size();

            long tp = System.nanoTime();
            Set<ElementValue> elementsByCategory = demoElementContainer.getElementsByCategory(guid);
            elapsed2 += (System.nanoTime() - tp) / 1000.0;

            if (elementsByCategory.size() == ids.size()) {
                // 验证查询结果：1）数量
                for (ElementValue elementValue : elementsByCategory) {
                    // 没查到
                    if (null == elementValue) {
                        continue;
                    }

                    // id不相等，查错了
                    if (!ids.contains(elementValue.getId())) {
                        continue;
                    }
                    ++nPassed2;
                }
            }
        }

        if (nShouldPass2 == nPassed2) {
            testContext.recordSpeedOfTestPoint("QueryElementsByCategory", nPassed2, elapsed2);
        }

        if (nShouldPass2 == nPassed2) {
            testContext.endTest();
        }

        System.out.println("[Test10] QueryElementsByCategory end\n");
        return true;
    }


    public List<ElementValue[]> splitElementIndices(ElementValue[] elementValues) {
        int totalNum = elementValues.length;
        int batchSize = totalNum / CPU_COUNT;
        int batchNum = CPU_COUNT;
        int lastBatchSize = totalNum % batchSize;

        int totalBatchCount = lastBatchSize > 0 ? batchNum + 1 : batchNum;
        List<ElementValue[]> elementIndicesList = new ArrayList<>(totalBatchCount);

        // 处理完整的批次
        for (int i = 0; i < batchNum; i++) {
            int from = i * batchSize;
            int to = from + batchSize;
            ElementValue[] batchElementValues = Arrays.copyOfRange(elementValues, from, to);
            elementIndicesList.add(batchElementValues);
        }

        // 处理最后不完整的批次
        if (lastBatchSize > 0) {
            int from = batchNum * batchSize;
            int to = from + lastBatchSize;
            ElementValue[] batchElementValues = Arrays.copyOfRange(elementValues, from, to);
            elementIndicesList.add(batchElementValues);
        }

        return elementIndicesList;
    }

    public List<Long[]> splitDeleteLoadIds(List<Long> loadedIds) {
        int totalNum = loadedIds.size();
        int batchSize = totalNum / CPU_COUNT;
        int batchNum = CPU_COUNT;
        int lastBatchSize = totalNum % batchSize;

        int totalBatchCount = lastBatchSize > 0 ? batchNum + 1 : batchNum;
        List<Long[]> elementIndicesList = new ArrayList<>(totalBatchCount);

        // 处理完整的批次
        for (int i = 0; i < batchNum; i++) {
            int from = i * batchSize;
            int to = from + batchSize;
            elementIndicesList.add(loadedIds.subList(from, to).toArray(new Long[0]));
        }

        // 处理最后不完整的批次
        if (lastBatchSize > 0) {
            int from = batchNum * batchSize;
            int to = from + lastBatchSize;
            elementIndicesList.add(loadedIds.subList(from, to).toArray(new Long[0]));
        }

        return elementIndicesList;

    }


    private int loopCount(int length) {
        //length和5000比较,如果大于5000返回1,如果小于5万就用(5000/length)+1
        return length > 50000 ? 1 : (50000 / length) + 1;
    }

}

