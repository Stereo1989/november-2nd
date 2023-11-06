package com.glodon.container.engine;

import com.glodon.base.conf.Config;
import com.glodon.base.table.Scanner;
import com.glodon.base.table.Tablet;
import com.glodon.base.value.ValueInt;
import com.glodon.base.value.ValueUuid;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * -server -Xmx6g -Xms6g -XX:NewSize=1g -Xss512k -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
 * <p>
 * Created by liujing on 2023/10/18.
 */
public class ElementTabletTest {

    protected Tablet<Long, ElementValue> tablet;
    private final String DATA = "随着人工智能(AI)技术的不断发展，它在设计领域的应用也日益广泛。一种人工智能辅助设计(AiAD)方式是，设计师带领多个AI助手，相当于多个手速超快的人类助手同时进行设计。\n" +
            "\n" +
            "为了应对这样的场景，作为支撑设计类软件的底层平台，广联达设计建模平台(GDMP)需要从支持单一模型修改来源，以及相对低频的多个模型修改来源（对应正常手速的多个人类设计师），扩展到支持高频并发的多个模型修改来源（对应超快手速的多个AI助手）。目前市场上的主流图形平台均服务于人类设计师交互特点，不支持这一特性。如果这项技术能成功推出，将是GDMP未来的核心竞争力之一。\n" +
            "\n" +
            "上述特性是一个体系的改进，本黑马主题关注的是其中一项基础能力：GDMP数据层能力增强。希望参赛队伍能够设计并实现一个满足下述功能和性能要求的内存型数据容器，用于支撑高频并发的设计场景。\n" +
            "\n" +
            "从功能角度看，该内存数据容器需要能存放承载设计业务数据的可持久化的C++数据对象(Element)，需要能够按照对象的唯一ID(ElementId)在数据容器中进行查找。还需要能提供按照对象摘要数据(Meta)中的各个字段进行快速查找的能力。同时，为了支持按需加载，在数据容器中存放的也可能是对象的二进制原始数据(Content)，在上层需要时才转换为数据对象(DTO)。\n" +
            "\n" +
            "从性能角度看，因数据对象会在并发环境下查询、加载、产生、修改和删除，对数据容器的吞吐能力有非常高的要求。希望在并发环境下也对使用者提供接近串行环境下的吞吐率，并且确保正确性。";

    void init() {
        tablet = new ElementTabletImpl("/Users/liujing/november-2nd", "my_table");
        tablet.init(Config.DEFAULT);
    }

    void close() {
        tablet.close();
    }

    static class Metrics {
        long write_time;//写入100W耗时ms
        long update_time;//更新100W耗时ms
        long save_time;// 落盘耗时ms
        long remove_time;//按id删除100W数据耗时ms
        long all_table_scan;//全表遍历扫描耗时
        long id_point_query_time;//按id点查询1000Wci耗时ms
        long tag_query_time;// 按tag查询100W次耗时ms
        long category_query_time;// 按类别查询100W次耗时ms
    }

    Metrics testElementValue() throws InterruptedException {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>> 开始测试...");
        Metrics metrics = new Metrics();
        System.out.println("准备写入数据...");
        int from = 1000000, to = 2000000, tag_step = 100000;
        String[] categoryArr = new String[100000];
        for (int i = 0; i < categoryArr.length; i++) {
            categoryArr[i] = UUID.randomUUID().toString();
        }
        final ElementValue[] elementValues = new ElementValue[to - from];
        int tag = 1, tagCount = 0;
        for (int i = from; i < to; i++) {
            if (tagCount < tag_step) {
                tagCount++;
            } else {
                tag++;
                tagCount = 1;
            }
            long id = i;
            String category = categoryArr[i % categoryArr.length];//new UUID(Long.valueOf(i), Long.valueOf(i)).toString();
            ElementValue value = new ElementValue(id, category, tag, "/Users/changhongsheng/work/hm/data", DATA.getBytes(StandardCharsets.UTF_8));
            elementValues[i - from] = value;
        }
        Arrays.sort(elementValues);
        Thread.sleep(1000L);
        System.out.println("开始写入100W数据量...");
        long start = System.currentTimeMillis();
        tablet.insert(elementValues);
        metrics.write_time = System.currentTimeMillis() - start;
        System.out.println("写入100W数据耗时: " + metrics.write_time + "ms");

        System.out.println("准备开始测试更新...");
        Thread.sleep(1000L);
        System.out.println("开始更新100W数据量...");
        start = System.currentTimeMillis();
        tablet.update(elementValues);
        metrics.update_time = System.currentTimeMillis() - start;
        System.out.println("更新100W数据耗时: " + metrics.update_time + "ms");

//        System.out.println("开始保存数据...");
//        Thread.sleep(1000L);
//        start = System.currentTimeMillis();
//        tablet.checkpoint();
//        metrics.save_time = System.currentTimeMillis() - start;
//        System.out.println("保存数据耗时: " + metrics.save_time + "ms");

        System.out.println("准备开始测试查询...");
        Thread.sleep(1000L);
        int rand = ThreadLocalRandom.current().nextInt(elementValues.length);
        ElementValue randomValue = elementValues[rand];
        ElementTagIndexValue tagIndexValue = ElementTagIndexValue.get(ValueInt.get(randomValue.getTag()));
        UUID categoryUuid = UUID.fromString(randomValue.getCategory());
        ElementCategoryIndexValue categoryIndexValue = ElementCategoryIndexValue.get(ValueUuid.get(categoryUuid.getMostSignificantBits(), categoryUuid.getLeastSignificantBits()));
        System.out.println("随机抽取数据: tag = " + randomValue.getTag() + ", category = " + randomValue.getCategory() + ", id = " + randomValue.getId());

        //全表扫描
        Thread.sleep(1000L);
        start = System.currentTimeMillis();
        AtomicLong bytesCount = new AtomicLong();
        tablet.scan(new Scanner<Long, ElementValue>() {
            @Override
            public void handle(Long id, ElementValue elementValue) {
                bytesCount.set(bytesCount.get() + elementValue.getContent().length);
            }
        });
        metrics.all_table_scan = System.currentTimeMillis() - start;
        System.out.println("全表扫描完成耗时: " + metrics.all_table_scan + "ms, 字节大小: " + bytesCount.get() / 1024 / 1024 + "MB");

        //串行按id点查1000W次
        Thread.sleep(1000L);
        Long[] idArr = new Long[10000];
        for (int i = 0, j = 0; i < 10000; ) {
            if (elementValues[j].getId() % 2 == 1) {
                idArr[i] = elementValues[j].getId();
                i++;
            }
            j++;
        }
        Arrays.sort(idArr);
        start = System.currentTimeMillis();
        for (int j = 0; j < 1000; j++) {
            for (int i = 0; i < idArr.length; i++) {
                ElementValue v = tablet.select(idArr[i]);
            }
        }
        metrics.id_point_query_time = System.currentTimeMillis() - start;
        System.out.println("按id点查询1000万次完成耗时: " + metrics.id_point_query_time + "ms");

        //串行按tag查询100W次
        Thread.sleep(1000L);
        start = System.currentTimeMillis();
        int inCount = 0;
        for (int i = 0; i < 1000000; i++) {
            Set<ElementValue> elementValueList = tablet.in(tagIndexValue);
            inCount = elementValueList.size();
        }
        metrics.tag_query_time = System.currentTimeMillis() - start;
        System.out.println("按tag查询100万次完成耗时: " + metrics.tag_query_time + "ms" + ", 实际结果数量: " + inCount);

        //串行按category查询100W次
        Thread.sleep(1000L);
        start = System.currentTimeMillis();
        inCount = 0;
        for (int i = 0; i < 1000000; i++) {
            Set<ElementValue> elementValueList = tablet.in(categoryIndexValue);
            inCount = elementValueList.size();
        }
        metrics.category_query_time = System.currentTimeMillis() - start;
        System.out.println("按category查询100万次完成耗时: " + metrics.category_query_time + "ms" + ", 实际结果数量: " + inCount);

        //串行删除100W数据
        System.out.println("准备开始删除数据...");
        Thread.sleep(1000L);
        //start = System.currentTimeMillis();
        //for (int i = from; i < to; i++) {
        //    long id = i;
        //    tablet.delete(id);
        //}
        Long[] ids = new Long[elementValues.length];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = elementValues[i].getId();
        }
        Arrays.sort(ids);
        start = System.currentTimeMillis();
        tablet.delete(ids);
        metrics.remove_time = System.currentTimeMillis() - start;
        System.out.println("删除数据耗时: " + metrics.remove_time + "ms");

        tablet.truncate();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>> 完成测试清空数据...");
        return metrics;
    }

    public static void main(String[] args) throws Exception {
        ElementTabletTest test = new ElementTabletTest();
        test.init();
        int count = 3;
        List<Metrics> metricsList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            metricsList.add(test.testElementValue());
        }
        long write_time = 0, remove_time = 0, category_query_time = 0, tag_query_time = 0, id_point_query_time = 0;
        for (Metrics m : metricsList) {
            write_time += m.write_time;
            remove_time += m.remove_time;
            category_query_time += m.category_query_time;
            tag_query_time += m.tag_query_time;
            id_point_query_time += m.id_point_query_time;
        }
        System.out.println("写入100W数据平均耗时: " + Math.round(write_time / count) + "ms");
        System.out.println("删除100W数据平均耗时: " + Math.round(remove_time / count) + "ms");
        System.out.println("按category查询100W次平均耗时: " + Math.round(category_query_time / count) + "ms");
        System.out.println("按tag查询100W次平均耗时: " + Math.round(tag_query_time / count) + "ms");
        System.out.println("按id查询1000W次平均耗时: " + Math.round(id_point_query_time / count) + "ms");
        test.close();
    }
}
