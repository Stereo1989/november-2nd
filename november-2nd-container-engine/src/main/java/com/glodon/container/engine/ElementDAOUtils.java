package com.glodon.container.engine;

import com.glodon.base.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.UUID;

/**
 * @program: november-2nd
 * @description:
 * @author: hons.chang
 * @since: 2023-10-26 09:58
 **/
public class ElementDAOUtils {

    /**
     * 生成ElementValue
     */
    public static ElementValue loadMetaAndContent(File metaFile, File contentFile) throws IOException {
        byte[] metaBinary = FileUtil.readBytes(metaFile);
        long length = contentFile.length();
        if (ElementValue.USE_FILE_SIZE < length) {
            return ElementValue.fromBinary(metaBinary, null, contentFile.getAbsolutePath());
        } else {
            byte[] contentBinary = FileUtil.readBytes(contentFile);
            return ElementValue.fromBinary(metaBinary, contentBinary, contentFile.getAbsolutePath());
        }
    }


    private final static String DATA = "随着人工智能(AI)技术的不断发展，它在设计领域的应用也日益广泛。一种人工智能辅助设计(AiAD)方式是，设计师带领多个AI助手，相当于多个手速超快的人类助手同时进行设计。\n" +
            "\n" +
            "为了应对这样的场景，作为支撑设计类软件的底层平台，广联达设计建模平台(GDMP)需要从支持单一模型修改来源，以及相对低频的多个模型修改来源（对应正常手速的多个人类设计师），扩展到支持高频并发的多个模型修改来源（对应超快手速的多个AI助手）。目前市场上的主流图形平台均服务于人类设计师交互特点，不支持这一特性。如果这项技术能成功推出，将是GDMP未来的核心竞争力之一。\n" +
            "\n" +
            "上述特性是一个体系的改进，本黑马主题关注的是其中一项基础能力：GDMP数据层能力增强。希望参赛队伍能够设计并实现一个满足下述功能和性能要求的内存型数据容器，用于支撑高频并发的设计场景。\n" +
            "\n" +
            "从功能角度看，该内存数据容器需要能存放承载设计业务数据的可持久化的C++数据对象(Element)，需要能够按照对象的唯一ID(ElementId)在数据容器中进行查找。还需要能提供按照对象摘要数据(Meta)中的各个字段进行快速查找的能力。同时，为了支持按需加载，在数据容器中存放的也可能是对象的二进制原始数据(Content)，在上层需要时才转换为数据对象(DTO)。\n" +
            "\n" +
            "从性能角度看，因数据对象会在并发环境下查询、加载、产生、修改和删除，对数据容器的吞吐能力有非常高的要求。希望在并发环境下也对使用者提供接近串行环境下的吞吐率，并且确保正确性。";

    public static void main(String[] args) {
        Random random = new Random();
        UUID categoryUUID = UUID.randomUUID();
        long start = 123411502200L;
        long end = 123411507200L;
        long uuidCount = end / 50;


        String path = "/Users/changhongsheng/work/hm/workdir3/test4/";

        for (long i = start; i <= end; i++) {
            // 生成id.meta文件
            long id = i;
            //每20000/50=400个元素，使用一个新的categoryUUID
            if (i % uuidCount == 0) {
                categoryUUID = UUID.randomUUID();
            }
            //如果path不存在就创建
            File file = new File(path);
            if (!file.exists()) {
                file.mkdirs();
            }

            // 随机生成 0, 1 或 2
            int tag = random.nextInt(3);

            // 准备数据写入缓冲区
            ByteBuffer buffer = ByteBuffer.allocate(28).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(id);
            buffer.putLong(categoryUUID.getMostSignificantBits());
            buffer.putLong(categoryUUID.getLeastSignificantBits());
            buffer.putInt(tag);

            // 写入id.meta文件
            try (FileOutputStream fos = new FileOutputStream(path + id + ".meta")) {
                fos.write(buffer.array());
            } catch (IOException e) {
                e.printStackTrace();
            }

            // 生成id.content文件
            try (FileOutputStream fos = new FileOutputStream(path + id + ".content")) {
                fos.write(DATA.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Data generation completed.");
    }

}
