package com.glodon.testing.test;

import com.glodon.container.api.DemoElementContainer;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * @program: november-2nd
 * @description:
 * @author: hons.chang
 * @since: 2023-10-26 13:49
 **/
public class TestRunner {

//    private static final String workDir = System.getProperty("test.dataPath");
//    private static final String workDir = System.getProperty("D:\\run");
//    private static final String workDir = "/Users/changhongsheng/work/hm/run";

//    public static final String testData = workDir + File.separator + "TestData";
    public static final String testData = "D:\\run\\TestData";

    private static final String tableData = "D:\\run\\data";

    public static final String talbeName = "my_table";


    public static void run() throws IOException, InterruptedException {
        System.out.println(testData);
        System.out.println(tableData);
        Test test = new Test();
        DemoElementContainer demoElementContainer = new DemoElementContainer();
        demoElementContainer.init(tableData, talbeName);

        File exeFile = new File(testData);
        File[] files = exeFile.listFiles();
        if (Objects.isNull(files)) {
            System.out.println("test data file is null");
            return;
        }

        //先预热一下
        for (int i = 0; i < 2; i++) {
            for (File curFile : files) {
                if (curFile.getName().startsWith(".")) {
                    continue;
                }
                TestContext testContext = new TestContext(curFile, demoElementContainer);

                System.out.println("test file: " + curFile.getName());

                //group 1
                testContext.resetContainer();
                test.test1AddElement(testContext);
                test.test2UpdateElement(testContext);
                test.test3DeleteElement(testContext);

                // group 2
                testContext.resetContainer();
                test.test4AddElementParallelly(testContext);
                test.test5UpdateElementParallelly(testContext);
                test.test6DeleteElementParallelly(testContext);

                // group 3
                testContext.resetContainer();
                test.test7QueryElementById(testContext);
                // group 4
                testContext.resetContainer();
                test.test8QueryElementsByIds(testContext);
                // group 5
                testContext.resetContainer();
                test.test9QueryElementsByTag(testContext);
                // group 6
                testContext.resetContainer();
                test.test10QueryElementsByCategory(testContext);
                //打印报告
                testContext.dumpTestResults();

                testContext.resetAllIds();
                demoElementContainer.truncate();

            }
        }
        //关闭容器
        demoElementContainer.close();
        test.close();
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        run();
        System.out.println("test end");
    }
}
