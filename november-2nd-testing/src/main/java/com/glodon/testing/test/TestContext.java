package com.glodon.testing.test;

import com.glodon.container.api.DemoElementContainer;
import com.glodon.container.engine.ElementCategoryIndexValue;
import com.glodon.container.engine.ElementDAOUtils;
import com.glodon.container.engine.ElementTagIndexValue;
import com.glodon.container.engine.ElementValue;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * @program: november-2nd
 * @description:
 * @author: hons.chang
 * @since: 2023-10-27 14:26
 **/
public class TestContext {

    public TestContext() {
    }

    public TestContext(File dataFile, DemoElementContainer demoElementContainer) {
        this.dataFile = dataFile;
        this.demoElementContainer = demoElementContainer;
    }

    private File dataFile;

    private List<Long> allIds;
    private List<Long> loadedIds;
    private DemoElementContainer demoElementContainer;

    private MetaAndContent metaAndContents;

    private String testName;

    private long startTime;

    /**
     * 正确性,测试用例执行时间
     */
    private LinkedHashMap<String, Pair> testResults;

    /**
     * 速度
     */
    private LinkedHashMap<String, List<SpeedMeasurement>> m_speedMeasurementsOfTestPoints;

    /**
     * 内存 java 获取不到  作集大小（working set size）或页面文件使用量  暂时使用jvm内存代替
     */
    private LinkedHashMap<String, List<MemoryMeasurement>> m_memoryMeasurementsOfTestPoints;
    /**
     * 文件大小
     */
    private LinkedHashMap<String, List<ElementMeasurement>> m_elementMeasurementsOfTestPoints;


    public void resetContainer() {
        // 清空
        this.loadedIds = new ArrayList<>();
        this.metaAndContents = new MetaAndContent();
    }

    public void resetAllIds() {
        // 清空
        this.allIds = new ArrayList<>();

    }


    void startTest(String testName) {
        this.testName = testName;
        if (this.testResults == null) {
            this.testResults = new LinkedHashMap<>();
        }
        testResults.put(testName, new Pair(false));
        this.startTime = System.nanoTime();
    }

    /**
     * 记录文件大小
     */
    public void recordTotalElementSize(String testName, long totalElementSize) {
        System.out.println("testName = " + testName + ", totalElementSize = " + totalElementSize);
        if (Objects.isNull(this.getM_elementMeasurementsOfTestPoints())) {
            this.m_elementMeasurementsOfTestPoints = new LinkedHashMap<>();
        }
        if (Objects.isNull(m_elementMeasurementsOfTestPoints.get(this.testName))) {
            m_elementMeasurementsOfTestPoints.put(this.testName, new ArrayList<>());
        }
        m_elementMeasurementsOfTestPoints.get(this.testName).add(new ElementMeasurement(testName, totalElementSize));
    }

    /**
     * 记录速度
     */
    public void recordSpeedOfTestPoint(String testName, long count, double durationInMicroSecond) {
        System.out.println("testName = " + testName + ", count = " + count + ", durationInMicroSecond = " + durationInMicroSecond);
        if (Objects.isNull(this.getM_speedMeasurementsOfTestPoints())) {
            this.m_speedMeasurementsOfTestPoints = new LinkedHashMap<>();
        }
        if (Objects.isNull(m_speedMeasurementsOfTestPoints.get(this.testName))) {
            m_speedMeasurementsOfTestPoints.put(this.testName, new ArrayList<>());
        }
        m_speedMeasurementsOfTestPoints.get(this.testName).add(new SpeedMeasurement(testName, count * 1e6 / durationInMicroSecond, count, durationInMicroSecond));
    }

    /**
     * 记录内存使用
     */
    public void recordMemoryUsageOfTestPoint(String testName, int memoryUsedBefore, int memoryUsedAfter) {
        System.out.println("testName = " + testName + ", memoryUsedBefore = " + memoryUsedBefore + ", memoryUsedAfter = " + memoryUsedAfter);
        if (Objects.isNull(this.getM_memoryMeasurementsOfTestPoints())) {
            this.m_memoryMeasurementsOfTestPoints = new LinkedHashMap<>();
        }

        if (Objects.isNull(m_memoryMeasurementsOfTestPoints.get(this.testName))) {
            m_memoryMeasurementsOfTestPoints.put(this.testName, new ArrayList<>());
        }

        m_memoryMeasurementsOfTestPoints.get(this.testName).add(new MemoryMeasurement(testName, memoryUsedAfter - memoryUsedBefore));
    }


    /**
     * 记录执行结果
     */
    public void endTest() {
        Pair pair = testResults.get(testName);
        pair.setFirst(true);
        pair.setSecond((System.nanoTime() - startTime) / 1000.0);

    }


    public void loadHalfElements(List<Long> loadIds, List<Long> notLoadIds) throws IOException {
        List<Long> elementIds = getIds();
        //把id分成两部分
        divideElementsBeforeTests(elementIds, loadIds, notLoadIds);

        ExecutorService executorService = Executors.newFixedThreadPool(50);

        List<Future<ElementValue>> futures = new ArrayList<>();

        for (int i = 0; i < loadIds.size(); i++) {
            final int index = i;
            Future<ElementValue> future = executorService.submit(() -> {
                Long loadId = loadIds.get(index);
                File metaFile = new File(dataFile, loadId + ".meta");
                File contentFile = new File(dataFile, loadId + ".content");
                try {
                    ElementValue elementValue = ElementDAOUtils.loadMetaAndContent(metaFile, contentFile);
                    if (elementValue != null) {
                        return elementValue;
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
            futures.add(future);
        }


        List<ElementValue> elementValues = new ArrayList<>();
        for (Future<ElementValue> future : futures) {
            try {
                ElementValue elementValue = future.get();
                if (elementValue != null) {
                    elementValues.add(elementValue);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();
        try {
            // 等待所有任务完成，或者超时
            while (!executorService.awaitTermination(10, TimeUnit.MILLISECONDS)) ;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 重新设置中断状态
        }

        elementValues.sort(Comparator.comparingLong(ElementValue::getId));

        for (ElementValue elementValue : elementValues) {
            recordLoadedElementId(elementValue.getId());
        }

        demoElementContainer.batchAddElement(elementValues.toArray(new ElementValue[0]));

        loadMetaAndContentIntoMemory(notLoadIds);
    }

    public List<Long> getIds() {
        if (Objects.nonNull(this.allIds)) {
            return this.allIds;
        }
        List<Long> elementIds = new ArrayList<>();

        Path dataFolderPath = Paths.get(this.getDataFile().getAbsolutePath());
        if (!Files.exists(dataFolderPath) || !Files.isDirectory(dataFolderPath)) {
            return elementIds;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        // 使用线程安全的队列来存储结果
        ConcurrentLinkedQueue<Long> concurrentElementIds = new ConcurrentLinkedQueue<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataFolderPath)) {
            for (Path filePath : stream) {
                executorService.submit(() -> {
                    try {
                        if (!Files.isDirectory(filePath)) {
                            String fileName = filePath.getFileName().toString();
                            if (fileName.endsWith(".meta")) {
                                long id = Long.parseLong(fileName.substring(0, fileName.length() - 5));
                                concurrentElementIds.add(id);
                            }
                        }
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        executorService.shutdown();
        try {
            // 等待所有任务完成，或者超时
            while (!executorService.awaitTermination(10, TimeUnit.MILLISECONDS)) ;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 重新设置中断状态
        }

        // 将线程安全队列中的结果添加到列表中
        elementIds.addAll(concurrentElementIds);
        Collections.sort(elementIds);
        this.allIds = elementIds;

        return elementIds;
    }


    private void recordLoadedElementId(Long loadId) {
        List<Long> elementIds = this.getLoadedIds();
        if (elementIds == null) {
            this.loadedIds = new ArrayList<>();
        }
        this.loadedIds.add(loadId);
    }

    private void divideElementsBeforeTests(List<Long> elementIds, List<Long> loadIds, List<Long> notLoadIds) {
        for (Long elementId : elementIds) {
            if (elementId % 2 == 0) {
                loadIds.add(elementId);
            } else {
                notLoadIds.add(elementId);
            }
        }
        Collections.sort(loadIds);
        Collections.sort(notLoadIds);
    }

    public void loadMetaAndContentIntoMemory(List<Long> toAddElementIds) throws IOException {
        ElementValue[] elementValues = new ElementValue[toAddElementIds.size()];

        long nTotalElementSize = 0;
        for (int i = 0; i < toAddElementIds.size(); i++) {
            File metaFile = new File(dataFile, toAddElementIds.get(i) + ".meta");
            File contentFile = new File(dataFile, toAddElementIds.get(i) + ".content");
            nTotalElementSize = metaFile.length() + contentFile.length();
            ElementValue elementValue = ElementDAOUtils.loadMetaAndContent(metaFile, contentFile);
            elementValues[i] = elementValue;
        }

        Arrays.sort(elementValues);

        this.metaAndContents = new MetaAndContent(elementValues, nTotalElementSize);
    }

    public void setLoadedIds(List<Long> loadedIds) {
        this.loadedIds = loadedIds;
    }

    public List<Long> getLoadedIds() {
        return loadedIds;
    }

    public List<Long> getAllIds() {
        return allIds;
    }

    public void setAllIds(List<Long> allIds) {
        this.allIds = allIds;
    }

    public MetaAndContent getMetaAndContents() {
        return metaAndContents;
    }

    public void setMetaAndContents(MetaAndContent metaAndContents) {
        this.metaAndContents = metaAndContents;
    }

    public File getDataFile() {
        return dataFile;
    }

    public void setDataFile(File dataFile) {
        this.dataFile = dataFile;
    }

    public DemoElementContainer getDemoElementContainer() {
        return demoElementContainer;
    }

    public void setDemoElementContainer(DemoElementContainer demoElementContainer) {
        this.demoElementContainer = demoElementContainer;
    }


    public String getTestName() {
        return testName;
    }

    public void setTestName(String testName) {
        this.testName = testName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public Map<String, Pair> getTestResults() {
        return testResults;
    }

    public void setTestResults(LinkedHashMap<String, Pair> testResults) {
        this.testResults = testResults;
    }

    public Map<String, List<SpeedMeasurement>> getM_speedMeasurementsOfTestPoints() {
        return m_speedMeasurementsOfTestPoints;
    }

    public void setM_speedMeasurementsOfTestPoints(LinkedHashMap<String, List<SpeedMeasurement>> m_speedMeasurementsOfTestPoints) {
        this.m_speedMeasurementsOfTestPoints = m_speedMeasurementsOfTestPoints;
    }

    public Map<String, List<MemoryMeasurement>> getM_memoryMeasurementsOfTestPoints() {
        return m_memoryMeasurementsOfTestPoints;
    }

    public void setM_memoryMeasurementsOfTestPoints(LinkedHashMap<String, List<MemoryMeasurement>> m_memoryMeasurementsOfTestPoints) {
        this.m_memoryMeasurementsOfTestPoints = m_memoryMeasurementsOfTestPoints;
    }

    public Map<String, List<ElementMeasurement>> getM_elementMeasurementsOfTestPoints() {
        return m_elementMeasurementsOfTestPoints;
    }

    public void setM_elementMeasurementsOfTestPoints(LinkedHashMap<String, List<ElementMeasurement>> m_elementMeasurementsOfTestPoints) {
        this.m_elementMeasurementsOfTestPoints = m_elementMeasurementsOfTestPoints;
    }

    static class SpeedMeasurement {
        private String name;
        private double speedPerSecond;
        private long count;
        private double durationInMicroSecond;

        public SpeedMeasurement(String name, double speedPerSecond, long count, double durationInMicroSecond) {
            this.name = name;
            this.speedPerSecond = speedPerSecond;
            this.count = count;
            this.durationInMicroSecond = durationInMicroSecond;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getSpeedPerSecond() {
            return speedPerSecond;
        }

        public void setSpeedPerSecond(double speedPerSecond) {
            this.speedPerSecond = speedPerSecond;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public double getDurationInMicroSecond() {
            return durationInMicroSecond;
        }

        public void setDurationInMicroSecond(double durationInMicroSecond) {
            this.durationInMicroSecond = durationInMicroSecond;
        }

    }

    static class MemoryMeasurement {
        private String name;
        private long workingSetSize;

        public MemoryMeasurement(String name, long workingSetSize) {
            this.name = name;
            this.workingSetSize = workingSetSize;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getWorkingSetSize() {
            return workingSetSize;
        }

        public void setWorkingSetSize(long workingSetSize) {
            this.workingSetSize = workingSetSize;
        }
    }


    static class ElementMeasurement {
        private String name;
        private long totalElementSize;


        public ElementMeasurement(String name, long totalElementSize) {
            this.name = name;
            this.totalElementSize = totalElementSize;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getTotalElementSize() {
            return totalElementSize;
        }

        public void setTotalElementSize(long totalElementSize) {
            this.totalElementSize = totalElementSize;
        }
    }

    static class Pair {
        /**
         * 正确性
         */
        public boolean first;
        /**
         * 测试用例执行时间
         */
        public double second;

        public Pair() {
        }

        public Pair(boolean first) {
            this.first = first;
        }

        public Pair(boolean first, double second) {
            this.first = first;
            this.second = second;
        }

        public boolean getFirst() {
            return first;
        }

        public void setFirst(boolean first) {
            this.first = first;
        }

        public double getSecond() {
            return second;
        }

        public void setSecond(double second) {
            this.second = second;
        }
    }


    static class MetaAndContent {
        private ElementValue[] elementValues;

        private long nTotalElementSize;

        public MetaAndContent() {
        }

        public MetaAndContent(ElementValue[] elementValues, long nTotalElementSize) {
            this.elementValues = elementValues;
            this.nTotalElementSize = nTotalElementSize;
        }

        public ElementValue[] getElementValues() {
            return elementValues;
        }

        public void setElementValues(ElementValue[] elementValues) {
            this.elementValues = elementValues;
        }

        public long getnTotalElementSize() {
            return nTotalElementSize;
        }

        public void setnTotalElementSize(long nTotalElementSize) {
            this.nTotalElementSize = nTotalElementSize;
        }

    }

    public static void main(String[] args) {
        int count = 10000;
        double durationInMicroSecond = 17844.23;
        double speed = count * 1e6 / durationInMicroSecond;
        System.out.println("speed = " + speed / 1000000);

    }

    public void dumpTestResults() {
        Map<String, Pair> testResults = this.getTestResults();
        StringBuilder oss = new StringBuilder();
        for (Map.Entry<String, Pair> item : testResults.entrySet()) {
            oss.append(item.getKey());
            if (Boolean.TRUE.equals(item.getValue().getFirst())) {
                oss.append(" PASSED ");
            } else {
                oss.append(" FAILED ");
            }

            List<SpeedMeasurement> speedMeasurements = m_speedMeasurementsOfTestPoints.get(item.getKey());
            List<MemoryMeasurement> memoryMeasurements = m_memoryMeasurementsOfTestPoints.get(item.getKey());
            List<ElementMeasurement> elementMeasurements = m_elementMeasurementsOfTestPoints.get(item.getKey());

            oss.append("which costs " + item.getValue().getSecond() + " micro seconds.\n");
            if (speedMeasurements != null && !speedMeasurements.isEmpty()) {
                for (SpeedMeasurement speedItem : speedMeasurements) {
                    oss.append("\t[Performance] Speed of ")
                            .append(speedItem.getName())
                            .append(" is ")
                            .append(speedItem.getSpeedPerSecond() * 1e-6)
                            .append(" million/s with count=")
                            .append(speedItem.getCount())
                            .append(" and duration=")
                            .append(speedItem.getDurationInMicroSecond())
                            .append(" us.\n");
                }
            }

            if (memoryMeasurements != null && !memoryMeasurements.isEmpty()) {
                for (MemoryMeasurement memoryItem : memoryMeasurements) {
                    oss.append("\t[Memory] Working set change of ")
                            .append(memoryItem.getName())
                            .append(" is ")
                            .append(memoryItem.getWorkingSetSize() / 1024)
                            .append("(KB).\n");
                }
            }


            if (elementMeasurements != null && !elementMeasurements.isEmpty()) {
                for (ElementMeasurement elementItem : elementMeasurements) {
                    oss.append("\t[Element] Total element size of ")
                            .append(elementItem.getName())
                            .append(" is ")
                            .append(elementItem.getTotalElementSize() / 1024)
                            .append("(KB).\n");
                }
            }

        }
        System.out.println(oss);

        StringBuilder scoreTitle = new StringBuilder();
        // 输出测试结果行标题
        scoreTitle.append(dataFile.getAbsolutePath()).append(",");
        for (Map.Entry<String, Pair> item : testResults.entrySet()) {
            List<SpeedMeasurement> speedMeasurements = m_speedMeasurementsOfTestPoints.get(item.getKey());
            List<MemoryMeasurement> memoryMeasurements = m_memoryMeasurementsOfTestPoints.get(item.getKey());
            List<ElementMeasurement> elementMeasurements = m_elementMeasurementsOfTestPoints.get(item.getKey());

            scoreTitle.append("(").append(item.getKey()).append("),");
            if (speedMeasurements != null && !speedMeasurements.isEmpty()) {
                for (SpeedMeasurement speedItem : speedMeasurements) {
                    scoreTitle.append("(").append(speedItem.getName()).append("[Speed]),");
                }
            }
            if (memoryMeasurements != null && !memoryMeasurements.isEmpty()) {
                for (MemoryMeasurement memoryItem : memoryMeasurements) {
                    scoreTitle.append("(").append(memoryItem.getName()).append("[WorkingSet KB]),");
                }
            }

            if (elementMeasurements != null && !elementMeasurements.isEmpty()) {
                for (ElementMeasurement elementItem : elementMeasurements) {
                    scoreTitle.append("(").append(elementItem.getName()).append("[ElementSize KB]),");
                }
            }
        }
        System.out.println(scoreTitle);

        StringBuilder score = new StringBuilder();
        // 设置输出格式为科学记数法
        java.text.DecimalFormat scientificFormat = new java.text.DecimalFormat("0.####E0");
        score.append(this.dataFile.getAbsolutePath()).append(",");
        for (Map.Entry<String, Pair> item : testResults.entrySet()) {
            List<SpeedMeasurement> speedMeasurements = m_speedMeasurementsOfTestPoints.get(item.getKey());
            List<MemoryMeasurement> memoryMeasurements = m_memoryMeasurementsOfTestPoints.get(item.getKey());
            List<ElementMeasurement> elementMeasurements = m_elementMeasurementsOfTestPoints.get(item.getKey());
            if (item.getValue().getFirst()) {
                score.append("1,");
            } else {
                score.append("0,");
            }
            if (speedMeasurements != null && !speedMeasurements.isEmpty()) {
                for (SpeedMeasurement speedItem : speedMeasurements) {
                    score.append(scientificFormat.format(speedItem.getSpeedPerSecond() * 1e-6)).append(",");
                }
            }
            if (memoryMeasurements != null && !memoryMeasurements.isEmpty()) {
                for (MemoryMeasurement memoryItem : memoryMeasurements) {
                    score.append(memoryItem.getWorkingSetSize() / 1024).append(",");
                }
            }

            if (elementMeasurements != null && !elementMeasurements.isEmpty()) {
                for (ElementMeasurement elementItem : elementMeasurements) {
                    score.append(elementItem.getTotalElementSize() / 1024).append(",");
                }
            }
        }
        System.out.println(score);
    }

}


