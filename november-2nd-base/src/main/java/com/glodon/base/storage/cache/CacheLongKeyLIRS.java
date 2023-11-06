package com.glodon.base.storage.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.glodon.base.util.DataUtils;

/**
 * 借鉴h2 cache
 * <p>
 * Created by liujing on 2023/10/12.
 */
public class CacheLongKeyLIRS<V> {

    private long maxMemory;
    private final Segment<V>[] segments;
    private final int segmentCount;
    private final int segmentShift;
    private final int segmentMask;
    private final int stackMoveDistance;
    private final int nonResidentQueueSize;

    @SuppressWarnings("unchecked")
    public CacheLongKeyLIRS(Config config) {
        setMaxMemory(config.maxMemory);
        this.nonResidentQueueSize = config.nonResidentQueueSize;
        DataUtils.checkArgument(Integer.bitCount(config.segmentCount) == 1,
                "The segment count must be a power of 2, is {0}", config.segmentCount);
        this.segmentCount = config.segmentCount;
        this.segmentMask = segmentCount - 1;
        this.stackMoveDistance = config.stackMoveDistance;
        segments = new Segment[segmentCount];
        clear();
        this.segmentShift = 32 - Integer.bitCount(segmentMask);
    }

    public void clear() {
        long max = Math.max(1, maxMemory / segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment<V>(max, stackMoveDistance, 8, nonResidentQueueSize);
        }
    }

    private Entry<V> find(long key) {
        int hash = getHash(key);
        return getSegment(hash).find(key, hash);
    }

    public boolean containsKey(long key) {
        int hash = getHash(key);
        return getSegment(hash).containsKey(key, hash);
    }

    public V peek(long key) {
        Entry<V> e = find(key);
        return e == null ? null : e.value;
    }

    public V put(long key, V value) {
        return put(key, value, sizeOf(value));
    }

    public V put(long key, V value, int memory) {
        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.put(key, hash, value, memory);
        }
    }

    private Segment<V> resizeIfNeeded(Segment<V> s, int segmentIndex) {
        int newLen = s.getNewMapLen();
        if (newLen == 0) {
            return s;
        }
        Segment<V> s2 = segments[segmentIndex];
        if (s == s2) {
            s = new Segment<V>(s, newLen);
            segments[segmentIndex] = s;
        }
        return s;
    }

    protected int sizeOf(V value) {
        return 1;
    }

    public V remove(long key) {
        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segments[segmentIndex];
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.remove(key, hash);
        }
    }

    public int getMemory(long key) {
        int hash = getHash(key);
        return getSegment(hash).getMemory(key, hash);
    }

    public V get(long key) {
        int hash = getHash(key);
        return getSegment(hash).get(key, hash);
    }

    private Segment<V> getSegment(int hash) {
        return segments[getSegmentIndex(hash)];
    }

    private int getSegmentIndex(int hash) {
        return (hash >>> segmentShift) & segmentMask;
    }

    static int getHash(long key) {
        int hash = (int) ((key >>> 32) ^ key);
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    public long getUsedMemory() {
        long x = 0;
        for (Segment<V> s : segments) {
            x += s.usedMemory;
        }
        return x;
    }

    public void setMaxMemory(long maxMemory) {
        DataUtils.checkArgument(maxMemory > 0, "Max memory must be larger than 0, is {0}", maxMemory);
        this.maxMemory = maxMemory;
        if (segments != null) {
            long max = 1 + maxMemory / segments.length;
            for (Segment<V> s : segments) {
                s.setMaxMemory(max);
            }
        }
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public synchronized Set<Map.Entry<Long, V>> entrySet() {
        HashMap<Long, V> map = new HashMap<Long, V>();
        for (long k : keySet()) {
            map.put(k, find(k).value);
        }
        return map.entrySet();
    }

    public Set<Long> keySet() {
        HashSet<Long> set = new HashSet<Long>();
        for (Segment<V> s : segments) {
            set.addAll(s.keySet());
        }
        return set;
    }

    public int sizeNonResident() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.queue2Size;
        }
        return x;
    }

    public int sizeMapArray() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.entries.length;
        }
        return x;
    }

    public int sizeHot() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.mapSize - s.queueSize - s.queue2Size;
        }
        return x;
    }

    public long getHits() {
        long x = 0;
        for (Segment<V> s : segments) {
            x += s.hits;
        }
        return x;
    }

    public long getMisses() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.misses;
        }
        return x;
    }

    public int size() {
        int x = 0;
        for (Segment<V> s : segments) {
            x += s.mapSize - s.queue2Size;
        }
        return x;
    }

    public List<Long> keys(boolean cold, boolean nonResident) {
        ArrayList<Long> keys = new ArrayList<Long>();
        for (Segment<V> s : segments) {
            keys.addAll(s.keys(cold, nonResident));
        }
        return keys;
    }

    public List<V> values() {
        ArrayList<V> list = new ArrayList<V>();
        for (long k : keySet()) {
            V value = find(k).value;
            if (value != null) {
                list.add(value);
            }
        }
        return list;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean containsValue(Object value) {
        return getMap().containsValue(value);
    }

    public Map<Long, V> getMap() {
        HashMap<Long, V> map = new HashMap<Long, V>();
        for (long k : keySet()) {
            V x = find(k).value;
            if (x != null) {
                map.put(k, x);
            }
        }
        return map;
    }

    public void putAll(Map<Long, ? extends V> m) {
        for (Map.Entry<Long, ? extends V> e : m.entrySet()) {
            // copy only non-null entries
            put(e.getKey(), e.getValue());
        }
    }

    private static class Segment<V> {

        int mapSize;

        int queueSize;

        int queue2Size;

        long hits;

        long misses;

        final Entry<V>[] entries;

        long usedMemory;

        private final int stackMoveDistance;

        private long maxMemory;

        private final int mask;

        private final int nonResidentQueueSize;

        private final Entry<V> stack;

        private int stackSize;

        private final Entry<V> queue;

        private final Entry<V> queue2;

        private int stackMoveCounter;

        Segment(long maxMemory, int stackMoveDistance, int len, int nonResidentQueueSize) {
            setMaxMemory(maxMemory);
            this.stackMoveDistance = stackMoveDistance;
            this.nonResidentQueueSize = nonResidentQueueSize;

            mask = len - 1;

            stack = new Entry<V>();
            stack.stackPrev = stack.stackNext = stack;
            queue = new Entry<V>();
            queue.queuePrev = queue.queueNext = queue;
            queue2 = new Entry<V>();
            queue2.queuePrev = queue2.queueNext = queue2;

            Entry<V>[] e = new Entry[len];
            entries = e;
        }

        Segment(Segment<V> old, int len) {
            this(old.maxMemory, old.stackMoveDistance, len, old.nonResidentQueueSize);
            hits = old.hits;
            misses = old.misses;
            Entry<V> s = old.stack.stackPrev;
            while (s != old.stack) {
                Entry<V> e = copy(s);
                addToMap(e);
                addToStack(e);
                s = s.stackPrev;
            }
            s = old.queue.queuePrev;
            while (s != old.queue) {
                Entry<V> e = find(s.key, getHash(s.key));
                if (e == null) {
                    e = copy(s);
                    addToMap(e);
                }
                addToQueue(queue, e);
                s = s.queuePrev;
            }
            s = old.queue2.queuePrev;
            while (s != old.queue2) {
                Entry<V> e = find(s.key, getHash(s.key));
                if (e == null) {
                    e = copy(s);
                    addToMap(e);
                }
                addToQueue(queue2, e);
                s = s.queuePrev;
            }
        }

        int getNewMapLen() {
            int len = mask + 1;
            if (len * 3 < mapSize * 4 && len < (1 << 28)) {
                return len * 2;
            } else if (len > 32 && len / 8 > mapSize) {
                return len / 2;
            }
            return 0;
        }

        private void addToMap(Entry<V> e) {
            int index = getHash(e.key) & mask;
            e.mapNext = entries[index];
            entries[index] = e;
            usedMemory += e.memory;
            mapSize++;
        }

        private static <V> Entry<V> copy(Entry<V> old) {
            Entry<V> e = new Entry<V>();
            e.key = old.key;
            e.value = old.value;
            e.memory = old.memory;
            e.topMove = old.topMove;
            return e;
        }

        int getMemory(long key, int hash) {
            Entry<V> e = find(key, hash);
            return e == null ? 0 : e.memory;
        }

        V get(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null) {
                misses++;
                return null;
            }
            V value = e.value;
            if (value == null) {
                misses++;
                return null;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 || stackMoveCounter - e.topMove > stackMoveDistance) {
                        access(key, hash);
                    }
                }
            } else {
                access(key, hash);
            }
            hits++;
            return value;
        }

        private synchronized void access(long key, int hash) {
            Entry<V> e = find(key, hash);
            if (e == null || e.value == null) {
                return;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 || stackMoveCounter - e.topMove > stackMoveDistance) {
                        boolean wasEnd = e == stack.stackPrev;
                        removeFromStack(e);
                        if (wasEnd) {
                            pruneStack();
                        }
                        addToStack(e);
                    }
                }
            } else {
                removeFromQueue(e);
                if (e.stackNext != null) {
                    removeFromStack(e);
                    convertOldestHotToCold();
                } else {
                    addToQueue(queue, e);
                }
                addToStack(e);
            }
        }

        synchronized V put(long key, int hash, V value, int memory) {
            if (value == null) {
                throw DataUtils.newIllegalArgumentException("The value may not be null");
            }
            V old;
            Entry<V> e = find(key, hash);
            if (e == null) {
                old = null;
            } else {
                old = e.value;
                remove(key, hash);
            }
            if (memory > maxMemory) {
                return old;
            }
            e = new Entry<V>();
            e.key = key;
            e.value = value;
            e.memory = memory;
            int index = hash & mask;
            e.mapNext = entries[index];
            entries[index] = e;
            usedMemory += memory;
            if (usedMemory > maxMemory) {
                evict();
                if (stackSize > 0) {
                    addToQueue(queue, e);
                }
            }
            mapSize++;
            addToStack(e);
            return old;
        }

        synchronized V remove(long key, int hash) {
            int index = hash & mask;
            Entry<V> e = entries[index];
            if (e == null) {
                return null;
            }
            V old;
            if (e.key == key) {
                old = e.value;
                entries[index] = e.mapNext;
            } else {
                Entry<V> last;
                do {
                    last = e;
                    e = e.mapNext;
                    if (e == null) {
                        return null;
                    }
                } while (e.key != key);
                old = e.value;
                last.mapNext = e.mapNext;
            }
            mapSize--;
            usedMemory -= e.memory;
            if (e.stackNext != null) {
                removeFromStack(e);
            }
            if (e.isHot()) {
                e = queue.queueNext;
                if (e != queue) {
                    removeFromQueue(e);
                    if (e.stackNext == null) {
                        addToStackBottom(e);
                    }
                }
            } else {
                removeFromQueue(e);
            }
            pruneStack();
            return old;
        }

        private void evict() {
            do {
                evictBlock();
            } while (usedMemory > maxMemory);
        }

        private void evictBlock() {
            while (queueSize <= (mapSize >>> 5) && stackSize > 0) {
                convertOldestHotToCold();
            }
            while (usedMemory > maxMemory && queueSize > 0) {
                Entry<V> e = queue.queuePrev;
                usedMemory -= e.memory;
                removeFromQueue(e);
                e.value = null;
                e.memory = 0;
                addToQueue(queue2, e);
                int maxQueue2Size = nonResidentQueueSize * (mapSize - queue2Size);
                if (maxQueue2Size >= 0) {
                    while (queue2Size > maxQueue2Size) {
                        e = queue2.queuePrev;
                        int hash = getHash(e.key);
                        remove(e.key, hash);
                    }
                }
            }
        }

        private void convertOldestHotToCold() {
            Entry<V> last = stack.stackPrev;
            if (last == stack) {
                throw new IllegalStateException();
            }
            removeFromStack(last);
            addToQueue(queue, last);
            pruneStack();
        }

        private void pruneStack() {
            while (true) {
                Entry<V> last = stack.stackPrev;
                if (last.isHot()) {
                    break;
                }
                removeFromStack(last);
            }
        }

        Entry<V> find(long key, int hash) {
            int index = hash & mask;
            Entry<V> e = entries[index];
            while (e != null && e.key != key) {
                e = e.mapNext;
            }
            return e;
        }

        private void addToStack(Entry<V> e) {
            e.stackPrev = stack;
            e.stackNext = stack.stackNext;
            e.stackNext.stackPrev = e;
            stack.stackNext = e;
            stackSize++;
            e.topMove = stackMoveCounter++;
        }

        private void addToStackBottom(Entry<V> e) {
            e.stackNext = stack;
            e.stackPrev = stack.stackPrev;
            e.stackPrev.stackNext = e;
            stack.stackPrev = e;
            stackSize++;
        }

        private void removeFromStack(Entry<V> e) {
            e.stackPrev.stackNext = e.stackNext;
            e.stackNext.stackPrev = e.stackPrev;
            e.stackPrev = e.stackNext = null;
            stackSize--;
        }

        private void addToQueue(Entry<V> q, Entry<V> e) {
            e.queuePrev = q;
            e.queueNext = q.queueNext;
            e.queueNext.queuePrev = e;
            q.queueNext = e;
            if (e.value != null) {
                queueSize++;
            } else {
                queue2Size++;
            }
        }

        private void removeFromQueue(Entry<V> e) {
            e.queuePrev.queueNext = e.queueNext;
            e.queueNext.queuePrev = e.queuePrev;
            e.queuePrev = e.queueNext = null;
            if (e.value != null) {
                queueSize--;
            } else {
                queue2Size--;
            }
        }

        synchronized List<Long> keys(boolean cold, boolean nonResident) {
            ArrayList<Long> keys = new ArrayList<Long>();
            if (cold) {
                Entry<V> start = nonResident ? queue2 : queue;
                for (Entry<V> e = start.queueNext; e != start; e = e.queueNext) {
                    keys.add(e.key);
                }
            } else {
                for (Entry<V> e = stack.stackNext; e != stack; e = e.stackNext) {
                    keys.add(e.key);
                }
            }
            return keys;
        }

        boolean containsKey(long key, int hash) {
            Entry<V> e = find(key, hash);
            return e != null && e.value != null;
        }

        synchronized Set<Long> keySet() {
            HashSet<Long> set = new HashSet<Long>();
            for (Entry<V> e = stack.stackNext; e != stack; e = e.stackNext) {
                set.add(e.key);
            }
            for (Entry<V> e = queue.queueNext; e != queue; e = e.queueNext) {
                set.add(e.key);
            }
            return set;
        }

        void setMaxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
        }

    }

    static class Entry<V> {

        long key;

        V value;

        int memory;

        int topMove;

        Entry<V> stackNext;

        Entry<V> stackPrev;

        Entry<V> queueNext;

        Entry<V> queuePrev;

        Entry<V> mapNext;

        boolean isHot() {
            return queueNext == null;
        }
    }

    public static class Config {

        public long maxMemory = 1;

        public int segmentCount = 16;

        public int stackMoveDistance = 32;

        public int nonResidentQueueSize = 3;
    }
}
