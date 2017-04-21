package core.accumulator;

import org.apache.commons.collections.map.HashedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by shaosong on 2017/4/21.
 */
public class AccumulatorPool<E> {
    private Map<Long, Map<String, AccumulatorWrapper<E>>> pool = null;
    private int maxPoolSize = -1;

    public AccumulatorPool(int maxPoolSize) {
        pool = new HashedMap();
        this.maxPoolSize = maxPoolSize;
    }

    public void add(AccumulatorWrapper<E> accumulatorWrapper) {
        if (contains(accumulatorWrapper.getTimestamp(), accumulatorWrapper.getName())) {
            return;
        }
        if (isFull(accumulatorWrapper.getTimestamp())) {
            deleteEarlyElements();
        }
        Map<String, AccumulatorWrapper<E>> map = null;
        Long ts = accumulatorWrapper.getTimestamp();
        if (pool.containsKey(ts)) {
            map = pool.get(ts);
        } else {
            map = new HashedMap();
        }
        map.put(accumulatorWrapper.getName(), accumulatorWrapper);
        pool.put(ts, map);
    }

    public boolean contains(Long ts, String name) {
        return pool.containsKey(ts) && pool.get(ts).containsKey(name);
    }

    public AccumulatorWrapper<E> get(Long ts, String name) {
        if (pool.containsKey(ts)) {
            Map<String, AccumulatorWrapper<E>> map = pool.get(ts);
            if (map.containsKey(name)) {
                return map.get(name);
            }
        }
        return null;
    }

    public List<AccumulatorWrapper<E>> list(Long fromTs, Long untilTs, String name) {
        List<AccumulatorWrapper<E>> list = new ArrayList<>();
        for (Map.Entry<Long, Map<String, AccumulatorWrapper<E>>> entry : pool.entrySet()) {
            if (entry.getKey() >= fromTs && entry.getKey() < untilTs) {
                if (entry.getValue().containsKey(name)) {
                    list.add(entry.getValue().get(name));
                }
            }
        }
        return list;
    }

    private boolean isFull(Long ts) {
        return pool.size() >= maxPoolSize && !pool.containsKey(ts);
    }

    private void deleteEarlyElements() {
        Object[] tsList = pool.keySet().toArray();
        long min = (Long) tsList[0];
        for (Object ts : tsList) {
            if ((Long) ts < min) {
                min = (Long) ts;
            }
        }
        pool.remove(min);
    }
}
