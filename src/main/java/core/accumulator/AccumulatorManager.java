package core.accumulator;

import org.apache.spark.Accumulator;

/**
 * Created by shaosong on 2017/4/20.
 */
public class AccumulatorManager<T> {
    private AccumulatorPool<T> pool;
    private static AccumulatorManager instance;
    private final static int POOL_SIZE = 100;

    private AccumulatorManager() {
        pool = new AccumulatorPool(POOL_SIZE);
    }

    public static synchronized AccumulatorManager getInstance() {
        if (instance == null) {
            instance = new AccumulatorManager();
        }
        return instance;
    }

    public void register(Accumulator<T> accumulator, long timestamp) {
        AccumulatorWrapper accumulatorWrapper = new AccumulatorWrapper(accumulator, timestamp);
        pool.add(accumulatorWrapper);
    }

    public Accumulator<T> getAccumulator(Long ts, String name) {
        AccumulatorWrapper accumulatorWrapper = pool.get(ts, name);
        return accumulatorWrapper == null ? null : accumulatorWrapper.getAccumulator();
    }

}
