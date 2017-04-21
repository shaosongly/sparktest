package core.accumulator;

import org.apache.spark.Accumulator;

/**
 * Created by shaosong on 2017/4/21.
 */
public class AccumulatorWrapper<T> {
    private Accumulator<T> accumulator = null;
    private long timestamp = -1;
    private String name = null;

    public AccumulatorWrapper(Accumulator<T> accumulator, long timestamp) {
        this(accumulator, timestamp, accumulator.name().isDefined() ? accumulator.name().get() : "default");
    }

    public AccumulatorWrapper(Accumulator<T> accumulator, long timestamp, String name) {
        this.accumulator = accumulator;
        this.timestamp = timestamp;
        this.name = name;
    }

    public Accumulator<T> getAccumulator() {
        return accumulator;
    }

    public void setAccumulator(Accumulator<T> accumulator) {
        this.accumulator = accumulator;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
