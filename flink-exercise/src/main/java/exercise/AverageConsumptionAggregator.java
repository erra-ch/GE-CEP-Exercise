package exercise;

import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageConsumptionAggregator implements AggregateFunction<MeterDataTuple, AverageAccumulator, Double> {

    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator(0, 0);
    }

    @Override
    public AverageAccumulator add(MeterDataTuple value, AverageAccumulator acc) {
        acc.sum += value.getConsumption();
        acc.count += 1;
        return acc;
    }

    @Override
    public Double getResult(AverageAccumulator acc) {
        return acc.count == 0 ? 0 : acc.sum / acc.count;
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        return null;
    }
}
