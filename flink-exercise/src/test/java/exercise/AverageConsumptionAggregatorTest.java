package exercise;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AverageConsumptionAggregatorTest {

    private AverageConsumptionAggregator aggregator;
    private AverageAccumulator accumulator;

    @BeforeEach
    void setUp() {
        this.aggregator = new AverageConsumptionAggregator();
        this.accumulator = new AverageAccumulator(0,0);
    }

    @Test
    void testAdd() {
        aggregator.add(new MeterDataTuple("Household_1", null, 5.0), accumulator);
        aggregator.add(new MeterDataTuple("Household_1", null, 3.0), accumulator);
        aggregator.add(new MeterDataTuple("Household_1", null, 7.0), accumulator);

        assertEquals(15.0, accumulator.sum, 0.001, "Sum should be the total consumptions");
        assertEquals(3, accumulator.count, "Count should be the number of readings");
    }

    @Test
    void testResult() {
        Double emptyResult = aggregator.getResult(accumulator);

        assertEquals(0.0, emptyResult, 0.001, "Returns 0 instead of division by zero error");

        AverageAccumulator acc = new AverageAccumulator(10, 500);
        Double result = aggregator.getResult(acc);

        assertEquals(50.0, result, 0.001, "Average should be sum divided by count");
    }

    @Test
    void testProcessWindowFunction() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        List<MeterDataTuple> testData = List.of(
                new MeterDataTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli(), 10.0),
                new MeterDataTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 1, 0).toInstant(ZoneOffset.UTC).toEpochMilli(), 20.0),
                new MeterDataTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 5, 0).toInstant(ZoneOffset.UTC).toEpochMilli(), 30.0),
                new MeterDataTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 8, 0).toInstant(ZoneOffset.UTC).toEpochMilli(), 40.0),
                new MeterDataTuple("Household_2",
                        LocalDateTime.of(2024, 10, 1, 1, 0).toInstant(ZoneOffset.UTC).toEpochMilli(), 50.0),
                new MeterDataTuple("Household_2",
                        LocalDateTime.of(2024, 10, 1, 5, 59).toInstant(ZoneOffset.UTC).toEpochMilli(), 60.0)
        );


        DataStream<MeterDataTuple> inputStream = env
                .fromData(testData)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MeterDataTuple>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        DataStream<AverageConsumptionTuple> resultStream = inputStream
                .keyBy(MeterDataTuple::getHouseholdID)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
                .aggregate(new AverageConsumptionAggregator(), new ProcessWindowFunction<Double, AverageConsumptionTuple, String, TimeWindow>() {

                    @Override
                    public void process(String houseHoldID, Context context, Iterable<Double> averages, Collector<AverageConsumptionTuple> out) {
                        Double average = averages.iterator().next();
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect(new AverageConsumptionTuple(houseHoldID, start, end, average));
                    }
                });


        List<AverageConsumptionTuple> results = new ArrayList<>();
        try {
            resultStream.executeAndCollect().forEachRemaining(results::add);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        results.sort(Comparator.comparing(AverageConsumptionTuple::getHouseHoldID));


        assertEquals(3, results.size());
        assertEquals("Household_1", results.get(0).f0);
        assertEquals("2024-10-01-00:00:00", results.get(0).getStartDate());
        assertEquals("2024-10-01-06:00:00", results.get(0).getEndDate());
        assertEquals(20.0, results.get(0).f3, 0.001);

        assertEquals("Household_1", results.get(1).f0);
        assertEquals("2024-10-01-06:00:00", results.get(1).getStartDate());
        assertEquals("2024-10-01-12:00:00", results.get(1).getEndDate());
        assertEquals(40.0, results.get(1).f3, 0.001);

        assertEquals("Household_2", results.get(2).f0);
        assertEquals("2024-10-01-00:00:00", results.get(2).getStartDate());
        assertEquals("2024-10-01-06:00:00", results.get(2).getEndDate());
        assertEquals(55.0, results.get(2).f3, 0.001);

    }
}