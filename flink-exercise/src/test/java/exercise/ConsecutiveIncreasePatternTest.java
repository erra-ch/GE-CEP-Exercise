package exercise;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsecutiveIncreasePatternTest {

    //@Test
    void testPattern() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<AverageConsumptionTuple> testData = List.of(
                new AverageConsumptionTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        LocalDateTime.of(2024, 10, 1, 6, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        10.0),
                new AverageConsumptionTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 6, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        LocalDateTime.of(2024, 10, 1, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        20.0),
                new AverageConsumptionTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        LocalDateTime.of(2024, 10, 1, 18, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        30.0),
                new AverageConsumptionTuple("Household_1",
                        LocalDateTime.of(2024, 10, 1, 18, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        LocalDateTime.of(2024, 10, 2, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        40.0),
                new AverageConsumptionTuple("Household_2",
                        LocalDateTime.of(2024, 10, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        LocalDateTime.of(2024, 10, 1, 6, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        10.0),
                new AverageConsumptionTuple("Household_2",
                        LocalDateTime.of(2024, 10, 1, 6, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        LocalDateTime.of(2024, 10, 1, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        30.0),
                new AverageConsumptionTuple("Household_2",
                        LocalDateTime.of(2024, 10, 1, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        LocalDateTime.of(2024, 10, 1, 18, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
                        20.0)
        );


        DataStream<AverageConsumptionTuple> averageConsumptionStream = env.fromData(testData);

        KeyedStream<AverageConsumptionTuple, String> keyedAverageStream = averageConsumptionStream.keyBy(AverageConsumptionTuple::getHouseHoldID);

        PatternStream<AverageConsumptionTuple> patternStream = CEP.pattern(keyedAverageStream, ConsecutiveIncreasePattern.getPattern());


        DataStream<String> matchedStream = patternStream.select(new PatternSelectFunction<AverageConsumptionTuple, String>() {
            @Override
            public String select(Map<String, List<AverageConsumptionTuple>> map) throws Exception {
                AverageConsumptionTuple first = map.get("first").get(0);
                AverageConsumptionTuple second = map.get("second").get(0);
                AverageConsumptionTuple third = map.get("third").get(0);

                return first.getHouseHoldID() + " has a sequence of increasing averages: " +
                        first.f3 + " -> " + second.f3 + " -> " + third.f3 +
                        " From " + first.getStartDate() + " to " + third.getEndDate();
            }
        });


        List<String> results = new ArrayList<>();
        try {
            matchedStream.executeAndCollect().forEachRemaining(results::add);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        assertEquals(2, results.size());
    }
}