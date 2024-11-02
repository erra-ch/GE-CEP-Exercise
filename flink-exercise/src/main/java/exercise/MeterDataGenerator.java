package exercise;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MeterDataGenerator {

    private static final int HOUSEHOLDS = 10;

    public static List<MeterDataTuple> generate(LocalDateTime startTimestamp, int hours) {
        Random random = new Random();
        List<MeterDataTuple> readings = new ArrayList<>();

        for (int hour = 0; hour < hours; hour++) {
            long timestamp = startTimestamp.plusHours(hour).toInstant(ZoneOffset.UTC).toEpochMilli();

            for (int household = 0; household < HOUSEHOLDS; household++) {
                Double consumption = 1.0 + (10.0 - 1.0) * random.nextDouble();
                MeterDataTuple reading = new MeterDataTuple("Household-" + household, timestamp, consumption);

                readings.add(reading);
            }
        }
        return readings;
    }
}
