package exercise;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MeterDataGenerator {

    private static final int HOUSEHOLDS = 10;

    public static List<MeterDataTuple> generate(LocalDateTime startTimestamp, int hours) {
        Random random = new Random();
        List<MeterDataTuple> readings = new ArrayList<>();

        for (int hour = 0; hour < hours; hour++) {
            LocalDateTime timestamp = startTimestamp.plusHours(hour);

            for (int household = 0; household < HOUSEHOLDS; household++) {
                Double consumption = random.nextDouble(1.0, 10.0);
                MeterDataTuple reading = new MeterDataTuple("Household-" + household, timestamp, consumption);

                readings.add(reading);
            }
        }
        return readings;
    }
}
