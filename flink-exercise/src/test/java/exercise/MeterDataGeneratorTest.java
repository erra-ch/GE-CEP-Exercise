package exercise;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MeterDataGeneratorTest {

    private LocalDateTime startTimestamp;
    private static final int HOURS = 24;
    private static final int HOUSEHOLDS = 10;
    private List<MeterDataTuple> meterReadings;

    @BeforeEach
    void setUp() {
        startTimestamp = LocalDateTime.of(2024, 10, 30, 0, 0);
        meterReadings = MeterDataGenerator.generate(startTimestamp, HOURS);
    }

    @Test
    void testHours() {
        for (int hour = 0; hour < HOURS; hour++) {
            Long expectedTimestamp = startTimestamp.plusHours(hour).toInstant(ZoneOffset.UTC).toEpochMilli();
            long readingCount = meterReadings.stream()
                .filter(reading -> reading.getTimestamp().equals(expectedTimestamp))
                .count();

            assertEquals(HOUSEHOLDS, readingCount, "Each household should have readings for every hour");
        }
    }

    @Test
    void testHouseholds() {
        Set<String> householdIds = meterReadings.stream()
            .map(MeterDataTuple::getHouseholdID)
            .collect(Collectors.toSet());

        assertEquals(HOUSEHOLDS, householdIds.size(), "Each household should have a unique ID");
    }

    @Test
    void testPositiveConsumption() {
        boolean allPositive = meterReadings.stream()
            .allMatch(reading -> reading.getConsumption() > 0);

        assertTrue(allPositive, "All consumption values should be positive");
    }

    @Test
    void testSortedByTime() {
        List<MeterDataTuple> sortedReadings = new ArrayList<>(meterReadings);
        sortedReadings.sort(Comparator.comparing(MeterDataTuple::getTimestamp));

        assertEquals(sortedReadings, meterReadings, "Readings should be sorted by timestamp in ascending order");
    }
}