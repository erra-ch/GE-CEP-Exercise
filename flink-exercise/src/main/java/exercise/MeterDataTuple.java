package exercise;

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class MeterDataTuple extends Tuple3<String, Long, Double> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    public MeterDataTuple() {}

    public MeterDataTuple(String houseHoldID, Long timestamp, Double consumption) {
        this.f0 = houseHoldID;
        this.f1 = timestamp;
        this.f2 = consumption;
    }

    public String getHouseholdID() {
        return f0;
    }

    public Long getTimestamp() {
        return f1;
    }

    public LocalDateTime getFormattedTimestamp() {
        return LocalDateTime.ofEpochSecond(f1, 0, ZoneOffset.UTC);
    }

    public Double getConsumption() {
        return f2;
    }
}
