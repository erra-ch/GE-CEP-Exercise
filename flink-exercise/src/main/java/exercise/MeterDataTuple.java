package exercise;

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MeterDataTuple extends Tuple3<String, LocalDateTime, Double> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    public MeterDataTuple() {}

    public MeterDataTuple(String houseHoldID, LocalDateTime timestamp, Double consumption) {
        this.f0 = houseHoldID;
        this.f1 = timestamp;
        this.f2 = consumption;
    }

    public String getHouseholdID() {
        return f0;
    }

    public LocalDateTime getTimestamp() {
        return f1;
    }

    public Double getConsumption() {
        return f2;
    }
}
