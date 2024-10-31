package exercise;

import org.apache.flink.api.java.tuple.Tuple4;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class AverageConsumptionTuple extends Tuple4<String, Long, Long, Double> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    public AverageConsumptionTuple() {
    }

    public AverageConsumptionTuple(String houseHoldID, Long startTimestamp, Long endTimeStamp, Double consumption) {
        this.f0 = houseHoldID;
        this.f1 = startTimestamp;
        this.f2 = endTimeStamp;
        this.f3 = consumption;
    }

    @Override
    public String toString() {
        return "AverageConsumption{" +
                "Household ID=" + f0 +
                ", Start time=" + getDate(f1) +
                ", End time=" + getDate(f2) +
                ", Consumption=" + f3 +
                '}';
    }

    public String getDate(Long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC).format(DATE_FORMAT);
    }

    public String getStartDate() {
        return getDate(this.f1);
    }

    public String getEndDate() {
        return getDate(this.f2);
    }

    public String getHouseHoldID() {
        return this.f0;
    }
}
