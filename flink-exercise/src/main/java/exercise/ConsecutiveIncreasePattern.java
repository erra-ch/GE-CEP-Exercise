package exercise;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class ConsecutiveIncreasePattern {

    public static Pattern<AverageConsumptionTuple, ?> getPattern() {
        return Pattern.<AverageConsumptionTuple>begin("first")
                .where(new IterativeCondition<AverageConsumptionTuple>() {
                    @Override
                    public boolean filter(AverageConsumptionTuple current, Context<AverageConsumptionTuple> context) throws Exception {
                        return true;
                    }
                })
                .next("second").where(new IterativeCondition<AverageConsumptionTuple>() {
                    @Override
                    public boolean filter(AverageConsumptionTuple current, Context<AverageConsumptionTuple> context) throws Exception {
                        AverageConsumptionTuple previous = context.getEventsForPattern("first").iterator().next();
                        return current.f3 > previous.f3;
                    }
                })
                .next("third").where(new IterativeCondition<AverageConsumptionTuple>() {
                    @Override
                    public boolean filter(AverageConsumptionTuple current, Context<AverageConsumptionTuple> context) throws Exception {
                        AverageConsumptionTuple previous = context.getEventsForPattern("second").iterator().next();
                        return current.f3 > previous.f3;
                    }
                });
    }
}
