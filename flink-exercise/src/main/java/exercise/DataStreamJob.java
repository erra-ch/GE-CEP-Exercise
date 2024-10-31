/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package exercise;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		LocalDateTime startTimestamp = LocalDateTime.of(2024, 10, 1, 0, 0);
		DataStreamSource<MeterDataTuple> meterDataStreamSource = env.fromData(MeterDataGenerator.generate(startTimestamp, 96));

		WatermarkStrategy<MeterDataTuple> watermarkStrategy = WatermarkStrategy.<MeterDataTuple>forBoundedOutOfOrderness(Duration.ofSeconds(1))
			.withTimestampAssigner((event, timestamp) -> event.getTimestamp());

		DataStream<MeterDataTuple> watermarkedMeterDataStream = meterDataStreamSource.assignTimestampsAndWatermarks(watermarkStrategy);

		KeyedStream<MeterDataTuple, String> keyedMeterDataStream = watermarkedMeterDataStream.keyBy(MeterDataTuple::getHouseholdID);

		DataStream<AverageConsumptionTuple> averageConsumptionStream = keyedMeterDataStream
				.window(TumblingEventTimeWindows.of(Duration.ofHours(6)))
				.aggregate(new AverageConsumptionAggregator(),
						new ProcessWindowFunction<Double, AverageConsumptionTuple, String, TimeWindow>() {

							@Override
							public void process(String houseHoldID, Context context, Iterable<Double> averages, Collector<AverageConsumptionTuple> out) {
								Double average = averages.iterator().next();
								long start = context.window().getStart();
								long end = context.window().getEnd();
								out.collect(new AverageConsumptionTuple(houseHoldID, start, end, average));
							}
						});
		averageConsumptionStream.print();
		env.execute("GE Flink Exercise");
	}
}
