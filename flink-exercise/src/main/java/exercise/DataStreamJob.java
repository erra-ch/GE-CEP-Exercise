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
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.file.sink.FileSink;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		int parallelism = params.getInt("parallelism", 0);
		if (parallelism > 0) {
			env.setParallelism(parallelism);
		}

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

		String outputPath = params.get("out","output/");
		FileSink<String> sink = FileSink
				.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
				.withRollingPolicy(OnCheckpointRollingPolicy.build())
				.build();

		matchedStream
				.map(item -> "Matched Item: " + item)
				.sinkTo(sink);
		
		env.execute("GE Flink Exercise");
	}
}
