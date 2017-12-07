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

package org.apache.flink.streaming.connectors.rocketmq;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * FlinkRocketMQProducerTest.
 *
 */
public class FlinkRocketMQConsumerTest {
	@Test
	@Ignore
	public void localConsumerTest() throws Exception {
		FlinkRocketMQConsumer<String> consumer = new FlinkRocketMQConsumer<>(
				"localhost:9876",
				"consumerGroup",
				"TopicTest",
				// TODO Need other schemas.
				new StringDesrializationSchema()
		);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		DataStream<String> input = env
				.addSource(consumer).setParallelism(1).returns(BasicTypeInfo.STRING_TYPE_INFO)
				.map((MapFunction<String, String>) value -> value.trim());
		input.print();
		env.execute("RocketMQ-4.1 Example");
	}

	private static class StringDesrializationSchema implements DeserializationSchema<String> {

		@Override
		public String deserialize(byte[] message) throws IOException {
			return new String(message);
		}

		@Override
		public boolean isEndOfStream(String nextElement) {
			return false;
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return BasicTypeInfo.STRING_TYPE_INFO;
		}
	}
}
