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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.SerializableObject;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Flink sink to produce data into RocketMQ.
 *
 */
public class FlinkRocketMQProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkRocketMQProducer.class);

	private String nameSrvAddr;
	private String groupName;
	private String topicName;
	private DefaultMQProducer producer;
	private List<MessageQueue> queueList;
	private SerializationSchema<T> schema;
	private Partitioner partitioner;
	private SendCallback callback;

	// for snapshot
	private volatile int pendingCount;
	private final SerializableObject pendingLock;
	private List<Throwable> errorList;

	@SuppressWarnings("WeakerAccess")
	public FlinkRocketMQProducer(
			String nameSrvAddr,
			String groupName,
			String topicName,
			SerializationSchema<T> schema) {
		// TODO Need more configurations here.
		requireNonNull(nameSrvAddr);
		requireNonNull(groupName);
		requireNonNull(topicName);
		requireNonNull(schema);
		this.nameSrvAddr = nameSrvAddr;
		this.groupName = groupName;
		this.topicName = topicName;
		this.schema = schema;

		pendingLock = new SerializableObject();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		producer = new DefaultMQProducer(groupName);
		producer.setNamesrvAddr(nameSrvAddr);
		producer.start();
		queueList = producer.fetchPublishMessageQueues(topicName);
		partitioner = new Partitioner();

		pendingCount = 0;
		errorList = Collections.synchronizedList(new LinkedList<>());

		callback = new SendCallback() {
			@Override
			public void onSuccess(SendResult sendResult) {
				// TODO Maybe we need to deal with different statuses here.
				synchronized (pendingLock) {
					--pendingCount;
					if (0 == pendingCount) {
						pendingLock.notifyAll();
					}
				}
			}

			@Override
			public void onException(Throwable throwable) {
				// TODO Why not passing in the message?
				errorList.add(throwable);
			}
		};

	}

	@Override
	public void close() throws Exception {
		producer.shutdown();
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		byte[] messageValue = schema.serialize(value);
		// To achieve exactly once, at least we need an unique ID for each value.
		Message message = new Message(topicName, messageValue);
		// Not sure whether the queue number will change in runtime.
		int queueNum = partitioner.partition(messageValue, queueList.size());
		synchronized (pendingLock) {
			++pendingCount;
		}
		producer.send(message, queueList.get(queueNum), callback);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// Do nothing.
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// We just wait to flush all the pending messages.
		synchronized (pendingLock) {
			while (pendingCount > 0) {
				pendingLock.wait();
			}
			assert(pendingCount == 0);
		}
		logErrors();
	}

	private void logErrors() throws Exception {
		Throwable lastThrowable = null;
		for (Throwable throwable : errorList) {
			LOG.error("Error while sending message", throwable);
			lastThrowable = throwable;
		}
		errorList.clear();
		if (null != lastThrowable) {
			throw new Exception(lastThrowable);
		}
	}

	/**
	 * Choose a queue for a given message.
	 */
	private static class Partitioner {
		int partition(byte[] message, int size) {
			// TODO We need a more powerful partitioning mechanism here.
			return Arrays.hashCode(message) % size;
		}
	}

}
