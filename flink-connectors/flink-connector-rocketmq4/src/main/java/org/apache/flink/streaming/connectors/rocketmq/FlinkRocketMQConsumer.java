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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.SerializableObject;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Flink source to read data from RocketMQ.
 *
 */
public class FlinkRocketMQConsumer<T> extends RichParallelSourceFunction<T> implements
		CheckpointListener, CheckpointedFunction, PullTaskCallback {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkRocketMQConsumer.class);
	private static final int MAX_SNAPSHOT_NUM = 100;

	private final SerializableObject runLock = new SerializableObject();
	private volatile boolean running = false;

	private String nameSrvAddr;
	private String groupName;
	private String topic;
	private DeserializationSchema<T> schema;
	private MQPullConsumerScheduleService scheduleService;

	private ListState<Tuple2<MessageQueue, Long>> offsetState;
	private ListStateOffsetStore offsetStore;
	private LinkedMap snapshotMap = new LinkedMap();

	private SourceContext<T> sourceContext;

	@SuppressWarnings("WeakerAccess")
	public FlinkRocketMQConsumer(
			String nameSrvAddr,
			String groupName,
			String topic,
			DeserializationSchema<T> schema) {
		this.nameSrvAddr = nameSrvAddr;
		this.groupName = groupName;
		this.topic = topic;
		this.schema = schema;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		LOG.info("Open");
		scheduleService = new MQPullConsumerScheduleService(groupName);
		DefaultMQPullConsumer consumer = scheduleService.getDefaultMQPullConsumer();
		consumer.setNamesrvAddr(nameSrvAddr);
		consumer.setOffsetStore(offsetStore);
		scheduleService.registerPullTaskCallback(topic, this);

		// TODO Not quite sure if the clientInstance can be identical with the one used in DefaultMQPullConsumerImpl.
		MQClientInstance clientInstance =
				MQClientManager.getInstance().getAndCreateMQClientInstance(consumer, null);
		offsetStore = new ListStateOffsetStore(clientInstance, groupName, topic);
		consumer.setOffsetStore(offsetStore);
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		this.sourceContext = ctx;
		scheduleService.start();
		running = true;
		while (running) {
			synchronized (runLock) {
				// The main thread just wait here.
				runLock.wait();
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
		scheduleService.shutdown();
		runLock.notifyAll();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void initializeState(FunctionInitializationContext context) throws Exception {
		offsetState = context.getOperatorStateStore().getUnionListState(
				new ListStateDescriptor<Tuple2<MessageQueue, Long>>(
						"RocketMQConsumer_" + topic,
						new TupleTypeInfo(PojoTypeInfo.of(MessageQueue.class), BasicTypeInfo.LONG_TYPE_INFO))
		);
		if (context.isRestored()) {
			offsetStore.restoreOffsets(offsetState);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (!running) {
			LOG.info("snapshotState() called on a closed source");
		} else {
			offsetState.clear();
			Map<MessageQueue, Long> offsetSnapshot = offsetStore.cloneOffsetTable(topic);
			Map<MessageQueue, Long> managedOffsetSnapshot = new HashMap<>();
			Set<MessageQueue> managedQueue = getManagedQueue();
			LOG.info("Managed queues: " + managedQueue);
			for (Map.Entry<MessageQueue, Long> entry : offsetSnapshot.entrySet()) {
				if (managedQueue.contains(entry.getKey())) {
					managedOffsetSnapshot.put(entry.getKey(), entry.getValue());
				}
			}
			snapshotMap.put(context.getCheckpointId(), managedOffsetSnapshot);
			for (Map.Entry<MessageQueue, Long> entry : offsetSnapshot.entrySet()) {
				offsetState.add(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}

		while (snapshotMap.size() > MAX_SNAPSHOT_NUM) {
			snapshotMap.remove(0);
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		int index = snapshotMap.indexOf(checkpointId);
		if (-1 == index) {
			LOG.info("Offset snapshot with ID = " + checkpointId + " has been removed.");
		} else {
			@SuppressWarnings("unchecked")
			Map<MessageQueue, Long> managedOffsetSnapshot = (Map<MessageQueue, Long>) snapshotMap.remove(index);
			// remove older checkpoints in map
			for (int i = 0; i < index; i++) {
				snapshotMap.remove(0);
			}
			if (null != managedOffsetSnapshot) {
				offsetStore.persistOffsets(managedOffsetSnapshot);
			}
		}
	}

	@Override
	public void doPullTask(MessageQueue mq, PullTaskContext context) {
		MQPullConsumer consumer = context.getPullConsumer();
		try {
			long offset = consumer.fetchConsumeOffset(mq, false);
			if (offset < 0) {
				offset = 0;
			}
			PullResult pullResult = consumer.pull(mq, "*", offset, 32);
			LOG.info("%s%n", offset + "\t" + mq + "\t" + pullResult);
			switch (pullResult.getPullStatus()) {
				case FOUND:
					for (MessageExt messageExt : pullResult.getMsgFoundList()) {
						sourceContext.collect(schema.deserialize(messageExt.getBody()));
					}
					break;
				case NO_MATCHED_MSG:
					LOG.info("NO_MATCHED_MSG");
					break;
				case NO_NEW_MSG:
					LOG.info("NO_NEW_MSG");
					break;
				case OFFSET_ILLEGAL:
					LOG.info("Illegal offset: " + offset);
					break;
				default:
					break;
			}
			consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** Use reflection to acquire the managed queues from the schedule service. */
	@SuppressWarnings("unchecked")
	private Set<MessageQueue> getManagedQueue() {
		Set<MessageQueue> queueSet = new HashSet<>();
		try {
			Field field = scheduleService.getClass().getDeclaredField("taskTable");
			field.setAccessible(true);
			ConcurrentMap<MessageQueue, Object> tableTask =
					(ConcurrentMap<MessageQueue, Object>) field.get (scheduleService);
			queueSet.addAll(tableTask.keySet());
		} catch (NoSuchFieldException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return queueSet;
	}
}
