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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ListStateOffsetStore.
 */
public class ListStateOffsetStore implements OffsetStore {

	private static final Logger LOG = ClientLogger.getLog();

	private final MQClientInstance mQClientFactory;
	private final String groupName;
	private final String topic;
	private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
			new ConcurrentHashMap<>();

	public ListStateOffsetStore(MQClientInstance mQClientFactory, String groupName, String topic) {
		this.mQClientFactory = mQClientFactory;
		this.groupName = groupName;
		this.topic = topic;
		offsetTable = new ConcurrentHashMap<>();
	}

	/** Loads offsets from the given list state. */
	public void restoreOffsets(ListState<Tuple2<MessageQueue, Long>> offsetState) throws Exception {
		Iterator<Tuple2<MessageQueue, Long>> iterator = offsetState.get().iterator();
		Tuple2<MessageQueue, Long> pair;
		while (iterator.hasNext()) {
			pair = iterator.next();
			if (!offsetTable.containsKey(pair.f0) || offsetTable.get(pair.f0).get() < pair.f1) {
				offsetTable.put(pair.f0, new AtomicLong(pair.f1));
			}
		}
	}

	/** Persists the given offsets snapshot. */
	public void persistOffsets(Map<MessageQueue, Long> offsetsSnapshot) {
		if (null == offsetsSnapshot || offsetsSnapshot.isEmpty()) {
			return;
		}

		final HashSet<MessageQueue> unusedMQ = new HashSet<>();
		for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			if (!offsetsSnapshot.containsKey(mq)) {
				unusedMQ.add(mq);
			}
			try {
				this.updateConsumeOffsetToBroker(mq, offsetsSnapshot.get(mq));
				LOG.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
						this.groupName,
						this.mQClientFactory.getClientId(),
						mq,
						offsetsSnapshot.get(mq));
			} catch (Exception e) {
				LOG.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
			}
		}

		if (!unusedMQ.isEmpty()) {
			for (MessageQueue mq : unusedMQ) {
				this.offsetTable.remove(mq);
				LOG.info("remove unused mq, {}, {}", mq, this.groupName);
			}
		}
	}

	@Override
	public void load() throws MQClientException {
		// Do nothing.
	}

	@Override
	public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
		if (mq != null) {
			AtomicLong offsetOld = this.offsetTable.get(mq);
			if (null == offsetOld) {
				offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
			}

			if (null != offsetOld) {
				if (increaseOnly) {
					MixAll.compareAndIncreaseOnly(offsetOld, offset);
				} else {
					offsetOld.set(offset);
				}
			}
		}
	}

	@Override
	public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
		if (mq != null) {
			switch (type) {
				case MEMORY_FIRST_THEN_STORE:
				case READ_FROM_MEMORY: {
					AtomicLong offset = this.offsetTable.get(mq);
					if (offset != null) {
						return offset.get();
					} else if (ReadOffsetType.READ_FROM_MEMORY == type) {
						return -1;
					}
				}
				break;
				case READ_FROM_STORE: {
					try {
						long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
						AtomicLong offset = new AtomicLong(brokerOffset);
						this.updateOffset(mq, offset.get(), false);
						return brokerOffset;
					}
					// No offset in broker
					catch (MQBrokerException e) {
						return -1;
					}
					//Other exceptions
					catch (Exception e) {
						LOG.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
						return -2;
					}
				}
				default:
					break;
			}
		}

		return -1;
	}

	@Override
	public void persistAll(Set<MessageQueue> mqs) {
		// Do nothing.
		System.out.println("persistAll");
	}

	@Override
	public void persist(MessageQueue mq) {
		// Do nothing.
		System.out.println("persist");
	}

	@Override
	public void removeOffset(MessageQueue mq) {
		if (mq != null) {
			this.offsetTable.remove(mq);
			LOG.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
					offsetTable.size());
		}
	}

	@Override
	public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
		Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
		for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
				continue;
			}
			cloneOffsetTable.put(mq, entry.getValue().get());
		}
		return cloneOffsetTable;
	}

	/**
	 * Update the Consumer Offset in one way, once the Master is off, updated to Slave,
	 * here need to be optimized.
	 */
	private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
			MQBrokerException, InterruptedException, MQClientException {
		updateConsumeOffsetToBroker(mq, offset, true);
	}

	/**
	 * Update the Consumer Offset synchronously, once the Master is off, updated to Slave,
	 * here need to be optimized.
	 */
	@Override
	public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
			MQBrokerException, InterruptedException, MQClientException {
		FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
		if (null == findBrokerResult) {
			// TODO Here may be heavily overhead for Name Server,need tuning
			this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
		}

		if (findBrokerResult != null) {
			UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setConsumerGroup(this.groupName);
			requestHeader.setQueueId(mq.getQueueId());
			requestHeader.setCommitOffset(offset);

			if (isOneway) {
				this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
						findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
			} else {
				this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
						findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
			}
		} else {
			throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
		}
	}

	private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
			InterruptedException, MQClientException {
		FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
		if (null == findBrokerResult) {
			// TODO Here may be heavily overhead for Name Server,need tuning
			this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
		}

		if (findBrokerResult != null) {
			QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
			requestHeader.setTopic(mq.getTopic());
			requestHeader.setConsumerGroup(this.groupName);
			requestHeader.setQueueId(mq.getQueueId());

			return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
					findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
		} else {
			throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
		}
	}

}
