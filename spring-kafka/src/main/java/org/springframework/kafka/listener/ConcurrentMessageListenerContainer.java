/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.util.Assert;

/**
 * Creates 1 or more {@link KafkaMessageListenerContainer}s based on
 * {@link #setConcurrency(int) concurrency}. If the
 * {@link #ConcurrentMessageListenerContainer(ConsumerFactory, TopicPartition...)}
 * constructor is used, the {@link TopicPartition}s are distributed evenly across the
 * instances.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class ConcurrentMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> {

	private final ConsumerFactory<K, V> consumerFactory;

	private final String[] topics;

	private final Pattern topicPattern;

	private final List<KafkaMessageListenerContainer<K, V>> containers = new ArrayList<>();

	private long recentOffset;

	private TopicPartition[] partitions;

	private int concurrency = 1;

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions - when using this constructor, {@link #setRecentOffset(long)
	 * recentOffset} can be specified.
	 * The topic partitions are distributed evenly across the delegate
	 * {@link KafkaMessageListenerContainer}s.
	 * @param consumerFactory the consumer factory.
	 * @param topicPartitions the topics/partitions; duplicates are eliminated.
	 */
	public ConcurrentMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			TopicPartition... topicPartitions) {
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		Assert.notEmpty(topicPartitions, "A list of partitions must be provided");
		Assert.noNullElements(topicPartitions, "The list of partitions cannot contain null elements");
		this.consumerFactory = consumerFactory;
		this.partitions = new LinkedHashSet<>(Arrays.asList(topicPartitions))
				.toArray(new TopicPartition[topicPartitions.length]);
		this.topics = null;
		this.topicPattern = null;
	}

	/**
	 * Construct an instance with the supplied configuration properties and topics.
	 * When using this constructor, {@link #setRecentOffset(long) recentOffset} is
	 * ignored.
	 * @param consumerFactory the consumer factory.
	 * @param topics the topics.
	 */
	public ConcurrentMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, String... topics) {
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		Assert.notNull(topics, "A list of topics must be provided");
		Assert.noNullElements(topics, "The list of topics cannot contain null elements");
		this.consumerFactory = consumerFactory;
		this.topics = Arrays.asList(topics).toArray(new String[topics.length]);
		this.topicPattern = null;
	}

	/**
	 * Construct an instance with the supplied configuration properties and topic
	 * pattern. When using this constructor, {@link #setRecentOffset(long) recentOffset} is
	 * ignored.
	 * @param consumerFactory the consumer factory.
	 * @param topicPattern the topic pattern.
	 */
	public ConcurrentMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, Pattern topicPattern) {
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		Assert.notNull(topicPattern, "A topic pattern must be provided");
		this.consumerFactory = consumerFactory;
		this.topics = null;
		this.topicPattern = topicPattern;
	}

	/**
	 * Set the offset to this number of records back from the latest when starting.
	 * Overrides any consumer properties (earliest, latest).
	 * Only applies when explicit topic/partition assignment is provided.
	 * @param recentOffset the offset from the latest; default 0.
	 */
	public void setRecentOffset(long recentOffset) {
		this.recentOffset = recentOffset;
	}

	public int getConcurrency() {
		return this.concurrency;
	}

	/**
	 * The maximum number of concurrent {@link KafkaMessageListenerContainer}s running.
	 * Messages from within the same partition will be processed sequentially.
	 * @param concurrency the concurrency.
	 */
	public void setConcurrency(int concurrency) {
		Assert.isTrue(concurrency > 0, "concurrency must be greater than 0");
		this.concurrency = concurrency;
	}

	/**
	 * Return the list of {@link KafkaMessageListenerContainer}s created by
	 * this container.
	 * @return the list of {@link KafkaMessageListenerContainer}s created by
	 * this container.
	 */
	public List<KafkaMessageListenerContainer<K, V>> getContainers() {
		return Collections.unmodifiableList(this.containers);
	}

	/*
	 * Under lifecycle lock.
	 */
	@Override
	protected void doStart() {
		if (!isRunning()) {
			if (this.partitions != null && this.concurrency > this.partitions.length) {
				logger.warn("When specific partitions are provided, the concurrency must be less than or "
						+ "equal to the number of partitions; reduced from " + this.concurrency
						+ " to " + this.partitions.length);
				this.concurrency = this.partitions.length;
			}
			setRunning(true);
			for (int i = 0; i < this.concurrency; i++) {
				KafkaMessageListenerContainer<K, V> container;
				if (this.partitions == null) {
					container = new KafkaMessageListenerContainer<>(this.consumerFactory, this.topics,
							this.topicPattern, this.partitions);
				}
				else {
					container = new KafkaMessageListenerContainer<>(this.consumerFactory, this.topics,
							this.topicPattern, partitionSubset(i));
				}
				container.setAckMode(getAckMode());
				container.setAckCount(getAckCount());
				container.setAckTime(getAckTime());
				container.setRecentOffset(this.recentOffset);
				container.setAutoStartup(false);
				container.setMessageListener(getMessageListener());
				if (getTaskExecutor() != null) {
					container.setTaskExecutor(getTaskExecutor());
				}
				if (getBeanName() != null) {
					container.setBeanName(getBeanName() + "-" + i);
				}
				container.start();
				this.containers.add(container);
			}
		}
	}

	private TopicPartition[] partitionSubset(int i) {
		if (this.concurrency == 1) {
			return this.partitions;
		}
		else {
			int numPartitions = this.partitions.length;
			if (numPartitions == this.concurrency) {
				return new TopicPartition[] { this.partitions[i] };
			}
			else {
				int perContainer = numPartitions / this.concurrency;
				TopicPartition[] subset;
				if (i == this.concurrency - 1) {
					subset = Arrays.copyOfRange(this.partitions, i * perContainer, this.partitions.length);
				}
				else {
					subset = Arrays.copyOfRange(this.partitions, i * perContainer, (i + 1) * perContainer);
				}
				return subset;
			}
		}
	}

	/*
	 * Under lifecycle lock.
	 */
	@Override
	protected void doStop() {
		if (isRunning()) {
			setRunning(false);
			for (KafkaMessageListenerContainer<K, V> container : this.containers) {
				container.stop();
			}
			this.containers.clear();
		}
	}

}
