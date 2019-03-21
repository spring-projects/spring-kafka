/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.util.Assert;

/**
 * Creates 1 or more {@link KafkaMessageListenerContainer}s based on
 * {@link #setConcurrency(int) concurrency}. If the
 * {@link ContainerProperties} is configured with {@link TopicPartition}s,
 * the {@link TopicPartition}s are distributed evenly across the
 * instances.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Murali Reddy
 * @author Jerome Mirc
 * @author Artem Bilan
 */
public class ConcurrentMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> {

	private final ConsumerFactory<K, V> consumerFactory;

	private final List<KafkaMessageListenerContainer<K, V>> containers = new ArrayList<>();

	private int concurrency = 1;

	/**
	 * Construct an instance with the supplied configuration properties.
	 * The topic partitions are distributed evenly across the delegate
	 * {@link KafkaMessageListenerContainer}s.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	public ConcurrentMessageListenerContainer(ConsumerFactory<K, V> consumerFactory,
			ContainerProperties containerProperties) {
		super(containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		this.consumerFactory = consumerFactory;
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
			ContainerProperties containerProperties = getContainerProperties();
			TopicPartitionInitialOffset[] topicPartitions = containerProperties.getTopicPartitions();
			if (topicPartitions != null
					&& this.concurrency > topicPartitions.length) {
				this.logger.warn("When specific partitions are provided, the concurrency must be less than or "
						+ "equal to the number of partitions; reduced from " + this.concurrency + " to "
						+ topicPartitions.length);
				this.concurrency = topicPartitions.length;
			}
			setRunning(true);

			for (int i = 0; i < this.concurrency; i++) {
				KafkaMessageListenerContainer<K, V> container;
				if (topicPartitions == null) {
					container = new KafkaMessageListenerContainer<>(this.consumerFactory, containerProperties);
				}
				else {
					container = new KafkaMessageListenerContainer<>(this.consumerFactory, containerProperties,
							partitionSubset(containerProperties, i));
				}
				if (getBeanName() != null) {
					container.setBeanName(getBeanName() + "-" + i);
				}
				if (getApplicationEventPublisher() != null) {
					container.setApplicationEventPublisher(getApplicationEventPublisher());
				}
				container.setClientIdSuffix("-" + i);
				container.start();
				this.containers.add(container);
			}
		}
	}

	private TopicPartitionInitialOffset[] partitionSubset(ContainerProperties containerProperties, int i) {
		TopicPartitionInitialOffset[] topicPartitions = containerProperties.getTopicPartitions();
		if (this.concurrency == 1) {
			return topicPartitions;
		}
		else {
			int numPartitions = topicPartitions.length;
			if (numPartitions == this.concurrency) {
				return new TopicPartitionInitialOffset[] { topicPartitions[i] };
			}
			else {
				int perContainer = numPartitions / this.concurrency;
				TopicPartitionInitialOffset[] subset;
				if (i == this.concurrency - 1) {
					subset = Arrays.copyOfRange(topicPartitions, i * perContainer, topicPartitions.length);
				}
				else {
					subset = Arrays.copyOfRange(topicPartitions, i * perContainer, (i + 1) * perContainer);
				}
				return subset;
			}
		}
	}

	/*
	 * Under lifecycle lock.
	 */
	@Override
	protected void doStop(final Runnable callback) {
		final AtomicInteger count = new AtomicInteger();
		if (isRunning()) {
			setRunning(false);
			for (KafkaMessageListenerContainer<K, V> container : this.containers) {
				if (container.isRunning()) {
					count.incrementAndGet();
				}
			}
			for (KafkaMessageListenerContainer<K, V> container : this.containers) {
				if (container.isRunning()) {
					container.stop(new Runnable() {

						@Override
						public void run() {
							if (count.decrementAndGet() <= 0) {
								callback.run();
							}
						}

					});
				}
			}
			this.containers.clear();
		}
	}

	@Override
	public String toString() {
		return "ConcurrentMessageListenerContainer [concurrency=" + this.concurrency + ", beanName="
				+ this.getBeanName() + ", running=" + this.isRunning() + "]";
	}

}
