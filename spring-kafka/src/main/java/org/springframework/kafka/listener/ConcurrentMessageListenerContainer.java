/*
 * Copyright 2015-2022 the original author or authors.
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Creates 1 or more {@link KafkaMessageListenerContainer}s based on
 * {@link #setConcurrency(int) concurrency}. If the
 * {@link ContainerProperties} is configured with {@link org.apache.kafka.common.TopicPartition}s,
 * the {@link org.apache.kafka.common.TopicPartition}s are distributed evenly across the
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
 * @author Vladimir Tsanev
 * @author Tomaz Fernandes
 */
public class ConcurrentMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> {

	private final List<KafkaMessageListenerContainer<K, V>> containers = new ArrayList<>();

	private final List<AsyncListenableTaskExecutor> executors = new ArrayList<>();

	private int concurrency = 1;

	private boolean alwaysClientIdSuffix = true;

	/**
	 * Construct an instance with the supplied configuration properties.
	 * The topic partitions are distributed evenly across the delegate
	 * {@link KafkaMessageListenerContainer}s.
	 * @param consumerFactory the consumer factory.
	 * @param containerProperties the container properties.
	 */
	public ConcurrentMessageListenerContainer(ConsumerFactory<? super K, ? super V> consumerFactory,
			ContainerProperties containerProperties) {

		super(consumerFactory, containerProperties);
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
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
	 * Set to false to suppress adding a suffix to the child container's client.id when
	 * the concurrency is only 1.
	 * @param alwaysClientIdSuffix false to suppress, true (default) to include.
	 * @since 2.2.14
	 */
	public void setAlwaysClientIdSuffix(boolean alwaysClientIdSuffix) {
		this.alwaysClientIdSuffix = alwaysClientIdSuffix;
	}

	/**
	 * Return the list of {@link KafkaMessageListenerContainer}s created by
	 * this container.
	 * @return the list of {@link KafkaMessageListenerContainer}s created by
	 * this container.
	 */
	public List<KafkaMessageListenerContainer<K, V>> getContainers() {
		synchronized (this.lifecycleMonitor) {
			return Collections.unmodifiableList(new ArrayList<>(this.containers));
		}
	}

	@Override
	public Collection<TopicPartition> getAssignedPartitions() {
		synchronized (this.lifecycleMonitor) {
			return this.containers.stream()
					.map(KafkaMessageListenerContainer::getAssignedPartitions)
					.filter(Objects::nonNull)
					.flatMap(Collection::stream)
					.collect(Collectors.toList());
		}
	}

	@Override
	public Map<String, Collection<TopicPartition>> getAssignmentsByClientId() {
		synchronized (this.lifecycleMonitor) {
			Map<String, Collection<TopicPartition>> assignments = new HashMap<>();
			this.containers.forEach(container -> {
				Map<String, Collection<TopicPartition>> byClientId = container.getAssignmentsByClientId();
				if (byClientId != null) {
					assignments.putAll(byClientId);
				}
			});
			return assignments;
		}
	}

	@Override
	public boolean isContainerPaused() {
		synchronized (this.lifecycleMonitor) {
			boolean paused = isPaused();
			if (paused) {
				for (AbstractMessageListenerContainer<K, V> container : this.containers) {
					if (!container.isContainerPaused()) {
						return false;
					}
				}
			}
			return paused;
		}
	}

	@Override
	public boolean isChildRunning() {
		if (!isRunning()) {
			return false;
		}
		for (MessageListenerContainer container : this.containers) {
			if (container.isRunning()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Map<String, Map<MetricName, ? extends Metric>> metrics() {
		synchronized (this.lifecycleMonitor) {
			Map<String, Map<MetricName, ? extends Metric>> metrics = new HashMap<>();
			for (KafkaMessageListenerContainer<K, V> container : this.containers) {
				metrics.putAll(container.metrics());
			}
			return Collections.unmodifiableMap(metrics);
		}
	}

	/*
	 * Under lifecycle lock.
	 */
	@Override
	protected void doStart() {
		if (!isRunning()) {
			checkTopics();
			ContainerProperties containerProperties = getContainerProperties();
			TopicPartitionOffset[] topicPartitions = containerProperties.getTopicPartitions();
			if (topicPartitions != null && this.concurrency > topicPartitions.length) {
				this.logger.warn(() -> "When specific partitions are provided, the concurrency must be less than or "
						+ "equal to the number of partitions; reduced from " + this.concurrency + " to "
						+ topicPartitions.length);
				this.concurrency = topicPartitions.length;
			}
			setRunning(true);

			for (int i = 0; i < this.concurrency; i++) {
				KafkaMessageListenerContainer<K, V> container =
						constructContainer(containerProperties, topicPartitions, i);
				configureChildContainer(i, container);
				if (isPaused()) {
					container.pause();
				}
				container.start();
				this.containers.add(container);
			}
		}
	}

	@SuppressWarnings("deprecation")
	private void configureChildContainer(int index, KafkaMessageListenerContainer<K, V> container) {
		String beanName = getBeanName();
		beanName = (beanName == null ? "consumer" : beanName) + "-" + index;
		container.setBeanName(beanName);
		ApplicationContext applicationContext = getApplicationContext();
		if (applicationContext != null) {
			container.setApplicationContext(applicationContext);
		}
		ApplicationEventPublisher publisher = getApplicationEventPublisher();
		if (publisher != null) {
			container.setApplicationEventPublisher(publisher);
		}
		container.setClientIdSuffix(this.concurrency > 1 || this.alwaysClientIdSuffix ? "-" + index : "");
		container.setGenericErrorHandler(getGenericErrorHandler());
		container.setCommonErrorHandler(getCommonErrorHandler());
		container.setAfterRollbackProcessor(getAfterRollbackProcessor());
		container.setRecordInterceptor(getRecordInterceptor());
		container.setBatchInterceptor(getBatchInterceptor());
		container.setInterceptBeforeTx(isInterceptBeforeTx());
		container.setListenerInfo(getListenerInfo());
		container.setEmergencyStop(() -> {
			stopAbnormally(() -> {
			});
		});
		AsyncListenableTaskExecutor exec = container.getContainerProperties().getConsumerTaskExecutor();
		if (exec == null) {
			if ((this.executors.size() > index)) {
				exec = this.executors.get(index);
			}
			else {
				exec = new SimpleAsyncTaskExecutor(beanName + "-C-");
				this.executors.add(exec);
			}
			container.getContainerProperties().setConsumerTaskExecutor(exec);
		}
	}

	private KafkaMessageListenerContainer<K, V> constructContainer(ContainerProperties containerProperties,
			@Nullable TopicPartitionOffset[] topicPartitions, int i) {

		KafkaMessageListenerContainer<K, V> container;
		if (topicPartitions == null) {
			container = new KafkaMessageListenerContainer<>(this, this.consumerFactory, containerProperties); // NOSONAR
		}
		else {
			container = new KafkaMessageListenerContainer<>(this, this.consumerFactory, // NOSONAR
					containerProperties, partitionSubset(containerProperties, i));
		}
		return container;
	}

	private TopicPartitionOffset[] partitionSubset(ContainerProperties containerProperties, int index) {
		TopicPartitionOffset[] topicPartitions = containerProperties.getTopicPartitions();
		if (this.concurrency == 1) {
			return topicPartitions; // NOSONAR
		}
		else {
			int numPartitions = topicPartitions.length; // NOSONAR
			if (numPartitions == this.concurrency) {
				return new TopicPartitionOffset[] { topicPartitions[index] };
			}
			else {
				int perContainer = numPartitions / this.concurrency;
				TopicPartitionOffset[] subset;
				if (index == this.concurrency - 1) {
					subset = Arrays.copyOfRange(topicPartitions, index * perContainer, topicPartitions.length);
				}
				else {
					subset = Arrays.copyOfRange(topicPartitions, index * perContainer, (index + 1) * perContainer);
				}
				return subset;
			}
		}
	}

	/*
	 * Under lifecycle lock.
	 */
	@Override
	protected void doStop(final Runnable callback, boolean normal) {
		final AtomicInteger count = new AtomicInteger();
		if (isRunning()) {
			boolean childRunning = isChildRunning();
			setRunning(false);
			if (!childRunning) {
				callback.run();
			}
			for (KafkaMessageListenerContainer<K, V> container : this.containers) {
				if (container.isRunning()) {
					count.incrementAndGet();
				}
			}
			for (KafkaMessageListenerContainer<K, V> container : this.containers) {
				if (container.isRunning()) {
					if (normal) {
						container.stop(() -> {
							if (count.decrementAndGet() <= 0) {
								callback.run();
							}
						});
					}
					else {
						container.stopAbnormally(() -> {
							if (count.decrementAndGet() <= 0) {
								callback.run();
							}
						});
					}
				}
			}
			this.containers.clear();
			setStoppedNormally(normal);
		}
	}

	@Override
	public void pause() {
		synchronized (this.lifecycleMonitor) {
			super.pause();
			this.containers.forEach(AbstractMessageListenerContainer::pause);
		}
	}

	@Override
	public void resume() {
		synchronized (this.lifecycleMonitor) {
			super.resume();
			this.containers.forEach(AbstractMessageListenerContainer::resume);
		}
	}

	@Override
	public void pausePartition(TopicPartition topicPartition) {
		synchronized (this.lifecycleMonitor) {
			super.pausePartition(topicPartition);
			this.containers
					.stream()
					.filter(container -> containsPartition(topicPartition, container))
					.forEach(container -> container.pausePartition(topicPartition));
		}
	}

	@Override
	public void resumePartition(TopicPartition topicPartition) {
		synchronized (this.lifecycleMonitor) {
			super.resumePartition(topicPartition);
			this.containers
					.stream()
					.filter(container -> containsPartition(topicPartition, container))
					.forEach(container -> container.resumePartition(topicPartition));
		}
	}

	@Override
	public boolean isPartitionPaused(TopicPartition topicPartition) {
		return this
				.containers
				.stream()
				.anyMatch(container -> container.isPartitionPaused(topicPartition));
	}

	@Override
	public boolean isInExpectedState() {
		return (isRunning() || isStoppedNormally()) && this.containers
				.stream()
				.map(container -> container.isInExpectedState())
				.allMatch(bool -> Boolean.TRUE.equals(bool));
	}

	private boolean containsPartition(TopicPartition topicPartition, KafkaMessageListenerContainer<K, V> container) {
		Collection<TopicPartition> assignedPartitions = container.getAssignedPartitions();
		return assignedPartitions != null && assignedPartitions.contains(topicPartition);
	}

	@Override
	public String toString() {
		return "ConcurrentMessageListenerContainer [concurrency=" + this.concurrency + ", beanName="
				+ this.getBeanName() + ", running=" + this.isRunning() + "]";
	}

}
