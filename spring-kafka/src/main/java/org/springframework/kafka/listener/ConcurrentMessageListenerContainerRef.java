/*
 * Copyright 2015-2024 the original author or authors.
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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.lang.Nullable;

/**
 * Reference of {@link ConcurrentMessageListenerContainer} to be passed to the {@link KafkaMessageListenerContainer}.
 * This container is used for internal purpose. Detects if the {@link KafkaMessageListenerContainer} is fenced and
 * forbids `stop` calls on {@link ConcurrentMessageListenerContainer}
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Lokesh Alamuri
 */
class ConcurrentMessageListenerContainerRef<K, V> extends AbstractMessageListenerContainer {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass())); // NOSONAR

	private final ConcurrentMessageListenerContainer concurrentMessageListenerContainer;

	private final ReentrantLock lifecycleLock;

	private KafkaMessageListenerContainer kafkaMessageListenerContainer;

	ConcurrentMessageListenerContainerRef(ConcurrentMessageListenerContainer concurrentMessageListenerContainer,
																						ReentrantLock lifecycleLock) {
		super();
		this.concurrentMessageListenerContainer = concurrentMessageListenerContainer;
		this.lifecycleLock = lifecycleLock;
	}

	void setKafkaMessageListenerContainer(KafkaMessageListenerContainer kafkaMessageListenerContainer) {
		this.kafkaMessageListenerContainer = kafkaMessageListenerContainer;
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		throw new UnsupportedOperationException("This container doesn't support setting up MessageListener");
	}

	@Override
	public Map<String, Map<MetricName, ? extends Metric>> metrics() {
		return this.concurrentMessageListenerContainer.metrics();
	}

	@Override
	public ContainerProperties getContainerProperties() {
		return this.concurrentMessageListenerContainer.getContainerProperties();
	}

	@Override
	public Collection<TopicPartition> getAssignedPartitions() {
		return this.concurrentMessageListenerContainer.getAssignedPartitions();
	}

	@Override
	public Map<String, Collection<TopicPartition>> getAssignmentsByClientId() {
		return this.concurrentMessageListenerContainer.getAssignmentsByClientId();
	}

	@Override
	public void enforceRebalance() {
		this.concurrentMessageListenerContainer.enforceRebalance();
	}

	@Override
	public void pause() {
		this.concurrentMessageListenerContainer.pause();
	}

	@Override
	public void resume() {
		this.concurrentMessageListenerContainer.resume();
	}

	@Override
	public void pausePartition(TopicPartition topicPartition) {
		this.concurrentMessageListenerContainer.pausePartition(topicPartition);
	}

	@Override
	public void resumePartition(TopicPartition topicPartition) {
		this.concurrentMessageListenerContainer.resumePartition(topicPartition);
	}

	@Override
	public boolean isPartitionPauseRequested(TopicPartition topicPartition) {
		return this.concurrentMessageListenerContainer.isPartitionPauseRequested(topicPartition);
	}

	@Override
	public boolean isPartitionPaused(TopicPartition topicPartition) {
		return this.concurrentMessageListenerContainer.isPartitionPaused(topicPartition);
	}

	@Override
	public boolean isPauseRequested() {
		return this.concurrentMessageListenerContainer.isPauseRequested();
	}

	@Override
	public boolean isContainerPaused() {
		return this.concurrentMessageListenerContainer.isContainerPaused();
	}

	@Override
	public String getGroupId() {
		return this.concurrentMessageListenerContainer.getGroupId();
	}

	@Override
	public String getListenerId() {
		return this.concurrentMessageListenerContainer.getListenerId();
	}

	@Override
	public String getMainListenerId() {
		return this.concurrentMessageListenerContainer.getMainListenerId();
	}

	@Override
	public byte[] getListenerInfo() {
		return this.concurrentMessageListenerContainer.getListenerInfo();
	}

	@Override
	public boolean isChildRunning() {
		return this.concurrentMessageListenerContainer.isChildRunning();
	}

	@Override
	public boolean isInExpectedState() {
		return this.concurrentMessageListenerContainer.isInExpectedState();
	}

	@Override
	public void stopAbnormally(Runnable callback) {
		this.lifecycleLock.lock();
		try {
			if (!this.kafkaMessageListenerContainer.isFenced()) {
				// kafkaMessageListenerContainer is not fenced. Allow stopAbnormally call on
				// concurrentMessageListenerContainer
				this.concurrentMessageListenerContainer.stopAbnormally(callback);
			}
			else if (this.concurrentMessageListenerContainer.isFenced() &&
					!this.concurrentMessageListenerContainer.isRunning()) {
				// kafkaMessageListenerContainer is fenced and concurrentMessageListenerContainer is not running. Allow
				// callback to run
				callback.run();
			}
			else {
				this.logger.error(() -> String.format("Suppressed `stopAbnormal` operation called by " +
						"MessageListenerContainer [" + this.kafkaMessageListenerContainer.getBeanName() + "]"));
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	protected void doStop(Runnable callback, boolean normal) {
		this.lifecycleLock.lock();
		try {
			if (!this.kafkaMessageListenerContainer.isFenced()) {
				// kafkaMessageListenerContainer is not fenced. Allow doStop call on
				// concurrentMessageListenerContainer
				this.concurrentMessageListenerContainer.doStop(callback, normal);
			}
			else if (this.concurrentMessageListenerContainer.isFenced() &&
					!this.concurrentMessageListenerContainer.isRunning()) {
				// kafkaMessageListenerContainer is fenced and concurrentMessageListenerContainer is not running. Allow
				// callback to run
				callback.run();
			}
			else {
				this.logger.error(() -> String.format("Suppressed `doStop` operation called by " +
						"MessageListenerContainer [" + this.kafkaMessageListenerContainer.getBeanName() + "]"));
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Override
	public MessageListenerContainer getContainerFor(String topic, int partition) {
		return this.concurrentMessageListenerContainer.getContainerFor(topic, partition);
	}

	@Override
	public void childStopped(MessageListenerContainer child, ConsumerStoppedEvent.Reason reason) {
		this.concurrentMessageListenerContainer.childStopped(child, reason);
	}

	@Override
	public void childStarted(MessageListenerContainer child) {
		this.concurrentMessageListenerContainer.childStarted(child);
	}

	@Override
	protected void doStart() {
		this.concurrentMessageListenerContainer.doStart();
	}

	@Override
	public boolean isRunning() {
		return this.concurrentMessageListenerContainer.isRunning();
	}

	@Override
	public boolean isAutoStartup() {
		return this.concurrentMessageListenerContainer.isAutoStartup();
	}

	@Override
	public void setAutoStartup(boolean autoStartup) {
		throw new UnsupportedOperationException("This container doesn't support `setAutoStartup`");
	}

	@Override
	public void stop(Runnable callback) {
		this.lifecycleLock.lock();
		try {
			if (!this.kafkaMessageListenerContainer.isFenced()) {
				// kafkaMessageListenerContainer is not fenced. Allow stop call on
				// concurrentMessageListenerContainer
				this.concurrentMessageListenerContainer.stop(callback);
			}
			else if (this.concurrentMessageListenerContainer.isFenced() &&
					!this.concurrentMessageListenerContainer.isRunning()) {
				// kafkaMessageListenerContainer is fenced and concurrentMessageListenerContainer is not running. Allow
				// callback to run
				callback.run();
			}
			else {
				this.logger.error(() -> String.format("Suppressed `stop` operation called by " +
						"MessageListenerContainer [" + this.kafkaMessageListenerContainer.getBeanName() + "]"));
			}
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	@Nullable
	protected ApplicationContext getApplicationContext() {
		return this.concurrentMessageListenerContainer.getApplicationContext();
	}

	/**
	 * Get the event publisher.
	 * @return the publisher
	 */
	@Nullable
	public ApplicationEventPublisher getApplicationEventPublisher() {
		return this.concurrentMessageListenerContainer.getApplicationEventPublisher();
	}

	/**
	 * Get the {@link CommonErrorHandler}.
	 * @return the handler.
	 * @since 2.8
	 */
	@Nullable
	public CommonErrorHandler getCommonErrorHandler() {
		return this.concurrentMessageListenerContainer.getCommonErrorHandler();
	}

	protected boolean isStoppedNormally() {
		return this.concurrentMessageListenerContainer.isStoppedNormally();
	}

	protected void setStoppedNormally(boolean stoppedNormally) {
		this.concurrentMessageListenerContainer.setStoppedNormally(stoppedNormally);
	}

	protected void setRunning(boolean running) {
		this.concurrentMessageListenerContainer.setRunning(running);
	}

	protected boolean isEnforceRebalanceRequested() {
		return this.concurrentMessageListenerContainer.isEnforceRebalanceRequested();
	}

	protected void setEnforceRebalanceRequested(boolean enforceRebalance) {
		this.concurrentMessageListenerContainer.setEnforceRebalanceRequested(enforceRebalance);
	}

	/**
	 * Return the currently configured {@link AfterRollbackProcessor}.
	 * @return the after rollback processor.
	 * @since 2.2.14
	 */
	public AfterRollbackProcessor<? super K, ? super V> getAfterRollbackProcessor() {
		return this.concurrentMessageListenerContainer.getAfterRollbackProcessor();
	}

	public boolean isChangeConsumerThreadName() {
		return this.concurrentMessageListenerContainer.isChangeConsumerThreadName();
	}

	/**
	 * Set to true to instruct the container to change the consumer thread name during
	 * initialization.
	 * @param changeConsumerThreadName true to change.
	 * @since 3.0.1
	 * @see #setThreadNameSupplier(Function)
	 */
	public void setChangeConsumerThreadName(boolean changeConsumerThreadName) {
		this.concurrentMessageListenerContainer.setChangeConsumerThreadName(changeConsumerThreadName);
	}

	/**
	 * Return the {@link KafkaAdmin}, used to find the cluster id for observation, if
	 * present.
	 * @return the kafkaAdmin
	 * @since 3.0.5
	 */
	@Nullable
	public KafkaAdmin getKafkaAdmin() {
		return this.concurrentMessageListenerContainer.getKafkaAdmin();
	}

	public void setKafkaAdmin(KafkaAdmin kafkaAdmin) {
		this.concurrentMessageListenerContainer.setKafkaAdmin(kafkaAdmin);
	}

	protected RecordInterceptor<K, V> getRecordInterceptor() {
		return this.concurrentMessageListenerContainer.getRecordInterceptor();
	}

	/**
	 * Set an interceptor to be called before calling the record listener.
	 * Does not apply to batch listeners.
	 * @param recordInterceptor the interceptor.
	 * @since 2.2.7
	 * @see #setInterceptBeforeTx(boolean)
	 */
	public void setRecordInterceptor(RecordInterceptor recordInterceptor) {
		this.concurrentMessageListenerContainer.setRecordInterceptor(recordInterceptor);
	}

	protected BatchInterceptor<K, V> getBatchInterceptor() {
		return this.concurrentMessageListenerContainer.getBatchInterceptor();
	}

	/**
	 * Set an interceptor to be called before calling the record listener.
	 * @param batchInterceptor the interceptor.
	 * @since 2.6.6
	 * @see #setInterceptBeforeTx(boolean)
	 */
	public void setBatchInterceptor(BatchInterceptor batchInterceptor) {
		this.concurrentMessageListenerContainer.setBatchInterceptor(batchInterceptor);
	}

	protected boolean isInterceptBeforeTx() {
		return this.concurrentMessageListenerContainer.isInterceptBeforeTx();
	}

	/**
	 * When false, invoke the interceptor after the transaction starts.
	 * @param interceptBeforeTx false to intercept within the transaction.
	 * Default true since 2.8.
	 * @since 2.3.4
	 * @see #setRecordInterceptor(RecordInterceptor)
	 * @see #setBatchInterceptor(BatchInterceptor)
	 */
	public void setInterceptBeforeTx(boolean interceptBeforeTx) {
		this.concurrentMessageListenerContainer.setInterceptBeforeTx(interceptBeforeTx);
	}

	/**
	 * Return this or a parent container if this has a parent.
	 * @return the parent or this.
	 * @since 2.2.1
	 */
	protected AbstractMessageListenerContainer<?, ?> parentOrThis() {
		return this;
	}

}
