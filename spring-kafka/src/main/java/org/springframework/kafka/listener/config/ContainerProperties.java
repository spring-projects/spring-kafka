/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.listener.config;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;

import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.GenericErrorHandler;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.util.Assert;

/**
 * Contains runtime properties for a listener container.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Artem Yakshin
 */
public class ContainerProperties {

	private static final int DEFAULT_SHUTDOWN_TIMEOUT = 10000;

	private static final int DEFAULT_QUEUE_DEPTH = 1;

	private static final int DEFAULT_PAUSE_AFTER = 10000;

	/**
	 * Topic names.
	 */
	private final String[] topics;

	/**
	 * Topic pattern.
	 */
	private final Pattern topicPattern;

	/**
	 * Topics/partitions/initial offsets.
	 */
	private final TopicPartitionInitialOffset[] topicPartitions;

	/**
	 * The ack mode to use when auto ack (in the configuration properties) is false.
	 * <ul>
	 * <li>RECORD: Ack after each record has been passed to the listener.</li>
	 * <li>BATCH: Ack after each batch of records received from the consumer has been
	 * passed to the listener</li>
	 * <li>TIME: Ack after this number of milliseconds; (should be greater than
	 * {@code #setPollTimeout(long) pollTimeout}.</li>
	 * <li>COUNT: Ack after at least this number of records have been received</li>
	 * <li>MANUAL: Listener is responsible for acking - use a
	 * {@link org.springframework.kafka.listener.AcknowledgingMessageListener}.
	 * </ul>
	 */
	private AbstractMessageListenerContainer.AckMode ackMode = AckMode.BATCH;

	/**
	 * The number of outstanding record count after which offsets should be
	 * committed when {@link AckMode#COUNT} or {@link AckMode#COUNT_TIME} is being
	 * used.
	 */
	private int ackCount;

	/**
	 * The time (ms) after which outstanding offsets should be committed when
	 * {@link AckMode#TIME} or {@link AckMode#COUNT_TIME} is being used. Should be
	 * larger than
	 */
	private long ackTime;

	/**
	 * The message listener; must be a {@link org.springframework.kafka.listener.MessageListener}
	 * or {@link org.springframework.kafka.listener.AcknowledgingMessageListener}.
	 */
	private Object messageListener;

	/**
	 * The max time to block in the consumer waiting for records.
	 */
	private volatile long pollTimeout = 1000;

	/**
	 * The executor for threads that poll the consumer.
	 */
	private AsyncListenableTaskExecutor consumerTaskExecutor;

	/**
	 * The executor for threads that invoke the listener.
	 */
	private AsyncListenableTaskExecutor listenerTaskExecutor;

	/**
	 * The error handler to call when the listener throws an exception.
	 */
	private GenericErrorHandler<?> errorHandler;

	/**
	 * When using Kafka group management and {@link #setPauseEnabled(boolean)} is
	 * true, the delay after which the consumer should be paused. Default 10000.
	 */
	private long pauseAfter = DEFAULT_PAUSE_AFTER;

	/**
	 * When true, avoids rebalancing when this consumer is slow or throws a
	 * qualifying exception - pauses the consumer. Default: true.
	 * @see #pauseAfter
	 */
	private boolean pauseEnabled = true;

	/**
	 * Set the queue depth for handoffs from the consumer thread to the listener
	 * thread. Default 1 (up to 2 in process).
	 */
	private int queueDepth = DEFAULT_QUEUE_DEPTH;

	/**
	 * The timeout for shutting down the container. This is the maximum amount of
	 * time that the invocation to {@code #stop(Runnable)} will block for, before
	 * returning.
	 */
	private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

	/**
	 * A user defined {@link ConsumerRebalanceListener} implementation.
	 */
	private ConsumerRebalanceListener consumerRebalanceListener;

	/**
	 * The commit callback; by default a simple logging callback is used to log
	 * success at DEBUG level and failures at ERROR level.
	 */
	private OffsetCommitCallback commitCallback;

	/**
	 * Whether or not to call consumer.commitSync() or commitAsync() when the
	 * container is responsible for commits. Default true. See
	 * https://github.com/spring-projects/spring-kafka/issues/62 At the time of
	 * writing, async commits are not entirely reliable.
	 */
	private boolean syncCommits = true;

	private boolean ackOnError = true;

	private Long idleEventInterval;

	public ContainerProperties(String... topics) {
		Assert.notEmpty(topics, "An array of topicPartitions must be provided");
		this.topics = Arrays.asList(topics).toArray(new String[topics.length]);
		this.topicPattern = null;
		this.topicPartitions = null;
	}

	public ContainerProperties(Pattern topicPattern) {
		this.topics = null;
		this.topicPattern = topicPattern;
		this.topicPartitions = null;
	}

	public ContainerProperties(TopicPartitionInitialOffset... topicPartitions) {
		this.topics = null;
		this.topicPattern = null;
		Assert.notEmpty(topicPartitions, "An array of topicPartitions must be provided");
		this.topicPartitions = new LinkedHashSet<>(Arrays.asList(topicPartitions))
				.toArray(new TopicPartitionInitialOffset[topicPartitions.length]);
	}

	/**
	 * Set the message listener; must be a {@link org.springframework.kafka.listener.MessageListener}
	 * or {@link org.springframework.kafka.listener.AcknowledgingMessageListener}.
	 * @param messageListener the listener.
	 */
	public void setMessageListener(Object messageListener) {
		this.messageListener = messageListener;
	}

	/**
	 * Set the ack mode to use when auto ack (in the configuration properties) is false.
	 * <ul>
	 * <li>RECORD: Ack after each record has been passed to the listener.</li>
	 * <li>BATCH: Ack after each batch of records received from the consumer has been
	 * passed to the listener</li>
	 * <li>TIME: Ack after this number of milliseconds; (should be greater than
	 * {@code #setPollTimeout(long) pollTimeout}.</li>
	 * <li>COUNT: Ack after at least this number of records have been received</li>
	 * <li>MANUAL: Listener is responsible for acking - use a
	 * {@link org.springframework.kafka.listener.AcknowledgingMessageListener}.
	 * </ul>
	 * @param ackMode the {@link AckMode}; default BATCH.
	 */
	public void setAckMode(AbstractMessageListenerContainer.AckMode ackMode) {
		this.ackMode = ackMode;
	}

	/**
	 * Set the max time to block in the consumer waiting for records.
	 * @param pollTimeout the timeout in ms; default 1000.
	 */
	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * Set the number of outstanding record count after which offsets should be
	 * committed when {@link AckMode#COUNT} or {@link AckMode#COUNT_TIME} is being used.
	 * @param count the count
	 */
	public void setAckCount(int count) {
		Assert.state(count > 0, "'ackCount' must be > 0");
		this.ackCount = count;
	}

	/**
	 * Set the time (ms) after which outstanding offsets should be committed when
	 * {@link AckMode#TIME} or {@link AckMode#COUNT_TIME} is being used. Should be
	 * larger than
	 * @param ackTime the time
	 */
	public void setAckTime(long ackTime) {
		Assert.state(ackTime > 0, "'ackTime' must be > 0");
		this.ackTime = ackTime;
	}

	/**
	 * Set the error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 * @see #setGenericErrorHandler(GenericErrorHandler)
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the error handler to call when the listener throws an exception.
	 * @param errorHandler the error handler.
	 */
	public void setGenericErrorHandler(GenericErrorHandler<?> errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the executor for threads that poll the consumer.
	 * @param consumerTaskExecutor the executor
	 */
	public void setConsumerTaskExecutor(AsyncListenableTaskExecutor consumerTaskExecutor) {
		this.consumerTaskExecutor = consumerTaskExecutor;
	}

	/**
	 * Set the executor for threads that invoke the listener.
	 * @param listenerTaskExecutor the executor.
	 */
	public void setListenerTaskExecutor(AsyncListenableTaskExecutor listenerTaskExecutor) {
		this.listenerTaskExecutor = listenerTaskExecutor;
	}

	/**
	 * When using Kafka group management and {@link #setPauseEnabled(boolean)} is
	 * true, set the delay after which the consumer should be paused. Default 10000.
	 * @param pauseAfter the delay.
	 */
	public void setPauseAfter(long pauseAfter) {
		this.pauseAfter = pauseAfter;
	}

	/**
	 * Set to true to avoid rebalancing when this consumer is slow or throws a
	 * qualifying exception - pause the consumer. Default: true.
	 * @param pauseEnabled true to pause.
	 * @see #setPauseAfter(long)
	 */
	public void setPauseEnabled(boolean pauseEnabled) {
		this.pauseEnabled = pauseEnabled;
	}

	/**
	 * Set the queue depth for handoffs from the consumer thread to the listener
	 * thread. Default 1 (up to 2 in process).
	 * @param queueDepth the queue depth.
	 */
	public void setQueueDepth(int queueDepth) {
		this.queueDepth = queueDepth;
	}

	/**
	 * Set the timeout for shutting down the container. This is the maximum amount of
	 * time that the invocation to {@code #stop(Runnable)} will block for, before
	 * returning.
	 * @param shutdownTimeout the shutdown timeout.
	 */
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	/**
	 * Set the user defined {@link ConsumerRebalanceListener} implementation.
	 * @param consumerRebalanceListener the {@link ConsumerRebalanceListener} instance
	 */
	public void setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
		this.consumerRebalanceListener = consumerRebalanceListener;
	}

	/**
	 * Set the commit callback; by default a simple logging callback is used to log
	 * success at DEBUG level and failures at ERROR level.
	 * @param commitCallback the callback.
	 */
	public void setCommitCallback(OffsetCommitCallback commitCallback) {
		this.commitCallback = commitCallback;
	}

	/**
	 * Set whether or not to call consumer.commitSync() or commitAsync() when the
	 * container is responsible for commits. Default true. See
	 * https://github.com/spring-projects/spring-kafka/issues/62 At the time of
	 * writing, async commits are not entirely reliable.
	 * @param syncCommits true to use commitSync().
	 */
	public void setSyncCommits(boolean syncCommits) {
		this.syncCommits = syncCommits;
	}

	/**
	 * Set the idle event interval; when set, an event is emitted if a poll returns
	 * no records and this interval has elapsed since a record was returned.
	 * @param idleEventInterval the interval.
	 */
	public void setIdleEventInterval(Long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	/**
	 * Set whether or not the container should commit offsets (ack messages) where the
	 * listener throws exceptions. This works in conjunction with {@link #ackMode} and is
	 * effective only when the kafka property {@code enable.auto.commit} is {@code false};
	 * it is not applicable to manual ack modes. When this property is set to {@code true}
	 * (the default), all messages handled will have their offset committed. When set to
	 * {@code false}, offsets will be committed only for successfully handled messages.
	 * Manual acks will always be applied. Bear in mind that, if the next message is
	 * successfully handled, its offset will be committed, effectively committing the
	 * offset of the failed message anyway, so this option has limited applicability.
	 * Perhaps useful for a component that starts throwing exceptions consistently;
	 * allowing it to resume when restarted from the last successfully processed message.
	 * @param ackOnError whether the container should acknowledge messages that throw
	 * exceptions.
	 */
	public void setAckOnError(boolean ackOnError) {
		this.ackOnError = ackOnError;
	}

	public String[] getTopics() {
		return this.topics;
	}

	public Pattern getTopicPattern() {
		return this.topicPattern;
	}

	public TopicPartitionInitialOffset[] getTopicPartitions() {
		return this.topicPartitions;
	}

	public AbstractMessageListenerContainer.AckMode getAckMode() {
		return this.ackMode;
	}

	public int getAckCount() {
		return this.ackCount;
	}

	public long getAckTime() {
		return this.ackTime;
	}

	public Object getMessageListener() {
		return this.messageListener;
	}

	public long getPollTimeout() {
		return this.pollTimeout;
	}

	public AsyncListenableTaskExecutor getConsumerTaskExecutor() {
		return this.consumerTaskExecutor;
	}

	public AsyncListenableTaskExecutor getListenerTaskExecutor() {
		return this.listenerTaskExecutor;
	}

	public GenericErrorHandler<?> getGenericErrorHandler() {
		return this.errorHandler;
	}

	public long getPauseAfter() {
		return this.pauseAfter;
	}

	public boolean isPauseEnabled() {
		return this.pauseEnabled;
	}

	public int getQueueDepth() {
		return this.queueDepth;
	}

	public long getShutdownTimeout() {
		return this.shutdownTimeout;
	}

	public ConsumerRebalanceListener getConsumerRebalanceListener() {
		return this.consumerRebalanceListener;
	}

	public OffsetCommitCallback getCommitCallback() {
		return this.commitCallback;
	}

	public boolean isSyncCommits() {
		return this.syncCommits;
	}

	public Long getIdleEventInterval() {
		return this.idleEventInterval;
	}

	public boolean isAckOnError() {
		return this.ackOnError &&
				!(AckMode.MANUAL_IMMEDIATE.equals(this.ackMode) || AckMode.MANUAL.equals(this.ackMode));
	}

}
