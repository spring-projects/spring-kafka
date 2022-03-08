/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.kafka.requestreply;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;

/**
 * A KafkaTemplate that implements request/reply semantics.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.3
 *
 */
public class ReplyingKafkaTemplate<K, V, R> extends KafkaTemplate<K, V> implements BatchMessageListener<K, R>,
		InitializingBean, SmartLifecycle, DisposableBean, ReplyingKafkaOperations<K, V, R> {

	private static final String WITH_CORRELATION_ID = " with correlationId: ";

	private static final int FIVE = 5;

	private static final Duration DEFAULT_REPLY_TIMEOUT = Duration.ofSeconds(FIVE);

	private final GenericMessageListenerContainer<K, R> replyContainer;

	private final ConcurrentMap<CorrelationKey, RequestReplyFuture<K, V, R>> futures = new ConcurrentHashMap<>();

	private final byte[] replyTopic;

	private final byte[] replyPartition;

	private TaskScheduler scheduler = new ThreadPoolTaskScheduler();

	private int phase;

	private boolean autoStartup = true;

	private Duration defaultReplyTimeout = DEFAULT_REPLY_TIMEOUT;

	private boolean schedulerSet;

	private boolean sharedReplyTopic;

	private Function<ProducerRecord<K, V>, CorrelationKey> correlationStrategy = ReplyingKafkaTemplate::defaultCorrelationIdStrategy;

	private String correlationHeaderName = KafkaHeaders.CORRELATION_ID;

	private String replyTopicHeaderName = KafkaHeaders.REPLY_TOPIC;

	private String replyPartitionHeaderName = KafkaHeaders.REPLY_PARTITION;

	private Function<ConsumerRecord<?, ?>, Exception> replyErrorChecker = rec -> null;

	private volatile boolean running;

	private volatile boolean schedulerInitialized;

	public ReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, R> replyContainer) {

		this(producerFactory, replyContainer, false);
	}

	public ReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, R> replyContainer, boolean autoFlush) {

		super(producerFactory, autoFlush);
		Assert.notNull(replyContainer, "'replyContainer' cannot be null");
		this.replyContainer = replyContainer;
		this.replyContainer.setupMessageListener(this);
		ContainerProperties properties = this.replyContainer.getContainerProperties();
		String tempReplyTopic = null;
		byte[] tempReplyPartition = null;
		TopicPartitionOffset[] topicPartitionsToAssign = properties.getTopicPartitions();
		String[] topics = properties.getTopics();
		if (topics != null && topics.length == 1) {
			tempReplyTopic = topics[0];
		}
		else if (topicPartitionsToAssign != null && topicPartitionsToAssign.length == 1) {
			TopicPartitionOffset topicPartitionOffset = topicPartitionsToAssign[0];
			Assert.notNull(topicPartitionOffset, "'topicPartitionsToAssign' must not be null");
			tempReplyTopic = topicPartitionOffset.getTopic();
			ByteBuffer buffer = ByteBuffer.allocate(4); // NOSONAR magic #
			buffer.putInt(topicPartitionOffset.getPartition());
			tempReplyPartition = buffer.array();
		}
		if (tempReplyTopic == null) {
			this.replyTopic = null;
			this.replyPartition = null;
			this.logger.debug(() -> "Could not determine container's reply topic/partition; senders must populate "
					+ "at least the " + KafkaHeaders.REPLY_TOPIC + " header, and optionally the "
					+ KafkaHeaders.REPLY_PARTITION + " header");
		}
		else {
			this.replyTopic = tempReplyTopic.getBytes(StandardCharsets.UTF_8);
			this.replyPartition = tempReplyPartition;
		}
	}

	public void setTaskScheduler(TaskScheduler scheduler) {
		Assert.notNull(scheduler, "'scheduler' cannot be null");
		this.scheduler = scheduler;
		this.schedulerSet = true;
	}

	/**
	 * Return the reply timeout used if no replyTimeout is provided in the
	 * {@link #sendAndReceive(ProducerRecord, Duration)} call.
	 * @return the timeout.
	 * @since 2.3
	 */
	protected Duration getDefaultReplyTimeout() {
		return this.defaultReplyTimeout;
	}

	/**
	 * Set the reply timeout used if no replyTimeout is provided in the
	 * {@link #sendAndReceive(ProducerRecord, Duration)} call.
	 * @param defaultReplyTimeout the timeout.
	 * @since 2.3
	 */
	public void setDefaultReplyTimeout(Duration defaultReplyTimeout) {
		Assert.notNull(defaultReplyTimeout, "'defaultReplyTimeout' cannot be null");
		Assert.isTrue(defaultReplyTimeout.toMillis() >= 0, "'replyTimeout' must be >= 0");
		this.defaultReplyTimeout = defaultReplyTimeout;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Return the topics/partitions assigned to the replying listener container.
	 * @return the topics/partitions.
	 */
	public Collection<TopicPartition> getAssignedReplyTopicPartitions() {
		return this.replyContainer.getAssignedPartitions();
	}

	/**
	 * Set to true when multiple templates are using the same topic for replies. This
	 * simply changes logs for unexpected replies to debug instead of error.
	 * @param sharedReplyTopic true if using a shared topic.
	 * @since 2.2
	 */
	public void setSharedReplyTopic(boolean sharedReplyTopic) {
		this.sharedReplyTopic = sharedReplyTopic;
	}

	/**
	 * Set a function to be called to establish a unique correlation key for each request
	 * record.
	 * @param correlationStrategy the function.
	 * @since 2.3
	 */
	public void setCorrelationIdStrategy(Function<ProducerRecord<K, V>, CorrelationKey> correlationStrategy) {
		Assert.notNull(correlationStrategy, "'correlationStrategy' cannot be null");
		this.correlationStrategy = correlationStrategy;
	}

	/**
	 * Set a custom header name for the correlation id. Default
	 * {@link KafkaHeaders#CORRELATION_ID}.
	 * @param correlationHeaderName the header name.
	 * @since 2.3
	 */
	public void setCorrelationHeaderName(String correlationHeaderName) {
		Assert.notNull(correlationHeaderName, "'correlationHeaderName' cannot be null");
		this.correlationHeaderName = correlationHeaderName;
	}

	/**
	 * Set a custom header name for the reply topic. Default
	 * {@link KafkaHeaders#REPLY_TOPIC}.
	 * @param replyTopicHeaderName the header name.
	 * @since 2.3
	 */
	public void setReplyTopicHeaderName(String replyTopicHeaderName) {
		Assert.notNull(replyTopicHeaderName, "'replyTopicHeaderName' cannot be null");
		this.replyTopicHeaderName = replyTopicHeaderName;
	}

	/**
	 * Set a custom header name for the reply partition. Default
	 * {@link KafkaHeaders#REPLY_PARTITION}.
	 * @param replyPartitionHeaderName the reply partition header name.
	 * @since 2.3
	 */
	public void setReplyPartitionHeaderName(String replyPartitionHeaderName) {
		Assert.notNull(replyPartitionHeaderName, "'replyPartitionHeaderName' cannot be null");
		this.replyPartitionHeaderName = replyPartitionHeaderName;
	}

	/**
	 * Set a function to examine replies for an error returned by the server.
	 * @param replyErrorChecker the error checker function.
	 * @since 2.6.7
	 */
	public void setReplyErrorChecker(Function<ConsumerRecord<?, ?>, Exception> replyErrorChecker) {
		Assert.notNull(replyErrorChecker, "'replyErrorChecker' cannot be null");
		this.replyErrorChecker = replyErrorChecker;
	}

	@Override
	public void afterPropertiesSet() {
		if (!this.schedulerSet && !this.schedulerInitialized) {
			((ThreadPoolTaskScheduler) this.scheduler).initialize();
			this.schedulerInitialized = true;
		}
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			try {
				afterPropertiesSet();
			}
			catch (Exception e) {
				throw new KafkaException("Failed to initialize", e);
			}
			this.replyContainer.start();
			this.running = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.running = false;
			this.replyContainer.stop();
			this.futures.clear();
		}
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public RequestReplyMessageFuture<K, V> sendAndReceive(Message<?> message) {
		return sendAndReceive(message, this.defaultReplyTimeout, null);
	}

	@Override
	public RequestReplyMessageFuture<K, V> sendAndReceive(Message<?> message, Duration replyTimeout) {
		return sendAndReceive(message, replyTimeout, null);
	}

	@Override
	public <P> RequestReplyTypedMessageFuture<K, V, P> sendAndReceive(Message<?> message,
			@Nullable ParameterizedTypeReference<P> returnType) {

		return sendAndReceive(message, this.defaultReplyTimeout, returnType);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <P> RequestReplyTypedMessageFuture<K, V, P> sendAndReceive(Message<?> message,
			@Nullable Duration replyTimeout,
			@Nullable ParameterizedTypeReference<P> returnType) {

		RequestReplyFuture<K, V, R> future = sendAndReceive((ProducerRecord<K, V>) getMessageConverter()
				.fromMessage(message, getDefaultTopic()), replyTimeout);
		RequestReplyTypedMessageFuture<K, V, P> replyFuture =
				new RequestReplyTypedMessageFuture<>(future.getSendFuture());
		future.addCallback(
				result -> {
					try {
						replyFuture.set(getMessageConverter()
							.toMessage(result, null, null, returnType == null ? null : returnType.getType()));
					}
					catch (Exception ex) { // NOSONAR
						replyFuture.setException(ex);
					}
				},
				ex -> replyFuture.setException(ex));
		return replyFuture;
	}

	@Override
	public RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record) {
		return sendAndReceive(record, this.defaultReplyTimeout);
	}

	@Override
	public RequestReplyFuture<K, V, R> sendAndReceive(ProducerRecord<K, V> record, @Nullable Duration replyTimeout) {
		Assert.state(this.running, "Template has not been start()ed"); // NOSONAR (sync)
		Duration timeout = replyTimeout;
		if (timeout == null) {
			timeout = this.defaultReplyTimeout;
		}
		CorrelationKey correlationId = this.correlationStrategy.apply(record);
		Assert.notNull(correlationId, "the created 'correlationId' cannot be null");
		Headers headers = record.headers();
		boolean hasReplyTopic = headers.lastHeader(KafkaHeaders.REPLY_TOPIC) != null;
		if (!hasReplyTopic && this.replyTopic != null) {
			headers.add(new RecordHeader(this.replyTopicHeaderName, this.replyTopic));
			if (this.replyPartition != null) {
				headers.add(new RecordHeader(this.replyPartitionHeaderName, this.replyPartition));
			}
		}
		headers.add(new RecordHeader(this.correlationHeaderName, correlationId.getCorrelationId()));
		this.logger.debug(() -> "Sending: " + KafkaUtils.format(record) + WITH_CORRELATION_ID + correlationId);
		RequestReplyFuture<K, V, R> future = new RequestReplyFuture<>();
		this.futures.put(correlationId, future);
		try {
			future.setSendFuture(send(record));
		}
		catch (Exception e) {
			this.futures.remove(correlationId);
			throw new KafkaException("Send failed", e);
		}
		scheduleTimeout(record, correlationId, timeout);
		return future;
	}

	private void scheduleTimeout(ProducerRecord<K, V> record, CorrelationKey correlationId, Duration replyTimeout) {
		this.scheduler.schedule(() -> {
			RequestReplyFuture<K, V, R> removed = this.futures.remove(correlationId);
			if (removed != null) {
				this.logger.warn(() -> "Reply timed out for: " + KafkaUtils.format(record)
						+ WITH_CORRELATION_ID + correlationId);
				if (!handleTimeout(correlationId, removed)) {
					removed.setException(new KafkaReplyTimeoutException("Reply timed out"));
				}
			}
		}, Instant.now().plus(replyTimeout));
	}

	/**
	 * Used to inform subclasses that a request has timed out so they can clean up state
	 * and, optionally, complete the future.
	 * @param correlationId the correlation id.
	 * @param future the future.
	 * @return true to indicate the future has been completed.
	 * @since 2.3
	 */
	protected boolean handleTimeout(@SuppressWarnings("unused") CorrelationKey correlationId,
			@SuppressWarnings("unused") RequestReplyFuture<K, V, R> future) {

		return false;
	}

	/**
	 * Return true if this correlation id is still active.
	 * @param correlationId the correlation id.
	 * @return true if pending.
	 * @since 2.3
	 */
	protected boolean isPending(CorrelationKey correlationId) {
		return this.futures.containsKey(correlationId);
	}

	@Override
	public void destroy() {
		if (!this.schedulerSet) {
			((ThreadPoolTaskScheduler) this.scheduler).destroy();
		}
	}

	private static <K, V> CorrelationKey defaultCorrelationIdStrategy(
			@SuppressWarnings("unused") ProducerRecord<K, V> record) {

		UUID uuid = UUID.randomUUID();
		byte[] bytes = new byte[16]; // NOSONAR magic #
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return new CorrelationKey(bytes);
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, R>> data) {
		data.forEach(record -> {
			Header correlationHeader = record.headers().lastHeader(this.correlationHeaderName);
			CorrelationKey correlationId = null;
			if (correlationHeader != null) {
				correlationId = new CorrelationKey(correlationHeader.value());
			}
			if (correlationId == null) {
				this.logger.error(() -> "No correlationId found in reply: " + KafkaUtils.format(record)
						+ " - to use request/reply semantics, the responding server must return the correlation id "
						+ " in the '" + this.correlationHeaderName + "' header");
			}
			else {
				RequestReplyFuture<K, V, R> future = this.futures.remove(correlationId);
				CorrelationKey correlationKey = correlationId;
				if (future == null) {
					logLateArrival(record, correlationId);
				}
				else {
					boolean ok = true;
					Exception exception = checkForErrors(record);
					if (exception != null) {
						ok = false;
						future.setException(exception);
					}
					if (ok) {
						this.logger.debug(() -> "Received: " + KafkaUtils.format(record)
								+ WITH_CORRELATION_ID + correlationKey);
						future.set(record);
					}
				}
			}
		});
	}

	/**
	 * Check for errors in a reply. The default implementation checks for {@link DeserializationException}s
	 * and invokes the {@link #setReplyErrorChecker(Function) replyErrorChecker} function.
	 * @param record the record.
	 * @return the exception, or null if none.
	 * @since 2.6.7
	 */
	@Nullable
	protected Exception checkForErrors(ConsumerRecord<K, R> record) {
		if (record.value() == null || record.key() == null) {
			DeserializationException de = checkDeserialization(record, this.logger);
			if (de != null) {
				return de;
			}
		}
		return this.replyErrorChecker.apply(record);
	}

	/**
	 * Return a {@link DeserializationException} if either the key or value failed
	 * deserialization; null otherwise. If you need to determine whether it was the key or
	 * value, call
	 * {@link ListenerUtils#getExceptionFromHeader(ConsumerRecord, String, LogAccessor)}
	 * with {@link ErrorHandlingDeserializer#KEY_DESERIALIZER_EXCEPTION_HEADER} and
	 * {@link ErrorHandlingDeserializer#VALUE_DESERIALIZER_EXCEPTION_HEADER} instead.
	 * @param record the record.
	 * @param logger a {@link LogAccessor}.
	 * @return the {@link DeserializationException} or {@code null}.
	 * @since 2.2.15
	 */
	@Nullable
	public static DeserializationException checkDeserialization(ConsumerRecord<?, ?> record, LogAccessor logger) {
		DeserializationException exception = ListenerUtils.getExceptionFromHeader(record,
				ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER, logger);
		if (exception != null) {
			logger.error(exception, () -> "Reply value deserialization failed for " + record.topic() + "-"
					+ record.partition() + "@" + record.offset());
			return exception;
		}
		exception = ListenerUtils.getExceptionFromHeader(record,
				ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER, logger);
		if (exception != null) {
			logger.error(exception, () -> "Reply key deserialization failed for " + record.topic() + "-"
					+ record.partition() + "@" + record.offset());
			return exception;
		}
		return null;
	}

	protected void logLateArrival(ConsumerRecord<K, R> record, CorrelationKey correlationId) {
		if (this.sharedReplyTopic) {
			this.logger.debug(() -> missingCorrelationLogMessage(record, correlationId));
		}
		else {
			this.logger.error(() -> missingCorrelationLogMessage(record, correlationId));
		}
	}

	private String missingCorrelationLogMessage(ConsumerRecord<K, R> record, CorrelationKey correlationId) {
		return "No pending reply: " + KafkaUtils.format(record) + WITH_CORRELATION_ID
				+ correlationId + ", perhaps timed out, or using a shared reply topic";
	}

}
