/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.listener.adapter;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.util.Assert;

/**
 *
 * A {@link AcknowledgingConsumerAwareMessageListener} implementation that looks for a
 * backoff dueTimestamp header and invokes a {@link KafkaConsumerBackoffManager} instance
 * that will back off if necessary.
 *
 * @param <K> the record key type.
 * @param <V> the record value type.
 * @author Tomaz Fernandes
 * @author Soby Chacko
 * @author Manoel Angelo
 * @since 2.7
 *
 */
public class KafkaBackoffAwareMessageListenerAdapter<K, V>
		extends AbstractDelegatingMessageListenerAdapter<MessageListener<K, V>>
		implements AcknowledgingConsumerAwareMessageListener<K, V> {

	private static final long DEFAULT_MAX_BACKOFF_MS = Duration.ofDays(1).toMillis();

	private static final long CLOCK_SKEW_TOLERANCE_MS = Duration.ofMinutes(1).toMillis();

	private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;

	private final String listenerId;

	private final String backoffTimestampHeader;

	private final Clock clock;

	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	/**
	 * The configuration for this listener adapter.
	 *
	 * @param delegate the MessageListener instance that will handle the messages.
	 * @param kafkaConsumerBackoffManager the manager that will handle the back off.
	 * @param listenerId the id of the listener container associated to this adapter.
	 * @param backoffTimestampHeader the header name that will be looked up in the
	 * 			incoming record to acquire the timestamp.
	 * @param clock the clock instance that will be used to timestamp the
	 * 			exception throwing.
	 * @since 2.7
	 */
	public KafkaBackoffAwareMessageListenerAdapter(MessageListener<K, V> delegate,
												KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
												String listenerId,
												String backoffTimestampHeader,
												Clock clock) {
		super(delegate);
		this.listenerId = listenerId;
		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.backoffTimestampHeader = backoffTimestampHeader;
		this.clock = clock;
	}

	public KafkaBackoffAwareMessageListenerAdapter(MessageListener<K, V> adapter,
			KafkaConsumerBackoffManager kafkaConsumerBackoffManager, String listenerId, Clock clock) throws KafkaBackoffException {
		this(adapter, kafkaConsumerBackoffManager, listenerId, RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP, clock);
	}

	/**
	 * Set the longest back off configured for the topics this adapter consumes from.
	 * A back off timestamp header further in the future than one day is honored only
	 * when it is within this back off, plus a small tolerance for clock skew; otherwise
	 * the header is ignored and the record is processed immediately. Back offs of one
	 * day or less do not change the default limit of one day.
	 * @param maxConfiguredBackOff the longest configured back off.
	 * @since 4.2.1
	 */
	public void setMaxConfiguredBackOff(Duration maxConfiguredBackOff) {
		Assert.notNull(maxConfiguredBackOff, "'maxConfiguredBackOff' cannot be null");
		Assert.isTrue(!maxConfiguredBackOff.isNegative(), "'maxConfiguredBackOff' cannot be negative");
		long backOffMs = maxConfiguredBackOff.compareTo(Duration.ofMillis(Long.MAX_VALUE - CLOCK_SKEW_TOLERANCE_MS)) > 0
				? Long.MAX_VALUE
				: maxConfiguredBackOff.toMillis() + CLOCK_SKEW_TOLERANCE_MS;
		this.maxBackoffMs = Math.max(DEFAULT_MAX_BACKOFF_MS, backOffMs);
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> consumerRecord, @Nullable Acknowledgment acknowledgment,
								@Nullable Consumer<?, ?> consumer) {
		maybeGetBackoffTimestamp(consumerRecord)
				.ifPresent(nextExecutionTimestamp -> this.kafkaConsumerBackoffManager
						.backOffIfNecessary(createContext(consumerRecord, nextExecutionTimestamp, consumer)));
		try {
			invokeDelegateOnMessage(consumerRecord, acknowledgment, consumer);
		}
		catch (Exception ex) {
			throw new TimestampedException(ex, Instant.now(this.clock));
		}
	}

	private void invokeDelegateOnMessage(ConsumerRecord<K, V> consumerRecord, @Nullable Acknowledgment acknowledgment, @Nullable Consumer<?, ?> consumer) {
		switch (this.delegateType) {
			case ACKNOWLEDGING_CONSUMER_AWARE -> this.delegate.onMessage(consumerRecord, acknowledgment, consumer);
			case ACKNOWLEDGING -> this.delegate.onMessage(consumerRecord, acknowledgment);
			case CONSUMER_AWARE -> this.delegate.onMessage(consumerRecord, consumer);
			case SIMPLE -> this.delegate.onMessage(consumerRecord);
		}
	}

	private KafkaConsumerBackoffManager.Context createContext(ConsumerRecord<K, V> data, long nextExecutionTimestamp,
			@Nullable Consumer<?, ?> consumer) {

		return this.kafkaConsumerBackoffManager.createContext(nextExecutionTimestamp, this.listenerId,
				new TopicPartition(data.topic(), data.partition()), consumer);
	}

	private Optional<Long> maybeGetBackoffTimestamp(ConsumerRecord<K, V> data) {
		return Optional
				.ofNullable(data.headers().lastHeader(this.backoffTimestampHeader))
				.filter(timestampHeader -> {
					byte[] value = timestampHeader.value();
					return value != null
							&& value.length > 0
							&& value.length <= Long.BYTES;
				})
				.map(timestampHeader -> new BigInteger(timestampHeader.value()).longValue())
				.filter(ts -> isWithinMaxBackoff(data, ts));
	}

	private boolean isWithinMaxBackoff(ConsumerRecord<K, V> data, long backoffTimestamp) {
		long pauseMs = backoffTimestamp - Instant.now(this.clock).toEpochMilli();
		if (pauseMs <= this.maxBackoffMs) {
			return true;
		}
		this.logger.warn(() -> String.format("Ignoring the %s header of the record from %s-%d@%d: it requests a pause "
						+ "of %d ms, which exceeds the maximum of %d ms; the record is processed immediately",
				this.backoffTimestampHeader, data.topic(), data.partition(), data.offset(), pauseMs, this.maxBackoffMs));
		return false;
	}

	/*
	 * Since the container uses the delegate's type to determine which method to call, we
	 * must implement them all.
	 */

	@Override
	public void onMessage(ConsumerRecord<K, V> data) {
		onMessage(data, null, null); // NOSONAR
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, @Nullable Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null); // NOSONAR
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, @Nullable Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}
}
