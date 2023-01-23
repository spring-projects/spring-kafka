/*
 * Copyright 2018-2023 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;

/**
 *
 * A {@link AcknowledgingConsumerAwareMessageListener} implementation that looks for a
 * backoff dueTimestamp header and invokes a {@link KafkaConsumerBackoffManager} instance
 * that will back off if necessary.
 *
 * @param <K> the record key type.
 * @param <V> the record value type.
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class KafkaBackoffAwareMessageListenerAdapter<K, V>
		extends AbstractDelegatingMessageListenerAdapter<MessageListener<K, V>>
		implements AcknowledgingConsumerAwareMessageListener<K, V> {

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

	private void invokeDelegateOnMessage(ConsumerRecord<K, V> consumerRecord, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
		switch (this.delegateType) {
			case ACKNOWLEDGING_CONSUMER_AWARE -> this.delegate.onMessage(consumerRecord, acknowledgment, consumer);
			case ACKNOWLEDGING -> this.delegate.onMessage(consumerRecord, acknowledgment);
			case CONSUMER_AWARE -> this.delegate.onMessage(consumerRecord, consumer);
			case SIMPLE -> this.delegate.onMessage(consumerRecord);
		}
	}

	private KafkaConsumerBackoffManager.Context createContext(ConsumerRecord<K, V> data, long nextExecutionTimestamp,
			Consumer<?, ?> consumer) {

		return this.kafkaConsumerBackoffManager.createContext(nextExecutionTimestamp, this.listenerId,
				new TopicPartition(data.topic(), data.partition()), consumer);
	}

	private Optional<Long> maybeGetBackoffTimestamp(ConsumerRecord<K, V> data) {
		return Optional
				.ofNullable(data.headers().lastHeader(this.backoffTimestampHeader))
				.map(timestampHeader -> new BigInteger(timestampHeader.value()).longValue());
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
	public void onMessage(ConsumerRecord<K, V> data, Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null); // NOSONAR
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}
}
