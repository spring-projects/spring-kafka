/*
 * Copyright 2018-2021 the original author or authors.
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
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.ListenerType;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;


/**
 *
 * An {@link AcknowledgingConsumerAwareMessageListener} implementation that looks for a backoff timestamp header
 * and passes to a {@link KafkaConsumerBackoffManager} instance that will backoff if necessary.
 *
 * @param <K> the record key type.
 * @param <V> the record value type.
 *
 * @author Tomaz Fernandes
 *
 * @since 2.7
 *
 */
public class KafkaBackoffAwareMessageListenerAdapter<K, V> extends AbstractDelegatingMessageListenerAdapter<AcknowledgingConsumerAwareMessageListener<K, V>> implements AcknowledgingConsumerAwareMessageListener<K, V> {

	private final String listenerId;

	private final String backoffTimestampHeader;

	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	public KafkaBackoffAwareMessageListenerAdapter(AcknowledgingConsumerAwareMessageListener<K, V> delegate, KafkaConsumerBackoffManager kafkaConsumerBackoffManager, String listenerId, String backoffTimestampHeader) {
		super(delegate);
		this.listenerId = listenerId;
		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.backoffTimestampHeader = backoffTimestampHeader;
	}

	public KafkaBackoffAwareMessageListenerAdapter(AcknowledgingConsumerAwareMessageListener<K, V> adapter, KafkaConsumerBackoffManager kafkaConsumerBackoffManager, String listenerId) {
		this(adapter, kafkaConsumerBackoffManager, listenerId, RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP);
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data) {
		throw new UnsupportedOperationException(String.format("%s requires %s or %s listener types.", this.getClass().getSimpleName(), ListenerType.ACKNOWLEDGING, ListenerType.ACKNOWLEDGING_CONSUMER_AWARE));
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Acknowledgment ack, Consumer<?, ?> consumer) throws KafkaBackoffException {
		maybeGetBackoffTimestamp(data)
				.ifPresent(nextExecutionTimestamp -> this.kafkaConsumerBackoffManager.maybeBackoff(createContext(data, nextExecutionTimestamp)));
		super.getDelegate().onMessage(data, ack, consumer);
		ack.acknowledge();
	}

	private KafkaConsumerBackoffManager.Context createContext(ConsumerRecord<K, V> data, long nextExecutionTimestamp) {
		return this.kafkaConsumerBackoffManager.createContext(nextExecutionTimestamp, this.listenerId, new TopicPartition(data.topic(), data.partition()));
	}

	private <V, K> Optional<Long> maybeGetBackoffTimestamp(ConsumerRecord<K, V> data) {
		return Optional
				.ofNullable(data.headers().lastHeader(this.backoffTimestampHeader))
				.map(timestampHeader -> new BigInteger(timestampHeader.value()).longValue());
	}
}
