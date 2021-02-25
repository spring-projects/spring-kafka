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

package org.springframework.kafka.retrytopic;

import java.math.BigInteger;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.core.NestedRuntimeException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekUtils;
import org.springframework.kafka.support.KafkaHeaders;

/**
 *
 * Creates and configures the {@link DeadLetterPublishingRecoverer} that will be used to
 * forward the messages using the {@link DestinationTopicResolver}.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class DeadLetterPublishingRecovererFactory {

	private final DestinationTopicResolver destinationTopicResolver;

	private Consumer<DeadLetterPublishingRecoverer> recovererCustomizer = recoverer -> { };

	public DeadLetterPublishingRecovererFactory(DestinationTopicResolver destinationTopicResolver) {
		this.destinationTopicResolver = destinationTopicResolver;
	}

	@SuppressWarnings("unchecked")
	public DeadLetterPublishingRecoverer create() {
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(// NOSONAR anon. class size
				this::resolveTemplate,
				false, (this::resolveDestination)) {

			@Override
			protected DeadLetterPublishingRecoverer.HeaderNames getHeaderNames() {
				return DeadLetterPublishingRecoverer.HeaderNames.Builder
						.original()
							.offsetHeader(KafkaHeaders.ORIGINAL_OFFSET)
							.timestampHeader(KafkaHeaders.ORIGINAL_TIMESTAMP)
							.timestampTypeHeader(KafkaHeaders.ORIGINAL_TIMESTAMP_TYPE)
							.topicHeader(KafkaHeaders.ORIGINAL_TOPIC)
							.partitionHeader(KafkaHeaders.ORIGINAL_PARTITION)
						.exception()
							.keyExceptionFqcn(KafkaHeaders.KEY_EXCEPTION_FQCN)
							.exceptionFqcn(KafkaHeaders.EXCEPTION_FQCN)
							.keyExceptionMessage(KafkaHeaders.KEY_EXCEPTION_MESSAGE)
							.exceptionMessage(KafkaHeaders.EXCEPTION_MESSAGE)
							.keyExceptionStacktrace(KafkaHeaders.KEY_EXCEPTION_STACKTRACE)
							.exceptionStacktrace(KafkaHeaders.EXCEPTION_STACKTRACE)
						.build();
			}
		};

		recoverer.setHeadersFunction((consumerRecord, e) -> addHeaders(consumerRecord, e, getAttempts(consumerRecord)));
		recoverer.setFailIfSendResultIsError(true);
		recoverer.setReplaceOriginalHeaders(false);
		recoverer.setThrowIfNoDestinationReturned(false);
		this.recovererCustomizer.accept(recoverer);
		return recoverer;
	}

	private KafkaOperations<?, ?> resolveTemplate(ProducerRecord<?, ?> outRecord) {
		return this.destinationTopicResolver
						.getCurrentTopic(outRecord.topic())
						.getKafkaOperations();
	}

	public void setDeadLetterPublishingRecovererCustomizer(Consumer<DeadLetterPublishingRecoverer> customizer) {
		this.recovererCustomizer = customizer;
	}

	private TopicPartition resolveDestination(ConsumerRecord<?, ?> cr, Exception e) {
		if (SeekUtils.isBackoffException(e)) {
			throw (NestedRuntimeException) e; // Necessary to not commit the offset and seek to current again
		}

		int attempt = getAttempts(cr);
		BigInteger originalTimestamp = new BigInteger(getOriginalTimestampHeader(cr));

		DestinationTopic nextDestination = this.destinationTopicResolver.resolveNextDestination(
				cr.topic(), attempt, e, originalTimestamp.longValue());

		return nextDestination.isNoOpsTopic()
					? null
					: new TopicPartition(nextDestination.getDestinationName(),
				cr.partition() % nextDestination.getDestinationPartitions());
	}

	private int getAttempts(ConsumerRecord<?, ?> consumerRecord) {
		Header header = consumerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		return header != null
				? header.value()[0]
				: 1;
	}

	private Headers addHeaders(ConsumerRecord<?, ?> consumerRecord, Exception e, int attempts) {

		Headers headers = new RecordHeaders();

		byte[] originalTimestampHeader = getOriginalTimestampHeader(consumerRecord);
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampHeader);

		headers.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
				BigInteger.valueOf(attempts + 1).toByteArray());
		long originalTimestamp = new BigInteger(originalTimestampHeader).longValue();

		long nextExecutionTimestamp = this.destinationTopicResolver
				.resolveDestinationNextExecutionTimestamp(consumerRecord.topic(), attempts, e,
						originalTimestamp);

		headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP,	BigInteger.valueOf(nextExecutionTimestamp).toByteArray());
		return headers;
	}

	private byte[] getOriginalTimestampHeader(ConsumerRecord<?, ?> consumerRecord) {
		Header currentOriginalTimestampHeader = consumerRecord.headers()
				.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP);
		return currentOriginalTimestampHeader != null
				? currentOriginalTimestampHeader.value()
				: BigInteger.valueOf(consumerRecord.timestamp()).toByteArray();
	}
}


