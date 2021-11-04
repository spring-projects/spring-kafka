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
import java.time.Instant;
import java.util.function.Consumer;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.core.NestedRuntimeException;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekUtils;
import org.springframework.kafka.listener.TimestampedException;
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

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(DeadLetterPublishingRecovererFactory.class));

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
		recoverer.setAppendOriginalHeaders(false);
		recoverer.setThrowIfNoDestinationReturned(false);
		this.recovererCustomizer.accept(recoverer);
		return recoverer;
	}

	private KafkaOperations<?, ?> resolveTemplate(ProducerRecord<?, ?> outRecord) {
		return this.destinationTopicResolver
						.getDestinationTopicByName(outRecord.topic())
						.getKafkaOperations();
	}

	public void setDeadLetterPublishingRecovererCustomizer(Consumer<DeadLetterPublishingRecoverer> customizer) {
		this.recovererCustomizer = customizer;
	}

	private TopicPartition resolveDestination(ConsumerRecord<?, ?> cr, Exception e) {
		if (SeekUtils.isBackoffException(e)) {
			throw (NestedRuntimeException) e; // Necessary to not commit the offset and seek to current again
		}

		DestinationTopic nextDestination = this.destinationTopicResolver.resolveDestinationTopic(
				cr.topic(), getAttempts(cr), e, getOriginalTimestampHeaderLong(cr));

		LOGGER.debug(() -> "Resolved topic: " + (nextDestination.isNoOpsTopic()
				? "none"
				: nextDestination.getDestinationName()));

		return nextDestination.isNoOpsTopic()
					? null
					: resolveTopicPartition(cr, nextDestination);
	}

	/**
	 * Creates and returns the {@link TopicPartition}, where the original record should be forwarded.
	 * By default, it will use the partition same as original record's partition, in the next destination topic.
	 *
	 * <p>{@link DeadLetterPublishingRecoverer#checkPartition} has logic to check whether that partition exists,
	 * and if it doesn't it sets -1, to allow the Producer itself to assign a partition to the record.</p>
	 *
	 * <p>Subclasses can inherit from this method to override the implementation, if necessary.</p>
	 *
	 * @param cr The original {@link ConsumerRecord}, which is to be forwarded to DLT
	 * @param nextDestination The next {@link DestinationTopic}, where the consumerRecord is to be forwarded
	 * @return An instance of {@link TopicPartition}, specifying the topic and partition, where the cr is to be sent
	 */
	protected TopicPartition resolveTopicPartition(final ConsumerRecord<?, ?> cr, final DestinationTopic nextDestination) {
		return new TopicPartition(nextDestination.getDestinationName(), cr.partition());
	}

	private int getAttempts(ConsumerRecord<?, ?> consumerRecord) {
		Header header = consumerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		return header != null
				? header.value()[0]
				: 1;
	}

	private Headers addHeaders(ConsumerRecord<?, ?> consumerRecord, Exception e, int attempts) {
		Headers headers = new RecordHeaders();
		byte[] originalTimestampHeader = getOriginalTimestampHeaderBytes(consumerRecord);
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampHeader);
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
				BigInteger.valueOf(attempts + 1).toByteArray());
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP,
				BigInteger.valueOf(getNextExecutionTimestamp(consumerRecord, e, originalTimestampHeader))
						.toByteArray());
		return headers;
	}

	private long getNextExecutionTimestamp(ConsumerRecord<?, ?> consumerRecord, Exception e,
			byte[] originalTimestampHeader) {

		long originalTimestamp = new BigInteger(originalTimestampHeader).longValue();
		long failureTimestamp = getFailureTimestamp(e);
		long nextExecutionTimestamp =  failureTimestamp + this.destinationTopicResolver
				.resolveDestinationTopic(consumerRecord.topic(), getAttempts(consumerRecord), e, originalTimestamp)
				.getDestinationDelay();
		LOGGER.debug(() -> String.format("FailureTimestamp: %s, Original timestamp: %s, nextExecutionTimestamp: %s",
				failureTimestamp, originalTimestamp, nextExecutionTimestamp));
		return nextExecutionTimestamp;
	}

	private long getFailureTimestamp(Exception e) {
		return e instanceof NestedRuntimeException && ((NestedRuntimeException) e).contains(TimestampedException.class)
					? getTimestampedException(e).getTimestamp()
					: Instant.now().toEpochMilli();
	}

	private TimestampedException getTimestampedException(Throwable e) {
		if (e == null) {
			throw new IllegalArgumentException("Provided exception does not contain a "
					+ TimestampedException.class.getSimpleName() + " cause.");
		}
		return e.getClass().isAssignableFrom(TimestampedException.class)
				? (TimestampedException) e
				: getTimestampedException(e.getCause());
	}

	private byte[] getOriginalTimestampHeaderBytes(ConsumerRecord<?, ?> consumerRecord) {
		Header currentOriginalTimestampHeader = getOriginaTimeStampHeader(consumerRecord);
		return currentOriginalTimestampHeader != null
				? currentOriginalTimestampHeader.value()
				: BigInteger.valueOf(consumerRecord.timestamp()).toByteArray();
	}

	private long getOriginalTimestampHeaderLong(ConsumerRecord<?, ?> consumerRecord) {
		Header currentOriginalTimestampHeader = getOriginaTimeStampHeader(consumerRecord);
		return currentOriginalTimestampHeader != null
				? new BigInteger(currentOriginalTimestampHeader.value()).longValue()
				: consumerRecord.timestamp();
	}

	private Header getOriginaTimeStampHeader(ConsumerRecord<?, ?> consumerRecord) {
		return consumerRecord.headers()
					.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP);
	}
}


