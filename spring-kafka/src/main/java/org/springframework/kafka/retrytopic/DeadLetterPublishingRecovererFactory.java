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

package org.springframework.kafka.retrytopic;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiFunction;
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
import org.springframework.util.Assert;

/**
 *
 * Creates and configures the {@link DeadLetterPublishingRecoverer} that will be used to
 * forward the messages using the {@link DestinationTopicResolver}.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class DeadLetterPublishingRecovererFactory {

	private static final LogAccessor LOGGER =
			new LogAccessor(LogFactory.getLog(DeadLetterPublishingRecovererFactory.class));

	private final DestinationTopicResolver destinationTopicResolver;

	private final Set<Class<? extends Exception>> fatalExceptions = new LinkedHashSet<>();

	private final Set<Class<? extends Exception>> nonFatalExceptions = new HashSet<>();

	private Consumer<DeadLetterPublishingRecoverer> recovererCustomizer = recoverer -> { };

	private BiFunction<ConsumerRecord<?, ?>, Exception, Headers> headersFunction;

	private ListenerExceptionLoggingStrategy loggingStrategy = ListenerExceptionLoggingStrategy.AFTER_RETRIES_EXHAUSTED;

	private BiFunction<ConsumerRecord<?, ?>, String, Integer> partitionResolver = (cr, nextTopic) -> cr.partition();

	public DeadLetterPublishingRecovererFactory(DestinationTopicResolver destinationTopicResolver) {
		this.destinationTopicResolver = destinationTopicResolver;
	}

	/**
	 * Set a function that creates additional headers for the output record, in addition to the standard
	 * retry headers added by this factory.
	 * @param headersFunction the function.
	 * @since 2.8.4
	 */
	public void setHeadersFunction(BiFunction<ConsumerRecord<?, ?>, Exception, Headers> headersFunction) {
		this.headersFunction = headersFunction;
	}

	/**
	 * Set a resolver for the partition number to publish to. By default the same partition as
	 * the consumer record is used. If the resolver returns {@code null} or a negative number, the
	 * partition is set to null in the producer record and the {@code KafkaProducer} decides which
	 * partition to publish to.
	 * @param resolver the resolver.
	 * @since 2.9.2
	 */
	public void setPartitionResolver(BiFunction<ConsumerRecord<?, ?>, String, Integer> resolver) {
		Assert.notNull(resolver, "'resolver' cannot be null");
		this.partitionResolver = resolver;
	}

	/**
	 * Add exception type to the default list. By default, the following exceptions will
	 * not be retried:
	 * <ul>
	 * <li>{@link org.springframework.kafka.support.serializer.DeserializationException}</li>
	 * <li>{@link org.springframework.messaging.converter.MessageConversionException}</li>
	 * <li>{@link org.springframework.kafka.support.converter.ConversionException}</li>
	 * <li>{@link org.springframework.messaging.handler.invocation.MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * @param exceptionType the exception type.
	 * @since 2.8
	 * @see #removeNotRetryableException(Class)
	 */
	public final void addNotRetryableException(Class<? extends Exception> exceptionType) {
		Assert.notNull(exceptionType, "'exceptionType' cannot be null");
		this.fatalExceptions.add(exceptionType);
	}

	/**
	 * Remove an exception type from the configured list. By default, the following
	 * exceptions will not be retried:
	 * <ul>
	 * <li>{@link org.springframework.kafka.support.serializer.DeserializationException}</li>
	 * <li>{@link org.springframework.messaging.converter.MessageConversionException}</li>
	 * <li>{@link org.springframework.kafka.support.converter.ConversionException}</li>
	 * <li>{@link org.springframework.messaging.handler.invocation.MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * @param exceptionType the exception type.
	 * @return true if the removal was successful.
	 * @see #addNotRetryableException(Class)
	 */
	public boolean removeNotRetryableException(Class<? extends Exception> exceptionType) {
		return this.nonFatalExceptions.add(exceptionType);
	}

	/**
	 * Never logs the listener exception.
	 * The default is logging only after retries are exhausted.
	 * @since 2.7.13
	 */
	public void neverLogListenerException() {
		this.loggingStrategy = ListenerExceptionLoggingStrategy.NEVER;
	}

	/**
	 * Logs the listener exception at each attempt.
	 * The default is logging only after retries are exhausted.
	 * @since 2.7.13
	 */
	public void alwaysLogListenerException() {
		this.loggingStrategy = ListenerExceptionLoggingStrategy.EACH_ATTEMPT;
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
							.consumerGroupHeader(KafkaHeaders.ORIGINAL_CONSUMER_GROUP)
						.exception()
							.keyExceptionFqcn(KafkaHeaders.KEY_EXCEPTION_FQCN)
							.exceptionFqcn(KafkaHeaders.EXCEPTION_FQCN)
							.exceptionCauseFqcn(KafkaHeaders.EXCEPTION_CAUSE_FQCN)
							.keyExceptionMessage(KafkaHeaders.KEY_EXCEPTION_MESSAGE)
							.exceptionMessage(KafkaHeaders.EXCEPTION_MESSAGE)
							.keyExceptionStacktrace(KafkaHeaders.KEY_EXCEPTION_STACKTRACE)
							.exceptionStacktrace(KafkaHeaders.EXCEPTION_STACKTRACE)
						.build();
			}
		};

		recoverer.setHeadersFunction((consumerRecord, e) -> addHeaders(consumerRecord, e, getAttempts(consumerRecord)));
		if (this.headersFunction != null) {
			recoverer.addHeadersFunction(this.headersFunction);
		}
		recoverer.setFailIfSendResultIsError(true);
		recoverer.setAppendOriginalHeaders(false);
		recoverer.setThrowIfNoDestinationReturned(false);
		recoverer.setSkipSameTopicFatalExceptions(false);
		this.recovererCustomizer.accept(recoverer);
		this.fatalExceptions.forEach(recoverer::addNotRetryableExceptions);
		this.nonFatalExceptions.forEach(recoverer::removeClassification);
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

		maybeLogListenerException(e, cr, nextDestination);

		return nextDestination.isNoOpsTopic()
					? null
					: resolveTopicPartition(cr, nextDestination);
	}

	private void maybeLogListenerException(Exception e, ConsumerRecord<?, ?> cr, DestinationTopic nextDestination) {
		if (nextDestination.isDltTopic()
				&& !ListenerExceptionLoggingStrategy.NEVER.equals(this.loggingStrategy)) {
			LOGGER.error(e, () -> getErrorMessage(cr) + " and won't be retried. "
					+ "Sending to DLT with name " + nextDestination.getDestinationName() + ".");
		}
		else if (nextDestination.isNoOpsTopic()
				&& !ListenerExceptionLoggingStrategy.NEVER.equals(this.loggingStrategy)) {
			LOGGER.error(e, () -> getErrorMessage(cr) + " and won't be retried. "
					+ "No further action will be taken with this record.");
		}
		else if (ListenerExceptionLoggingStrategy.EACH_ATTEMPT.equals(this.loggingStrategy)) {
			LOGGER.error(e, () -> getErrorMessage(cr) + ". "
					+ "Sending to retry topic " + nextDestination.getDestinationName() + ".");
		}
		else {
			LOGGER.debug(e, () -> getErrorMessage(cr) + ". "
					+ "Sending to retry topic " + nextDestination.getDestinationName() + ".");
		}
	}

	private static String getErrorMessage(ConsumerRecord<?, ?> cr) {
		return "Record: " + getRecordInfo(cr) + " threw an error at topic " + cr.topic();
	}

	private static String getRecordInfo(ConsumerRecord<?, ?> cr) {
		Header originalTopicHeader = cr.headers().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		return String.format("topic = %s, partition = %s, offset = %s, main topic = %s",
				cr.topic(), cr.partition(), cr.offset(),
				originalTopicHeader != null ? new String(originalTopicHeader.value()) : cr.topic());
	}

	/**
	 * Creates and returns the {@link TopicPartition}, where the original record should be forwarded.
	 * By default, it will use the partition same as original record's partition, in the next destination topic.
	 *
	 * <p>{@link DeadLetterPublishingRecoverer#checkPartition} has logic to check whether that partition exists,
	 * and if it doesn't it sets -1, to allow the Producer itself to assign a partition to the record.</p>
	 *
	 * <p>Subclasses can inherit from this method to override the implementation, if necessary.</p>
	 * The destination partition can also be customized using {@link #setPartitionResolver(BiFunction)}.
	 *
	 * @param cr The original {@link ConsumerRecord}, which is to be forwarded to DLT
	 * @param nextDestination The next {@link DestinationTopic}, where the consumerRecord is to be forwarded
	 * @return An instance of {@link TopicPartition}, specifying the topic and partition, where the cr is to be sent.
	 * @see #setPartitionResolver(BiFunction)
	 */
	protected TopicPartition resolveTopicPartition(final ConsumerRecord<?, ?> cr, final DestinationTopic nextDestination) {
		String nextTopic = nextDestination.getDestinationName();
		Integer partition = this.partitionResolver.apply(cr, nextTopic);
		return new TopicPartition(nextTopic, partition == null ? -1 : partition);
	}

	private int getAttempts(ConsumerRecord<?, ?> consumerRecord) {
		Header header = consumerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		if (header != null) {
			byte[] value = header.value();
			if (value.length == Byte.BYTES) { // backwards compatibility
				return value[0];
			}
			else if (value.length == Integer.BYTES) {
				return ByteBuffer.wrap(value).getInt();
			}
			else {
				LOGGER.debug(() -> "Unexected size for " + RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS + " header: "
						+ value.length);
			}
		}
		return 1;
	}

	private Headers addHeaders(ConsumerRecord<?, ?> consumerRecord, Exception e, int attempts) {
		Headers headers = new RecordHeaders();
		byte[] originalTimestampHeader = getOriginalTimestampHeaderBytes(consumerRecord);
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampHeader);
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
				ByteBuffer.wrap(new byte[Integer.BYTES]).putInt(attempts + 1).array());
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

	private enum ListenerExceptionLoggingStrategy {

		/**
		 * Never log the listener exception.
		 */
		NEVER,

		/**
		 * Log the listener exception after each attempt.
		 */
		EACH_ATTEMPT,

		/**
		 * Log the listener only after retries are exhausted.
		 */
		AFTER_RETRIES_EXHAUSTED

	}
}
