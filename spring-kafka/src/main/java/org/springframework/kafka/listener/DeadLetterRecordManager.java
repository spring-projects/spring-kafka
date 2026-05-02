/*
 * Copyright 2018-present the original author or authors.
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.util.Assert;

/**
 * Manage the creation of headers for records published to a DLT by a {@link DeadLetterPublishingRecoverer}
 * or a {@link org.springframework.kafka.streams.AbstractRecoveringExceptionHandler} implementation.
 *
 * @author Loïc Greffier
 * @since 4.1.0
 */
public class DeadLetterRecordManager {

	private static final BiFunction<ConsumerRecord<?, ?>, Exception, @Nullable Headers> DEFAULT_HEADERS_FUNCTION =
			(rec, ex) -> null;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private final EnumSet<DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd> whichHeaders = EnumSet.allOf(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.class);

	private DeadLetterPublishingRecoverer.@org.jspecify.annotations.Nullable HeaderNames headerNames;

	private boolean retainExceptionHeader;

	private BiFunction<ConsumerRecord<?, ?>, Exception, @Nullable Headers> headersFunction = DEFAULT_HEADERS_FUNCTION;

	private boolean appendOriginalHeaders = true;

	private boolean stripPreviousExceptionHeaders = true;

	private DeadLetterPublishingRecoverer.ExceptionHeadersCreator exceptionHeadersCreator = this::addExceptionInfoHeaders;

	private Supplier<DeadLetterPublishingRecoverer.HeaderNames> headerNamesSupplier = () -> DeadLetterPublishingRecoverer.HeaderNames.Builder
			.original()
			.offsetHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)
			.timestampHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)
			.timestampTypeHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)
			.topicHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)
			.partitionHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)
			.consumerGroupHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)
			.exception()
			.keyExceptionFqcn(KafkaHeaders.DLT_KEY_EXCEPTION_FQCN)
			.exceptionFqcn(KafkaHeaders.DLT_EXCEPTION_FQCN)
			.exceptionCauseFqcn(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
			.keyExceptionMessage(KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE)
			.exceptionMessage(KafkaHeaders.DLT_EXCEPTION_MESSAGE)
			.keyExceptionStacktrace(KafkaHeaders.DLT_KEY_EXCEPTION_STACKTRACE)
			.exceptionStacktrace(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)
			.build();

	public DeadLetterRecordManager() { }

	/**
	 * Enriches headers with deserialization exception information and metadata, then creates a producer record for dead letter publishing.
	 * @param record the source consumer record
	 * @param exception the exception that triggered the dead letter publishing
	 * @param topicPartition the target topic and partition
	 * @return a producer record
	 * @since 4.1.0
	 */
	public ProducerRecord<byte[], byte[]> enrichHeadersAndCreateProducerRecord(ConsumerRecord<byte[], byte[]> record, Exception exception, TopicPartition topicPartition) {
		DeserializationException vDeserEx = SerializationUtils.getExceptionFromHeader(record,
				KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, this.logger);
		DeserializationException kDeserEx = SerializationUtils.getExceptionFromHeader(record,
				KafkaUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, this.logger);
		Headers headers = new RecordHeaders(record.headers().toArray());
		addAndEnhanceHeaders(record, exception, vDeserEx, kDeserEx, headers);
		return new ProducerRecord<>(topicPartition.topic(),
				topicPartition.partition() < 0 ? null : topicPartition.partition(),
				record.key(),
				record.value(),
				headers);
	}

	/**
	 * Adds and enriches headers with deserialization exception information, original record metadata, and custom headers.
	 * @param record the consumer record
	 * @param exception the exception that triggered the dead letter publishing
	 * @param vDeserEx the value deserialization exception, if any occurred during deserialization
	 * @param kDeserEx the key deserialization exception, if any occurred during deserialization
	 * @param headers the headers to be enriched
	 */
	public void addAndEnhanceHeaders(ConsumerRecord<?, ?> record, Exception exception, @Nullable DeserializationException vDeserEx, @Nullable DeserializationException kDeserEx, Headers headers) {
		if (this.headerNames == null) {
			this.headerNames = this.headerNamesSupplier.get();
		}
		if (kDeserEx != null) {
			if (!this.retainExceptionHeader) {
				headers.remove(KafkaUtils.KEY_DESERIALIZER_EXCEPTION_HEADER);
			}
			this.exceptionHeadersCreator.create(headers, kDeserEx, true, this.headerNames);
		}
		if (vDeserEx != null) {
			if (!this.retainExceptionHeader) {
				headers.remove(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
			}
			this.exceptionHeadersCreator.create(headers, vDeserEx, false, this.headerNames);
		}
		if (kDeserEx == null && vDeserEx == null) {
			this.exceptionHeadersCreator.create(headers, exception, false, this.headerNames);
		}
		enhanceHeaders(headers, record, exception); // NOSONAR headers are never null
	}

	private void enhanceHeaders(Headers kafkaHeaders, ConsumerRecord<?, ?> record, Exception exception) {
		maybeAddOriginalHeaders(kafkaHeaders, record, exception);
		Headers headers = this.headersFunction.apply(record, exception);
		if (headers != null) {
			headers.forEach(header -> {
				if (header instanceof DeadLetterPublishingRecoverer.SingleRecordHeader) {
					kafkaHeaders.remove(header.key());
				}
				kafkaHeaders.add(header);
			});
		}
	}

	private void maybeAddOriginalHeaders(Headers kafkaHeaders, ConsumerRecord<?, ?> record, Exception ex) {
		maybeAddHeader(kafkaHeaders, Objects.requireNonNull(this.headerNames).getOriginal().topicHeader,
				() -> record.topic().getBytes(StandardCharsets.UTF_8), DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.TOPIC);
		maybeAddHeader(kafkaHeaders, this.headerNames.getOriginal().partitionHeader,
				() -> ByteBuffer.allocate(Integer.BYTES).putInt(record.partition()).array(),
				DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.PARTITION);
		maybeAddHeader(kafkaHeaders, this.headerNames.getOriginal().offsetHeader,
				() -> ByteBuffer.allocate(Long.BYTES).putLong(record.offset()).array(),
				DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.OFFSET);
		maybeAddHeader(kafkaHeaders, this.headerNames.getOriginal().timestampHeader,
				() -> ByteBuffer.allocate(Long.BYTES).putLong(record.timestamp()).array(), DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.TS);
		maybeAddHeader(kafkaHeaders, this.headerNames.getOriginal().timestampTypeHeader,
				() -> record.timestampType().toString().getBytes(StandardCharsets.UTF_8),
				DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.TS_TYPE);
		if (ex instanceof ListenerExecutionFailedException) {
			String consumerGroup = ((ListenerExecutionFailedException) ex).getGroupId();
			if (consumerGroup != null) {
				maybeAddHeader(kafkaHeaders, this.headerNames.getOriginal().consumerGroup,
						() -> consumerGroup.getBytes(StandardCharsets.UTF_8), DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.GROUP);
			}
		}
	}

	private void maybeAddHeader(Headers kafkaHeaders, String header, Supplier<byte[]> valueSupplier, DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd hta) {

		if (this.whichHeaders.contains(hta)
				&& (this.appendOriginalHeaders || kafkaHeaders.lastHeader(header) == null)) {
			kafkaHeaders.add(header, valueSupplier.get());
		}
	}

	private void addExceptionInfoHeaders(Headers kafkaHeaders, Exception exception, boolean isKey, DeadLetterPublishingRecoverer.HeaderNames names) {

		appendOrReplace(kafkaHeaders,
				isKey ? names.getExceptionInfo().keyExceptionFqcn : names.getExceptionInfo().exceptionFqcn,
				() -> exception.getClass().getName().getBytes(StandardCharsets.UTF_8),
				DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EXCEPTION);
		Exception cause = ErrorHandlingUtils.findRootCause(exception);
		if (cause != null) {
			appendOrReplace(kafkaHeaders,
					names.getExceptionInfo().exceptionCauseFqcn,
					() -> cause.getClass().getName().getBytes(StandardCharsets.UTF_8),
					DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_CAUSE);
		}
		String message = buildMessage(exception, cause);
		if (message != null) {
			appendOrReplace(kafkaHeaders,
					isKey ? names.getExceptionInfo().keyExceptionMessage : names.getExceptionInfo().exceptionMessage,
					() -> message.getBytes(StandardCharsets.UTF_8),
					DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_MSG);
		}
		appendOrReplace(kafkaHeaders,
				isKey ? names.getExceptionInfo().keyExceptionStacktrace : names.getExceptionInfo().exceptionStacktrace,
				() -> getStackTraceAsString(exception).getBytes(StandardCharsets.UTF_8),
				DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_STACKTRACE);
	}

	private void appendOrReplace(Headers headers, String header, Supplier<byte[]> valueSupplier, DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd hta) {

		if (this.whichHeaders.contains(hta)) {
			if (this.stripPreviousExceptionHeaders) {
				headers.remove(header);
			}
			headers.add(header, valueSupplier.get());
		}
	}

	private @Nullable String buildMessage(Exception exception, @Nullable Throwable cause) {
		String message = exception.getMessage();
		if (!exception.equals(cause)) {
			if (message != null) {
				message = message + "; ";
			}
			String causeMsg = Objects.requireNonNull(cause).getMessage();
			if (causeMsg != null) {
				if (message != null) {
					message = message + causeMsg;
				}
				else {
					message = causeMsg;
				}
			}
		}
		return message;
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

	/**
	 * Set a {@link Supplier} for {@link DeadLetterPublishingRecoverer.HeaderNames}.
	 * @param supplier the supplier.
	 * @since 4.1.0
	 */
	public void setHeaderNamesSupplier(Supplier<DeadLetterPublishingRecoverer.HeaderNames> supplier) {
		Assert.notNull(supplier, "'DeadLetterPublishingRecoverer.HeaderNames' supplier cannot be null");
		this.headerNamesSupplier = supplier;
	}

	/**
	 * Set to true to retain a Java serialized {@link DeserializationException} header. By
	 * default, such headers are removed from the published record, unless both key and
	 * value deserialization exceptions occur, in which case, the DLT_* headers are
	 * created from the value exception and the key exception header is retained.
	 * @param retainExceptionHeader true to retain the exception header.
	 * @since 4.1.0
	 */
	public void setRetainExceptionHeader(boolean retainExceptionHeader) {
		this.retainExceptionHeader = retainExceptionHeader;
	}

	/**
	 * Set a {@link DeadLetterPublishingRecoverer.ExceptionHeadersCreator} implementation to completely take over
	 * setting the exception headers in the output record. Disables all headers that are
	 * set by default.
	 * @param headersCreator the creator.
	 * @since 4.1.0
	 */
	public void setExceptionHeadersCreator(DeadLetterPublishingRecoverer.ExceptionHeadersCreator headersCreator) {
		Assert.notNull(headersCreator, "'headersCreator' cannot be null");
		this.exceptionHeadersCreator = headersCreator;
	}

	/**
	 * Set a function which will be called to obtain additional headers to add to the
	 * published record. If a {@link Header} returned is an instance of
	 * {@link DeadLetterPublishingRecoverer.SingleRecordHeader}, then that header will replace any existing header of
	 * that name, rather than being appended as a new value.
	 * @param headersFunction the headers function.
	 * @since 4.1.0
	 */
	public void setHeadersFunction(BiFunction<ConsumerRecord<?, ?>, Exception, @Nullable Headers> headersFunction) {
		Assert.notNull(headersFunction, "'headersFunction' cannot be null");
		if (!this.headersFunction.equals(DEFAULT_HEADERS_FUNCTION)) {
			this.logger.warn(() -> "Replacing custom headers function: " + this.headersFunction
					+ ", consider using addHeadersFunction() if you need multiple functions");
		}
		this.headersFunction = headersFunction;
	}

	/**
	 * Set to false if you don't want to append the current "original" headers (topic,
	 * partition, etc.) if they are already present. When false, only the first "original"
	 * headers are retained.
	 * @param appendOriginalHeaders set to false not to replace.
	 * @since 4.1.0
	 */
	public void setAppendOriginalHeaders(boolean appendOriginalHeaders) {
		this.appendOriginalHeaders = appendOriginalHeaders;
	}

	/**
	 * Set to {@code false} to retain previous exception headers as well as headers for the
	 * current exception. Default is true, which means only the current headers are
	 * retained; setting it to false this can cause a growth in record size when a record
	 * is republished many times.
	 * @param stripPreviousExceptionHeaders false to retain all.
	 * @since 4.1.0
	 */
	public void setStripPreviousExceptionHeaders(boolean stripPreviousExceptionHeaders) {
		this.stripPreviousExceptionHeaders = stripPreviousExceptionHeaders;
	}

	/**
	 * Is the headers function the default one.
	 * @return true if it is, false otherwise.
	 * @since 4.1.0
	 */
	public boolean isDefaultHeadersFunction() {
		return this.headersFunction.equals(DEFAULT_HEADERS_FUNCTION);
	}

	/**
	 * Return the headers function.
	 * @return the headers function
	 * @since 4.1.0
	 */
	public BiFunction<ConsumerRecord<?, ?>, Exception, @Nullable Headers> getHeadersFunction() {
		return this.headersFunction;
	}

	/**
	 * Remove a header from the set of headers to be added to the published record.
	 * @param header the header to exclude
	 * @since 4.1.0
	 */
	public void removeHeaderToAdd(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd header) {
		this.whichHeaders.remove(header);
	}

	/**
	 * Add all headers to be added to the published record.
	 * @param headers the headers to include
	 * @since 4.1.0
	 */
	public void addAllToHeaders(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd... headers) {
		Collections.addAll(this.whichHeaders, headers);
	}
}
