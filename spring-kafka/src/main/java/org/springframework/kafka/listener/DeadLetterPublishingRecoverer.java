/*
 * Copyright 2018-2019 the original author or authors.
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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.util.Assert;

/**
 * A {@link BiConsumer} that publishes a failed record to a dead-letter topic.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class DeadLetterPublishingRecoverer implements BiConsumer<ConsumerRecord<?, ?>, Exception> {

	private static final Log logger = LogFactory.getLog(DeadLetterPublishingRecoverer.class); // NOSONAR

	private final KafkaTemplate<Object, Object> template;

	private final boolean transactional;

	private final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver;

	/**
	 * Create an instance with the provided template and a default destination resolving
	 * function that returns a TopicPartition based on the original topic (appended with ".DLT")
	 * from the failed record, and the same partition as the failed record. Therefore the
	 * dead-letter topic must have at least as many partitions as the original topic.
	 * @param template the {@link KafkaTemplate} to use for publishing.
	 */
	public DeadLetterPublishingRecoverer(KafkaTemplate<Object, Object> template) {
		this(template, (cr, e) -> new TopicPartition(cr.topic() + ".DLT", cr.partition()));
	}

	/**
	 * Create an instance with the provided template and destination resolving function,
	 * that receives the failed consumer record and the exception and returns a
	 * {@link TopicPartition}. If the partition in the {@link TopicPartition} is less than 0, no
	 * partition is set when publishing to the topic.
	 * @param template the {@link KafkaTemplate} to use for publishing.
	 * @param destinationResolver the resolving function.
	 */
	public DeadLetterPublishingRecoverer(KafkaTemplate<Object, Object> template,
			BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {

		Assert.notNull(template, "The template cannot be null");
		Assert.notNull(destinationResolver, "The destinationResolver cannot be null");
		this.template = template;
		this.transactional = template.isTransactional();
		this.destinationResolver = destinationResolver;
	}

	@Override
	public void accept(ConsumerRecord<?, ?> record, Exception exception) {
		TopicPartition tp = this.destinationResolver.apply(record, exception);
		RecordHeaders headers = new RecordHeaders(record.headers().toArray());
		enhanceHeaders(headers, record, exception);
		ProducerRecord<Object, Object> outRecord = createProducerRecord(record, tp, headers);
		if (this.transactional && !this.template.inTransaction()) {
			this.template.executeInTransaction(t -> {
				publish(outRecord, t);
				return null;
			});
		}
		else {
			publish(outRecord, this.template);
		}
	}

	/**
	 * Subclasses can override this method to customize the producer record to send to the DLQ.
	 * The default implementation simply copies the key and value from the consumer record
	 * and adds the headers. The timestamp is not set (the original timestamp is in one of
	 * the headers).
	 * IMPORTANT: if the partition in the {@link TopicPartition} is less than 0, it must be set to null
	 * in the {@link ProducerRecord}.
	 * @param record the failed record
	 * @param topicPartition the {@link TopicPartition} returned by the destination resolver.
	 * @param headers the headers - original record headers plus DLT headers.
	 * @return the producer record to send.
	 * @see KafkaHeaders
	 */
	protected ProducerRecord<Object, Object> createProducerRecord(ConsumerRecord<?, ?> record,
			TopicPartition topicPartition, RecordHeaders headers) {

		return new ProducerRecord<>(topicPartition.topic(),
				topicPartition.partition() < 0 ? null : topicPartition.partition(),
				record.key(), record.value(), headers);
	}

	/**
	 * Override this if you want more than just logging of the send result.
	 * @param outRecord the record to send.
	 * @param kafkaTemplate the template.
	 * @since 2.2.5
	 */
	protected void publish(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> kafkaTemplate) {
		try {
			kafkaTemplate.send(outRecord).addCallback(result -> {
				if (logger.isDebugEnabled()) {
					logger.debug("Successful dead-letter publication: " + result);
				}
			}, ex -> {
				logger.error("Dead-letter publication failed for: " + outRecord, ex);
			});
		}
		catch (Exception e) {
			logger.error("Dead-letter publication failed for: " + outRecord, e);
		}
	}

	private void enhanceHeaders(RecordHeaders kafkaHeaders, ConsumerRecord<?, ?> record, Exception exception) {
		kafkaHeaders.add(
				new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC, record.topic().getBytes(StandardCharsets.UTF_8)));
		kafkaHeaders.add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION,
				ByteBuffer.allocate(Integer.BYTES).putInt(record.partition()).array()));
		kafkaHeaders.add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET,
				ByteBuffer.allocate(Long.BYTES).putLong(record.offset()).array()));
		kafkaHeaders.add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP,
				ByteBuffer.allocate(Long.BYTES).putLong(record.timestamp()).array()));
		kafkaHeaders.add(new RecordHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE,
				record.timestampType().toString().getBytes(StandardCharsets.UTF_8)));
		kafkaHeaders.add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_FQCN,
				exception.getClass().getName().getBytes(StandardCharsets.UTF_8)));
		kafkaHeaders.add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE,
				exception.getMessage().getBytes(StandardCharsets.UTF_8)));
		kafkaHeaders.add(new RecordHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE,
				getStackTraceAsString(exception).getBytes(StandardCharsets.UTF_8)));
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

}
