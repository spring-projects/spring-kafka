/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

	private static final Log logger = LogFactory.getLog(DeadLetterPublishingRecoverer.class);

	private final KafkaTemplate<Object, Object> template;

	private final boolean transactional;

	private final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver;

	/**
	 * Create an instance with the provided template and a default destinatio resolving
	 * function that returns a TopicPartition based on the original topic (appended with ".DLT")
	 * from the failed record, and the same partition as the failed record.
	 * @param template the {@link KafkaTemplate} to use for publishing.
	 */
	public DeadLetterPublishingRecoverer(KafkaTemplate<Object, Object> template) {
		this(template, (cr, e) -> new TopicPartition(cr.topic() + ".DLT", cr.partition()));
	}

	/**
	 * Create an instance with the provided template and destination resolving function,
	 * that receives the failed consumer record and the exception and returns a
	 * {@link TopicPartition}. If the partition in the {@link TopicPartition} is < 0, no
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
		ProducerRecord<Object, Object> outRecord = new ProducerRecord<>(tp.topic(),
				tp.partition() < 0 ? null : tp.partition(), record.key(), record.value(), headers);
		if (this.transactional) {
			this.template.executeInTransaction(t -> {
				publish(outRecord, t);
				return null;
			});
		}
		else {
			publish(outRecord, this.template);
		}
	}

	private void publish(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> template) {
		template.send(outRecord).addCallback(result -> {
			if (logger.isDebugEnabled()) {
				logger.debug("Successful dead-letter publication: " + result);
			}
		}, ex -> {
			logger.error("Dead-letter publication failed for: " + outRecord, ex);
		});
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
