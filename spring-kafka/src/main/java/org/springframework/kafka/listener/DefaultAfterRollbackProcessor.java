/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SeekUtils;
import org.springframework.lang.Nullable;

/**
 * Default implementation of {@link AfterRollbackProcessor}. Seeks all
 * topic/partitions so the records will be re-fetched, including the failed
 * record. Starting with version 2.2 after a configurable number of failures
 * for the same topic/partition/offset, that record will be skipped after
 * calling a {@link BiConsumer} recoverer. The default recoverer simply logs
 * the failed record.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 1.3.5
 *
 */
public class DefaultAfterRollbackProcessor<K, V> implements AfterRollbackProcessor<K, V> {

	private static final Log logger = LogFactory.getLog(DefaultAfterRollbackProcessor.class); // NOSONAR

	private final FailedRecordTracker failureTracker;

	private boolean processInTransaction;

	private KafkaTemplate<K, V> kafkaTemplate;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor() {
		this(null, SeekUtils.DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * 'maxFailures' have occurred for a topic/partition/offset.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @since 2.2.1
	 */
	public DefaultAfterRollbackProcessor(int maxFailures) {
		this(null, maxFailures);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
		this(recoverer, SeekUtils.DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * maxFailures have occurred for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
			int maxFailures) {
		this.failureTracker = new FailedRecordTracker(recoverer, maxFailures, logger);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer, Exception exception,
			boolean recoverable) {

		if (SeekUtils.doSeeks(((List) records), consumer, exception, recoverable, this.failureTracker::skip, logger)
				&& this.kafkaTemplate != null && this.kafkaTemplate.isTransactional()) {
			ConsumerRecord<K, V> skipped = records.get(0);
			this.kafkaTemplate.sendOffsetsToTransaction(
					Collections.singletonMap(new TopicPartition(skipped.topic(), skipped.partition()),
							new OffsetAndMetadata(skipped.offset() + 1)));
		}
	}

	@Override
	public boolean isProcessInTransaction() {
		return this.processInTransaction;
	}

	/**
	 * Set to true to run the {@link #process(List, Consumer, Exception, boolean)}
	 * method in a transaction. Requires a {@link KafkaTemplate}.
	 * @param processInTransaction true to process in a transaction.
	 * @since 2.2.5
	 * @see #process(List, Consumer, Exception, boolean)
	 * @see #setKafkaTemplate(KafkaTemplate)
	 */
	public void setProcessInTransaction(boolean processInTransaction) {
		this.processInTransaction = processInTransaction;
	}

	/**
	 * Set a {@link KafkaTemplate} to use to send the offset of a recovered record
	 * to a transaction.
	 * @param kafkaTemplate the template
	 * @since 2.2.5
	 * @see #setProcessInTransaction(boolean)
	 */
	public void setKafkaTemplate(KafkaTemplate<K, V> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public void clearThreadState() {
		this.failureTracker.clearThreadState();
	}

}
