/*
 * Copyright 2018-2020 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.EOSMode;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;

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
public class DefaultAfterRollbackProcessor<K, V> extends FailedRecordProcessor
		implements AfterRollbackProcessor<K, V>, InitializingBean {

	private KafkaOperations<?, ?> kafkaTemplate;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor() {
		this(null, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param backOff the {@link BackOff}.
	 * @since 2.3
	 */
	public DefaultAfterRollbackProcessor(BackOff backOff) {
		this(null, backOff);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 * @since 2.2
	 */
	public DefaultAfterRollbackProcessor(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
		this(recoverer, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param backOff the {@link BackOff}.
	 * @since 2.3
	 */
	public DefaultAfterRollbackProcessor(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
			BackOff backOff) {

		this(recoverer, backOff, null, false);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after the
	 * backOff returns STOP for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param backOff the {@link BackOff}.
	 * @param kafkaOperations for sending the recovered offset to the transaction.
	 * @param commitRecovered true to commit the recovered record's offset; requires a
	 * {@link KafkaOperations}.
	 * @since 2.5.3
	 */
	public DefaultAfterRollbackProcessor(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
			BackOff backOff, @Nullable KafkaOperations<?, ?> kafkaOperations, boolean commitRecovered) {

		super(recoverer, backOff);
		this.kafkaTemplate = kafkaOperations;
		super.setCommitRecovered(commitRecovered);
		checkConfig();
	}

	@Override
	public void afterPropertiesSet() {
		// remove InitializingBean when the deprecated setters are removed.
		checkConfig();
	}

	private void checkConfig() {
		Assert.isTrue(!isCommitRecovered() || this.kafkaTemplate != null,
				"A KafkaOperations is required when 'commitRecovered' is true");
	}

	@Override
	@Deprecated
	public void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer, Exception exception,
			boolean recoverable) {

		process(records, consumer, exception, recoverable, EOSMode.ALPHA);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer, Exception exception,
			boolean recoverable, @Nullable EOSMode eosMode) {

		if (SeekUtils.doSeeks(((List) records), consumer, exception, recoverable,
				getSkipPredicate((List) records, exception), this.logger)
					&& isCommitRecovered() && this.kafkaTemplate.isTransactional()) {
			ConsumerRecord<K, V> skipped = records.get(0);
			if (EOSMode.ALPHA.equals(eosMode)) {
				this.kafkaTemplate.sendOffsetsToTransaction(
						Collections.singletonMap(new TopicPartition(skipped.topic(), skipped.partition()),
								new OffsetAndMetadata(skipped.offset() + 1)));
			}
			else {
				this.kafkaTemplate.sendOffsetsToTransaction(
						Collections.singletonMap(new TopicPartition(skipped.topic(), skipped.partition()),
								new OffsetAndMetadata(skipped.offset() + 1)), consumer.groupMetadata());
			}
		}
	}

	@Override
	public boolean isProcessInTransaction() {
		return isCommitRecovered();
	}

	/**
	 * {@inheritDoc} Set to true and the container will run the {@link #process(List,
	 * Consumer, Exception, boolean, EOSMode)} method in a transaction and, if a record
	 * is skipped and recovered, we will send its offset to the transaction. Requires a
	 * {@link KafkaOperations}.
	 * @param commitRecovered true to process in a transaction.
	 * @since 2.3
	 * @deprecated in favor of
	 * {@link #DefaultAfterRollbackProcessor(BiConsumer, BackOff, KafkaOperations, boolean)}.
	 * @see #isProcessInTransaction()
	 * @see #process(List, Consumer, Exception, boolean, EOSMode)
	 */
	@Deprecated
	@Override
	public void setCommitRecovered(boolean commitRecovered) { // NOSONAR enhanced javadoc
		super.setCommitRecovered(commitRecovered);
	}

	/**
	 * Set a {@link KafkaTemplate} to use to send the offset of a recovered record to a
	 * transaction.
	 * @param kafkaTemplate the template.
	 * @since 2.2.5
	 * @deprecated in favor of
	 * {@link #DefaultAfterRollbackProcessor(BiConsumer, BackOff, KafkaOperations,
	 * boolean)}.
	 * @see #setCommitRecovered(boolean)
	 */
	@Deprecated
	public void setKafkaTemplate(KafkaTemplate<K, V> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	/**
	 * Set a {@link KafkaOperations} to use to send the offset of a recovered record to a
	 * transaction.
	 * @param kafkaOperations the operations.
	 * @since 2.5.1
	 * @deprecated in favor of
	 * {@link #DefaultAfterRollbackProcessor(BiConsumer, BackOff, KafkaOperations,
	 * boolean)}.
	 * @see #setCommitRecovered(boolean)
	 */
	@Deprecated
	public void setKafkaOperations(KafkaOperations<K, V> kafkaOperations) {
		this.kafkaTemplate = kafkaOperations;
	}

}
