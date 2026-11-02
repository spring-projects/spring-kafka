/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * The default {@link BatchToRecordAdapter} implementation; if the supplied recoverer
 * throws an exception, the batch will be aborted; otherwise the next record will be
 * processed.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.4.2
 */
public class DefaultBatchToRecordAdapter<K, V> implements BatchToRecordAdapter<K, V> {

	private static final LogAccessor LOGGER = new LogAccessor(DefaultBatchToRecordAdapter.class);

	private final ConsumerRecordRecoverer recoverer;

	/**
	 * Construct an instance with the default recoverer which simply logs the failed
	 * record.
	 */
	public DefaultBatchToRecordAdapter() {
		this((record, ex) -> LOGGER.error(ex, () -> "Failed to process " + record));
	}

	/**
	 * Construct an instance with the provided recoverer.
	 * @param recoverer the recoverer.
	 */
	public DefaultBatchToRecordAdapter(ConsumerRecordRecoverer recoverer) {
		Assert.notNull(recoverer, "'recoverer' cannot be null");
		this.recoverer = recoverer;
	}

	@Override
	public void adapt(List<Message<?>> messages, List<ConsumerRecord<K, V>> records, @Nullable Acknowledgment ack,
			@Nullable Consumer<?, ?> consumer, Callback<K, V> callback) {

		for (int i = 0; i < messages.size(); i++) {
			Message<?> message = messages.get(i);
			try {
				callback.invoke(records.get(i), ack, consumer, message);
			}
			catch (Exception e) {
				this.recoverer.accept(records.get(i), e);
			}
		}

	}

}
