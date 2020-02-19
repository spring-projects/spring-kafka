/*
 * Copyright 2020 the original author or authors.
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

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

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
	public void adapt(List<Message<?>> messages, List<ConsumerRecord<K, V>> records, Acknowledgment ack,
			Consumer<?, ?> consumer, Callback<K, V> callback) {

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
