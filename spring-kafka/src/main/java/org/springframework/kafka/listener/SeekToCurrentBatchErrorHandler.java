/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.kafka.KafkaException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * An error handler that seeks to the current offset for each topic in a batch of records.
 * Used to rewind partitions after a message failure so that the batch can be replayed.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
public class SeekToCurrentBatchErrorHandler extends KafkaExceptionLogLevelAware
		implements ContainerAwareBatchErrorHandler {

	private final ThreadLocal<BackOffExecution> backOffs = new ThreadLocal<>(); // Intentionally not static

	private final ThreadLocal<Long> lastIntervals = new ThreadLocal<>(); // Intentionally not static

	private BackOff backOff;

	/**
	 * Set a {@link BackOff} to suspend the thread after performing the seek. Since this
	 * error handler can never "recover" after retries are exhausted, if the back off
	 * returns STOP, then the previous interval is used.
	 * @param backOff the back off.
	 * @since 2.3
	 */
	public void setBackOff(BackOff backOff) {
		this.backOff = backOff;
	}

	@Override
	public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		data.partitions()
				.stream()
				.collect(
						Collectors.toMap(tp -> tp,
								tp -> data.records(tp).get(0).offset(), (u, v) -> (long) v, LinkedHashMap::new))
				.forEach(consumer::seek);

		if (this.backOff != null) {
			ListenerUtils.unrecoverableBackOff(this.backOff, this.backOffs, this.lastIntervals);
		}

		throw new KafkaException("Seek to current after exception", getLogLevel(), thrownException);
	}

	@Override
	public void clearThreadState() {
		this.backOffs.remove();
		this.lastIntervals.remove();
	}

}
