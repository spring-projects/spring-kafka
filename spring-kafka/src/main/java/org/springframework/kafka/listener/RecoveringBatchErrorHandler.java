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

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.KafkaException;
import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;

/**
 * An error handler that seeks to the current offset for each topic in a batch of records.
 * Used to rewind partitions after a message failure so that the batch can be replayed. If
 * the listener throws a {@link BatchListenerFailedException}, with the failed record. The
 * records before the record will have their offsets committed and the partitions for the
 * remaining records will be repositioned and/or the failed record can be recovered and
 * skipped. If some other exception is thrown, or a valid record is not provided in the
 * exception, error handling is delegated to a {@link SeekToCurrentBatchErrorHandler} with
 * this handler's {@link BackOff}. If the record is recovered, its offset is committed.
 *
 * @author Gary Russell
 * @author Myeonghyeon Lee
 * @since 2.5
 *
 */
public class RecoveringBatchErrorHandler extends FailedRecordProcessor
		implements ContainerAwareBatchErrorHandler {

	private static final LoggingCommitCallback LOGGING_COMMIT_CALLBACK = new LoggingCommitCallback();

	private final SeekToCurrentBatchErrorHandler fallbackHandler = new SeekToCurrentBatchErrorHandler();

	private boolean ackAfterHandle = true;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 */
	public RecoveringBatchErrorHandler() {
		this(null, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param backOff the {@link BackOff}.
	 */
	public RecoveringBatchErrorHandler(BackOff backOff) {
		this(null, backOff);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 */
	public RecoveringBatchErrorHandler(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
		this(recoverer, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after the
	 * backOff returns STOP for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param backOff the {@link BackOff}.
	 * @since 2.3
	 */
	public RecoveringBatchErrorHandler(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
			BackOff backOff) {

		super(recoverer, backOff);
		this.fallbackHandler.setBackOff(backOff);
	}

	@Override
	public boolean isAckAfterHandle() {
		return this.ackAfterHandle;
	}

	@Override
	public void setAckAfterHandle(boolean ackAfterHandle) {
		this.ackAfterHandle = ackAfterHandle;
	}

	@Override
	public void handle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		BatchListenerFailedException batchListenerFailedException = getBatchListenerFailedException(thrownException);
		if (batchListenerFailedException == null) {
			this.logger.debug(thrownException, "Expected a BatchListenerFailedException; re-seeking batch");
			this.fallbackHandler.handle(thrownException, data, consumer, container);
		}
		else {
			ConsumerRecord<?, ?> record = batchListenerFailedException.getRecord();
			int index = record != null ? findIndex(data, record) : batchListenerFailedException.getIndex();
			if (index < 0 || index >= data.count()) {
				this.logger.warn(batchListenerFailedException, () -> String.format("Record not found in batch: %s-%d@%d; re-seeking batch",
						record.topic(), record.partition(), record.offset()));
				this.fallbackHandler.handle(thrownException, data, consumer, container);
			}
			else {
				seekOrRecover(thrownException, data, consumer, container, index);
			}
		}
	}

	private int findIndex(ConsumerRecords<?, ?> data, ConsumerRecord<?, ?> record) {
		if (record == null) {
			return -1;
		}
		int i = 0;
		Iterator<?> iterator = data.iterator();
		while (iterator.hasNext()) {
			ConsumerRecord<?, ?> candidate = (ConsumerRecord<?, ?>) iterator.next();
			if (candidate.topic().equals(record.topic()) && candidate.partition() == record.partition()
					&& candidate.offset() == record.offset()) {
				break;
			}
			i++;
		}
		return i;
	}

	private void seekOrRecover(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, int indexArg) {

		Iterator<?> iterator = data.iterator();
		List<ConsumerRecord<?, ?>> toCommit = new ArrayList<>();
		List<ConsumerRecord<?, ?>> remaining = new ArrayList<>();
		int index = indexArg;
		while (iterator.hasNext()) {
			ConsumerRecord<?, ?> record = (ConsumerRecord<?, ?>) iterator.next();
			if (index-- > 0) {
				toCommit.add(record);
			}
			else {
				remaining.add(record);
			}
		}
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		toCommit.forEach(rec -> offsets.compute(new TopicPartition(rec.topic(), rec.partition()),
				(key, val) -> new OffsetAndMetadata(rec.offset() + 1)));
		if (offsets.size() > 0) {
			commit(consumer, container, offsets);
		}
		if (remaining.size() > 0) {
			SeekUtils.seekOrRecover(thrownException, remaining, consumer, container, false,
					getSkipPredicate(remaining, thrownException), this.logger, getLogLevel());
			ConsumerRecord<?, ?> recovered = remaining.get(0);
			commit(consumer, container,
					Collections.singletonMap(new TopicPartition(recovered.topic(), recovered.partition()),
							new OffsetAndMetadata(recovered.offset() + 1)));
			if (remaining.size() > 1) {
				throw new KafkaException("Seek to current after exception", getLogLevel(), thrownException);
			}
		}
	}

	private void commit(Consumer<?, ?> consumer, MessageListenerContainer container,
			Map<TopicPartition, OffsetAndMetadata> offsets) {

		boolean syncCommits = container.getContainerProperties().isSyncCommits();
		Duration timeout = container.getContainerProperties().getSyncCommitTimeout();
		if (syncCommits) {
			consumer.commitSync(offsets, timeout);
		}
		else {
			OffsetCommitCallback commitCallback = container.getContainerProperties().getCommitCallback();
			if (commitCallback == null) {
				commitCallback = LOGGING_COMMIT_CALLBACK;
			}
			consumer.commitAsync(offsets, commitCallback);
		}
	}

	private BatchListenerFailedException getBatchListenerFailedException(Throwable throwableArg) {
		if (throwableArg == null || throwableArg instanceof BatchListenerFailedException) {
			return (BatchListenerFailedException) throwableArg;
		}

		BatchListenerFailedException target = null;

		Throwable throwable = throwableArg;
		Set<Throwable> checked = new HashSet<>();
		while (throwable.getCause() != null && !checked.contains(throwable.getCause())) {
			throwable = throwable.getCause();
			checked.add(throwable);

			if (throwable instanceof BatchListenerFailedException) {
				target = (BatchListenerFailedException) throwable;
				break;
			}
		}

		return target;
	}
}
