/*
 * Copyright 2014-2024 the original author or authors.
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

package org.springframework.kafka.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.springframework.util.StringUtils;
import org.springframework.kafka.annotation.EnableParallelConsumer;

/**
 * ParallelConsumerConfig is for config of {@link io.confluent.parallelconsumer}.
 * This will be registered as Spring Bean when {@link EnableParallelConsumer} is annotated to your spring application.
 * @author ...
 * @since 3.2.0
 */

public class ParallelConsumerConfig {


	private static final String PARALLEL_CONSUMER_MAX_CONCURRENCY = "PARALLEL_CONSUMER_MAX_CONCURRENCY";
	private static final String PARALLEL_CONSUMER_ORDERING = "PARALLEL_CONSUMER_ORDERING";
	private static final String ALLOW_EAGER_PROCESSING_DURING_TRANSACTION_COMMIT = "ALLOW_EAGER_PROCESSING_DURING_TRANSACTION_COMMIT";
	private static final String COMMIT_LOCK_ACQUISITION_TIMEOUT = "COMMIT_LOCK_ACQUISITION_TIMEOUT";
	private static final String COMMIT_INTERVAL = "COMMIT_INTERVAL";
	private final Map<String, String> properties = new HashMap<>();

	public ParallelConsumerConfig() {

		final String maxConcurrency = System.getenv(PARALLEL_CONSUMER_MAX_CONCURRENCY);
		final String ordering = System.getenv(PARALLEL_CONSUMER_ORDERING);
		final String allowEagerProcessingDuringTransactionCommit = System.getenv(ALLOW_EAGER_PROCESSING_DURING_TRANSACTION_COMMIT);
		final String commitLockAcquisitionTimeout = System.getenv(COMMIT_LOCK_ACQUISITION_TIMEOUT);
		final String commitInterval = System.getenv(COMMIT_INTERVAL);

		this.properties.put(PARALLEL_CONSUMER_MAX_CONCURRENCY, maxConcurrency);
		this.properties.put(PARALLEL_CONSUMER_ORDERING, ordering);
		this.properties.put(ALLOW_EAGER_PROCESSING_DURING_TRANSACTION_COMMIT, allowEagerProcessingDuringTransactionCommit);
		this.properties.put(COMMIT_LOCK_ACQUISITION_TIMEOUT, commitLockAcquisitionTimeout);
		this.properties.put(COMMIT_INTERVAL, commitInterval);
	}

	private ProcessingOrder toOrder(String order) {
		return switch (order) {
			case "partition" -> ProcessingOrder.PARTITION;
			case "unordered" -> ProcessingOrder.UNORDERED;
			default -> ProcessingOrder.KEY; // Confluent Consumer Default Policy
		};
	}

	public <K,V> ParallelConsumerOptions<K, V> toConsumerOptions(Consumer<K, V> consumer) {

		ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V> builder = ParallelConsumerOptions.builder();
		builder.consumer(consumer);

		final String maxConcurrencyString = this.properties.get(PARALLEL_CONSUMER_MAX_CONCURRENCY);
		final String orderingString = this.properties.get(PARALLEL_CONSUMER_ORDERING);
		final String allowEagerProcessingDuringTransactionCommitString = this.properties.get(ALLOW_EAGER_PROCESSING_DURING_TRANSACTION_COMMIT);
		final String commitLockAcquisitionTimeoutString = this.properties.get(COMMIT_LOCK_ACQUISITION_TIMEOUT);
		final String commitIntervalString = this.properties.get(COMMIT_INTERVAL);

		if (StringUtils.hasText(maxConcurrencyString)) {
			final Integer maxConcurrency = Integer.valueOf(maxConcurrencyString);
			builder.maxConcurrency(maxConcurrency);
		}

		if (StringUtils.hasText(orderingString)) {
			final ProcessingOrder processingOrder = toOrder(orderingString);
			builder.ordering(processingOrder);
		}

		if (StringUtils.hasText(allowEagerProcessingDuringTransactionCommitString)) {
			final Boolean allowEagerProcessingDuringTransactionCommit = Boolean.valueOf(allowEagerProcessingDuringTransactionCommitString);
			builder.allowEagerProcessingDuringTransactionCommit(allowEagerProcessingDuringTransactionCommit);
		}

		if (StringUtils.hasText(commitLockAcquisitionTimeoutString)) {
			final Long commitLockAcquisitionTimeout = Long.valueOf(commitLockAcquisitionTimeoutString);
			builder.commitLockAcquisitionTimeout(Duration.ofSeconds(commitLockAcquisitionTimeout));
		}

		if (StringUtils.hasText(commitIntervalString)) {
			final Long commitInterval = Long.valueOf(commitIntervalString);
			builder.commitInterval(Duration.ofMillis(commitInterval));
		}

		return builder.build();
	}
}