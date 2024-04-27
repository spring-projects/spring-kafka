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


import org.apache.kafka.clients.consumer.Consumer;

import io.confluent.parallelconsumer.ParallelConsumerOptions;

import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.parallelconsumer.ParallelConsumerOptionsProvider;
import org.springframework.kafka.annotation.EnableParallelConsumer;

// It would be better to be migrated to org.springframework.boot.autoconfigure.kafka.KafkaProperties.

/**
 * ParallelConsumerConfig is for config of {@link io.confluent.parallelconsumer}.
 * This will be registered as Spring Bean when {@link EnableParallelConsumer} is annotated to your spring application.
 *
 * @author Sanghyeok An
 *
 * @since 3.3
 */

public class ParallelConsumerConfig<K, V> {

	public static final String DEFAULT_BEAN_NAME = "parallelConsumerConfig";
	private final ParallelConsumerOptionsProvider<K, V> provider;

	public ParallelConsumerConfig(ParallelConsumerOptionsProvider<K, V> provider) {
		this.provider = provider;
	}

	public ParallelConsumerOptions<K, V> toConsumerOptions(
			ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V> builder,
			Consumer<K, V> consumer,
			Producer<K, V> producer) {

		builder.producer(producer);
		return toConsumerOptions(builder, consumer);
	}

	public ParallelConsumerOptions<K, V> toConsumerOptions(
			ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V> builder,
			Consumer<K, V> consumer) {
		builder.consumer(consumer);
		return buildRemainOptions(builder);
	}

	private ParallelConsumerOptions<K, V> buildRemainOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<K, V> builder) {
		if (this.provider.managedExecutorService() != null){
			builder.managedExecutorService(this.provider.managedExecutorService());
		}

		if (this.provider.managedThreadFactory() != null){
			builder.managedThreadFactory(this.provider.managedThreadFactory());
		}

		if (this.provider.meterRegistry() != null){
			builder.meterRegistry(this.provider.meterRegistry());
		}

		if (this.provider.pcInstanceTag() != null){
			builder.pcInstanceTag(this.provider.pcInstanceTag());
		}

		if (this.provider.metricsTags() != null){
			builder.metricsTags(this.provider.metricsTags());
		}

		if (this.provider.allowEagerProcessingDuringTransactionCommit() != null){
			builder.allowEagerProcessingDuringTransactionCommit(this.provider.allowEagerProcessingDuringTransactionCommit());
		}

		if (this.provider.commitLockAcquisitionTimeout() != null){
			builder.commitLockAcquisitionTimeout(this.provider.commitLockAcquisitionTimeout());
		}

		if (this.provider.produceLockAcquisitionTimeout() != null){
			builder.produceLockAcquisitionTimeout(this.provider.produceLockAcquisitionTimeout());
		}

		if (this.provider.commitInterval() != null){
			builder.commitInterval(this.provider.commitInterval());
		}

		if (this.provider.ordering() != null){
			builder.ordering(this.provider.ordering());
		}

		if (this.provider.commitMode() != null){
			builder.commitMode(this.provider.commitMode());
		}

		if (this.provider.maxConcurrency() != null){
			builder.maxConcurrency(this.provider.maxConcurrency());
		}

		if (this.provider.invalidOffsetMetadataPolicy() != null){
			builder.invalidOffsetMetadataPolicy(this.provider.invalidOffsetMetadataPolicy());
		}

		if (this.provider.retryDelayProvider() != null){
			builder.retryDelayProvider(this.provider.retryDelayProvider());
		}

		if (this.provider.sendTimeout() != null){
			builder.sendTimeout(this.provider.sendTimeout());
		}

		if (this.provider.offsetCommitTimeout() != null){
			builder.offsetCommitTimeout(this.provider.offsetCommitTimeout());
		}

		if (this.provider.batchSize() != null){
			builder.batchSize(this.provider.batchSize());
		}

		if (this.provider.thresholdForTimeSpendInQueueWarning() != null){
			builder.thresholdForTimeSpendInQueueWarning(this.provider.thresholdForTimeSpendInQueueWarning());
		}

		if (this.provider.maxFailureHistory() != null){
			builder.maxFailureHistory(this.provider.maxFailureHistory());
		}

		if (this.provider.shutdownTimeout() != null){
			builder.shutdownTimeout(this.provider.shutdownTimeout());
		}

		if (this.provider.drainTimeout() != null){
			builder.drainTimeout(this.provider.drainTimeout());
		}

		if (this.provider.messageBufferSize() != null){
			builder.messageBufferSize(this.provider.messageBufferSize());
		}

		if (this.provider.initialLoadFactor() != null){
			builder.initialLoadFactor(this.provider.initialLoadFactor());
		}
		if (this.provider.maximumLoadFactor() != null){
			builder.maximumLoadFactor(this.provider.maximumLoadFactor());
		}

		return builder.build();
	}

}
