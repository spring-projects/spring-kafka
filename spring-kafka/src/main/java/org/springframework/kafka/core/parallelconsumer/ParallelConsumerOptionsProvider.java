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

package org.springframework.kafka.core.parallelconsumer;

import java.time.Duration;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.springframework.kafka.config.ParallelConsumerContext;
import org.springframework.kafka.core.ParallelConsumerFactory;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ParallelConsumerOptionsBuilder;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.RecordContext;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

/**
 *
 * @author Sanghyeok An
 * @since 3.3
 */

public interface ParallelConsumerOptionsProvider<K, V> {

	default void hello() {
		ParallelConsumerOptionsBuilder<Object, Object> builder = ParallelConsumerOptions.builder();
	}

	@Nullable
	default String managedExecutorService() {
		return null;
	}

	@Nullable
	default String managedThreadFactory() {
		return null;
	}

	@Nullable
	default MeterRegistry meterRegistry() {
		return null;
	}

	@Nullable
	default String pcInstanceTag() {
		return null;
	}

	@Nullable
	default Iterable<Tag> metricsTags() {
		return null;
	}

	@Nullable
	default Boolean allowEagerProcessingDuringTransactionCommit() {
		return null;
	}

	@Nullable
	default Duration commitLockAcquisitionTimeout() {
		return null;
	}

	@Nullable
	default Duration produceLockAcquisitionTimeout() {
		return null;
	}

	@Nullable
	default Duration commitInterval() {
		return null;
	}

	@Nullable
	default ProcessingOrder ordering() {
		return null;
	}

	@Nullable
	default CommitMode commitMode() {
		return null;
	}

	@Nullable
	default Integer maxConcurrency() {
		return null;
	}

	@Nullable
	default InvalidOffsetMetadataHandlingPolicy invalidOffsetMetadataPolicy() {
		return null;
	}

	@Nullable
	default Function<RecordContext<K, V>, Duration> retryDelayProvider() {
		return null;
	}

	@Nullable
	default Duration sendTimeout() {
		return null;
	}

	@Nullable
	default Duration offsetCommitTimeout() {
		return null;
	}

	@Nullable
	default Integer batchSize() {
		return null;
	}

	@Nullable
	default Duration thresholdForTimeSpendInQueueWarning () {
		return null;
	}

	@Nullable
	default Integer maxFailureHistory() {
		return null;
	}

	@Nullable
	default Duration shutdownTimeout() {
		return null;
	}

	@Nullable
	default Duration drainTimeout() {
		return null;
	}

	@Nullable
	default Integer messageBufferSize() {
		return null;
	}

	@Nullable
	default Integer initialLoadFactor() {
		return null;
	}

	@Nullable
	default Integer maximumLoadFactor() {
		return null;
	}

}
