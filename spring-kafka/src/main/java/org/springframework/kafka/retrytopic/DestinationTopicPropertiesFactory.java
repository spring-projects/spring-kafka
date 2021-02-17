/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.util.StringUtils;

/**
 *
 * Creates a list of {@link DestinationTopic.Properties} based on the
 * provided configurations.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class DestinationTopicPropertiesFactory {

	private static final String MAIN_TOPIC_SUFFIX = "";

	private final DestinationTopicSuffixes destinationTopicSuffixes;

	private final List<Long> backOffValues;

	private final BinaryExceptionClassifier exceptionClassifier;

	private final int numPartitions;

	private final int maxAttempts;

	private final KafkaOperations<?, ?> kafkaOperations;

	private final FixedDelayStrategy fixedDelayStrategy;

	private final DltStrategy dltStrategy;

	private final TopicSuffixingStrategy topicSuffixingStrategy;

	private final long timeout;

	public DestinationTopicPropertiesFactory(String retryTopicSuffix, String dltSuffix, int maxAttempts,
			BackOffPolicy backOffPolicy, BinaryExceptionClassifier exceptionClassifier,
			int numPartitions, KafkaOperations<?, ?> kafkaOperations,
			FixedDelayStrategy fixedDelayStrategy,
			DltStrategy dltStrategy,
			TopicSuffixingStrategy topicSuffixingStrategy,
			long timeout) {

		this.dltStrategy = dltStrategy;
		this.kafkaOperations = kafkaOperations;
		this.exceptionClassifier = exceptionClassifier;
		this.numPartitions = numPartitions;
		this.fixedDelayStrategy = fixedDelayStrategy;
		this.topicSuffixingStrategy = topicSuffixingStrategy;
		this.timeout = timeout;
		this.destinationTopicSuffixes = new DestinationTopicSuffixes(retryTopicSuffix, dltSuffix);
		this.backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();
		// Max Attempts include the initial try.
		this.maxAttempts = this.backOffValues.size() + 1;
	}

	public List<DestinationTopic.Properties> createProperties() {
		return isSingleTopicFixedDelay()
				? createPropertiesForFixedDelaySingleTopic()
				: createPropertiesForDefaultTopicStrategy();
	}

	private List<DestinationTopic.Properties> createPropertiesForFixedDelaySingleTopic() {
		return Arrays.asList(createMainTopicProperties(), createRetryProperties(1,
				DestinationTopic.Type.SINGLE_TOPIC_RETRY, getShouldRetryOn()), createDltProperties());
	}

	private boolean isSingleTopicFixedDelay() {
		return isFixedDelay() && isSingleTopicStrategy();
	}

	private boolean isSingleTopicStrategy() {
		return FixedDelayStrategy.SINGLE_TOPIC.equals(this.fixedDelayStrategy);
	}

	private List<DestinationTopic.Properties> createPropertiesForDefaultTopicStrategy() {
		return IntStream.rangeClosed(0, DltStrategy.NO_DLT.equals(this.dltStrategy)
							? this.maxAttempts - 1
							: this.maxAttempts)
				.mapToObj(index -> createRetryOrDltTopicSuffixes(index))
				.collect(Collectors.toList());
	}

	private DestinationTopic.Properties createMainTopicProperties() {
		return new DestinationTopic.Properties(0, MAIN_TOPIC_SUFFIX, DestinationTopic.Type.MAIN, this.maxAttempts,
				this.numPartitions, this.dltStrategy, this.kafkaOperations, getShouldRetryOn(), this.timeout);
	}

	private DestinationTopic.Properties createRetryOrDltTopicSuffixes(int index) {
		BiPredicate<Integer, Throwable> shouldRetryOn = getShouldRetryOn();
		return index == 0
				? createMainTopicProperties()
				: index < this.maxAttempts
				? createRetryProperties(index,
				DestinationTopic.Type.RETRY, shouldRetryOn)
				: createDltProperties();
	}

	private DestinationTopic.Properties createDltProperties() {
		return new DestinationTopic.Properties(0, this.destinationTopicSuffixes.getDltSuffix(),
				DestinationTopic.Type.DLT, this.maxAttempts, this.numPartitions, this.dltStrategy,
				this.kafkaOperations, (a, e) -> false, this.timeout);
	}

	private BiPredicate<Integer, Throwable> getShouldRetryOn() {
		return (attempt, throwable) -> attempt < this.maxAttempts && this.exceptionClassifier.classify(throwable);
	}

	private DestinationTopic.Properties createRetryProperties(int index,
															DestinationTopic.Type topicType,
															BiPredicate<Integer, Throwable> shouldRetryOn) {
		int indexInBackoffValues = index - 1;
		Long thisBackOffValue = this.backOffValues.get(indexInBackoffValues);
		return createProperties(topicType, shouldRetryOn, indexInBackoffValues,
				getTopicSuffix(indexInBackoffValues, thisBackOffValue));
	}

	private String getTopicSuffix(int indexInBackoffValues, Long thisBackOffValue) {
		return isSingleTopicFixedDelay()
				? this.destinationTopicSuffixes.getRetrySuffix()
				: isSuffixWithIndexStrategy() || isFixedDelay()
					? joinWithRetrySuffix(indexInBackoffValues)
					: hasDuplicates(thisBackOffValue)
						? joinWithRetrySuffix(thisBackOffValue)
						.concat("-" + getIndexInBackoffValues(indexInBackoffValues, thisBackOffValue))
						: joinWithRetrySuffix(thisBackOffValue);
	}

	private int getIndexInBackoffValues(int indexInBackoffValues, Long thisBackOffValue) {
		return indexInBackoffValues - this.backOffValues.indexOf(thisBackOffValue);
	}

	private boolean isSuffixWithIndexStrategy() {
		return TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE.equals(this.topicSuffixingStrategy);
	}

	private boolean hasDuplicates(Long thisBackOffValue) {
		return this
				.backOffValues
				.stream()
				.filter(value -> value.equals(thisBackOffValue))
				.count() > 1;
	}

	private DestinationTopic.Properties createProperties(DestinationTopic.Type topicType,
														BiPredicate<Integer, Throwable> shouldRetryOn,
														int indexInBackoffValues,
														String suffix) {
		return new DestinationTopic.Properties(this.backOffValues.get(indexInBackoffValues), suffix,
				topicType, this.maxAttempts, this.numPartitions, this.dltStrategy,
				this.kafkaOperations, shouldRetryOn, this.timeout);
	}

	private boolean isFixedDelay() {
		// If all values are the same, such as in NoBackOffPolicy and FixedBackoffPolicy
		return this.backOffValues.size() > 1 && this.backOffValues.stream().distinct().count() == 1;
	}

	private String joinWithRetrySuffix(long parameter) {
		return String.join("-", this.destinationTopicSuffixes.getRetrySuffix(), String.valueOf(parameter));
	}

	public static class DestinationTopicSuffixes {

		private final String retryTopicSuffix;

		private final String dltSuffix;

		public DestinationTopicSuffixes(String retryTopicSuffix, String dltSuffix) {
			this.retryTopicSuffix = StringUtils.hasText(retryTopicSuffix)
					? retryTopicSuffix
					: RetryTopicConstants.DEFAULT_RETRY_SUFFIX;
			this.dltSuffix = StringUtils.hasText(dltSuffix) ? dltSuffix : RetryTopicConstants.DEFAULT_DLT_SUFFIX;
		}

		public String getRetrySuffix() {
			return this.retryTopicSuffix;
		}

		public String getDltSuffix() {
			return this.dltSuffix;
		}
	}
}
