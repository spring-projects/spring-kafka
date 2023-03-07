/*
 * Copyright 2018-2023 the original author or authors.
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
import org.springframework.kafka.retrytopic.DestinationTopic.Type;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 *
 * Creates a list of {@link DestinationTopic.Properties} based on the
 * provided configurations.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author João Lima
 * @since 2.7
 *
 */
public class DestinationTopicPropertiesFactory {

	private static final String DEPRECATION = "deprecation";

	private static final String MAIN_TOPIC_SUFFIX = "";

	private final DestinationTopicSuffixes destinationTopicSuffixes;

	private final List<Long> backOffValues;

	private final BinaryExceptionClassifier exceptionClassifier;

	private final int numPartitions;

	private final int maxAttempts;

	private final KafkaOperations<?, ?> kafkaOperations;

	@SuppressWarnings(DEPRECATION)
	private final FixedDelayStrategy fixedDelayStrategy;

	private final DltStrategy dltStrategy;

	private final TopicSuffixingStrategy topicSuffixingStrategy;

	private final SameIntervalTopicReuseStrategy sameIntervalTopicReuseStrategy;

	private final long timeout;

	@Nullable
	private Boolean autoStartDltHandler;

	@SuppressWarnings(DEPRECATION)
	public DestinationTopicPropertiesFactory(String retryTopicSuffix, String dltSuffix, List<Long> backOffValues,
			BinaryExceptionClassifier exceptionClassifier,
			int numPartitions, KafkaOperations<?, ?> kafkaOperations,
			FixedDelayStrategy fixedDelayStrategy,
			DltStrategy dltStrategy,
			TopicSuffixingStrategy topicSuffixingStrategy,
			SameIntervalTopicReuseStrategy sameIntervalTopicReuseStrategy,
			long timeout) {

		this.dltStrategy = dltStrategy;
		this.kafkaOperations = kafkaOperations;
		this.exceptionClassifier = exceptionClassifier;
		this.numPartitions = numPartitions;
		this.fixedDelayStrategy = fixedDelayStrategy;
		this.topicSuffixingStrategy = topicSuffixingStrategy;
		this.sameIntervalTopicReuseStrategy = sameIntervalTopicReuseStrategy;
		this.timeout = timeout;
		this.destinationTopicSuffixes = new DestinationTopicSuffixes(retryTopicSuffix, dltSuffix);
		this.backOffValues = backOffValues;
		// Max Attempts include the initial try.
		this.maxAttempts = this.backOffValues.size() + 1;
	}

	@SuppressWarnings(DEPRECATION)
	public DestinationTopicPropertiesFactory(String retryTopicSuffix, String dltSuffix, List<Long> backOffValues,
			BinaryExceptionClassifier exceptionClassifier,
			int numPartitions, KafkaOperations<?, ?> kafkaOperations,
			FixedDelayStrategy fixedDelayStrategy,
			DltStrategy dltStrategy,
			TopicSuffixingStrategy topicSuffixingStrategy,
			long timeout) {
		this(retryTopicSuffix, dltSuffix, backOffValues, exceptionClassifier, numPartitions, kafkaOperations,
				fixedDelayStrategy, dltStrategy, topicSuffixingStrategy, SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
				timeout);
	}
	/**
	 * Set to false to not start the DLT handler.
	 * @param autoStart false to not start.
	 * @return this factory.
	 * @since 2.8
	 */
	public DestinationTopicPropertiesFactory autoStartDltHandler(@Nullable Boolean autoStart) {
		this.autoStartDltHandler = autoStart;
		return this;
	}

	public List<DestinationTopic.Properties> createProperties() {
		return isSingleTopicFixedDelay()
				? createPropertiesForFixedDelaySingleTopic()
				: createPropertiesForDefaultTopicStrategy();
	}

	private List<DestinationTopic.Properties> createPropertiesForFixedDelaySingleTopic() {
		return isNoDltStrategy()
					? Arrays.asList(createMainTopicProperties(),
							createRetryProperties(1, getShouldRetryOn()))
					: Arrays.asList(createMainTopicProperties(),
							createRetryProperties(1, getShouldRetryOn()),
							createDltProperties());
	}

	private boolean isSingleTopicFixedDelay() {
		return isFixedDelay() && (isSingleTopicStrategy() || isSingleTopicSameIntervalTopicReuseStrategy());
	}

	@SuppressWarnings(DEPRECATION)
	private boolean isSingleTopicStrategy() {
		return FixedDelayStrategy.SINGLE_TOPIC.equals(this.fixedDelayStrategy);
	}

	private boolean isSingleTopicSameIntervalTopicReuseStrategy() {
		return SameIntervalTopicReuseStrategy.SINGLE_TOPIC.equals(this.sameIntervalTopicReuseStrategy);
	}

	private List<DestinationTopic.Properties> createPropertiesForDefaultTopicStrategy() {

		int retryTopicsAmount = retryTopicsAmount();

		return IntStream.rangeClosed(0, isNoDltStrategy()
												? retryTopicsAmount
												: retryTopicsAmount + 1)
												.mapToObj(this::createTopicProperties)
												.collect(Collectors.toList());
	}

	int retryTopicsAmount() {
		return this.backOffValues.size() - reusableTopicAttempts();
	}

	private int reusableTopicAttempts() {
		return this.backOffValues.size() > 0
				? !isFixedDelay()
					? isSingleTopicSameIntervalTopicReuseStrategy()
						// Assuming that duplicates are always in
						//  the end of the list.
						? amountOfDuplicates(this.backOffValues.get(this.backOffValues.size() - 1)) - 1
						: 0
					: isSingleTopicStrategy()
						? this.backOffValues.size() - 1
						: 0
				: 0;
	}

	private boolean isNoDltStrategy() {
		return DltStrategy.NO_DLT.equals(this.dltStrategy);
	}

	private DestinationTopic.Properties createTopicProperties(int index) {
		BiPredicate<Integer, Throwable> shouldRetryOn = getShouldRetryOn();
		return index == 0
				? createMainTopicProperties()
				: (index <= this.retryTopicsAmount())
					? createRetryProperties(index, shouldRetryOn)
					: createDltProperties();
	}

	private DestinationTopic.Properties createMainTopicProperties() {
		return new DestinationTopic.Properties(0, MAIN_TOPIC_SUFFIX, DestinationTopic.Type.MAIN, this.maxAttempts,
				this.numPartitions, this.dltStrategy, this.kafkaOperations, getShouldRetryOn(), this.timeout);
	}

	private DestinationTopic.Properties createDltProperties() {
		return new DestinationTopic.Properties(0, this.destinationTopicSuffixes.getDltSuffix(),
				DestinationTopic.Type.DLT, this.maxAttempts, this.numPartitions, this.dltStrategy,
				this.kafkaOperations, (a, e) -> false, this.timeout, this.autoStartDltHandler);
	}

	private BiPredicate<Integer, Throwable> getShouldRetryOn() {
		return (attempt, throwable) -> attempt < this.maxAttempts && this.exceptionClassifier.classify(throwable);
	}

	@SuppressWarnings(DEPRECATION)
	private DestinationTopic.Properties createRetryProperties(int index,
															BiPredicate<Integer, Throwable> shouldRetryOn) {
		int indexInBackoffValues = index - 1;
		Long thisBackOffValue = this.backOffValues.get(indexInBackoffValues);
		DestinationTopic.Type topicTypeToUse = isSingleTopicFixedDelay()
			? Type.SINGLE_TOPIC_RETRY
			: isDelayWithReusedTopic(thisBackOffValue)
				? Type.REUSABLE_RETRY_TOPIC
				: Type.RETRY;
		return createProperties(topicTypeToUse, shouldRetryOn, indexInBackoffValues,
				getTopicSuffix(indexInBackoffValues, thisBackOffValue));
	}

	private String getTopicSuffix(int indexInBackoffValues, Long thisBackOffValue) {
		return isSingleTopicFixedDelay()
				? this.destinationTopicSuffixes.getRetrySuffix()
				: isSuffixWithIndexStrategy() || isFixedDelay()
					? joinWithRetrySuffix(indexInBackoffValues)
					: hasDuplicates(thisBackOffValue)
						? joinWithRetrySuffix(thisBackOffValue)
							.concat(suffixForRepeatedInterval(indexInBackoffValues, thisBackOffValue))
						: joinWithRetrySuffix(thisBackOffValue);
	}

	private String suffixForRepeatedInterval(int indexInBackoffValues, Long thisBackOffValue) {
		return isSingleTopicSameIntervalTopicReuseStrategy()
				? ""
				: "-" + getIndexInBackoffValues(indexInBackoffValues, thisBackOffValue);
	}

	private boolean isDelayWithReusedTopic(Long backoffValue) {
		return ((isSingleTopicFixedDelay()) ||
				(hasDuplicates(backoffValue) && isSingleTopicSameIntervalTopicReuseStrategy()));
	}

	private int getIndexInBackoffValues(int indexInBackoffValues, Long thisBackOffValue) {
		return indexInBackoffValues - this.backOffValues.indexOf(thisBackOffValue);
	}

	private boolean isSuffixWithIndexStrategy() {
		return TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE.equals(this.topicSuffixingStrategy);
	}

	private boolean hasDuplicates(Long thisBackOffValue) {
		return amountOfDuplicates(thisBackOffValue) > 1;
	}

	private int amountOfDuplicates(Long thisBackOffValue) {
		return Long.valueOf(this.backOffValues
				.stream()
				.filter(value -> value.equals(thisBackOffValue))
				.count()).intValue();
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
