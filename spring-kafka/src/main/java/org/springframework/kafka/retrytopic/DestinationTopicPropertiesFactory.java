/*
 * Copyright 2018-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.DestinationTopic.Type;
import org.springframework.kafka.support.ExceptionMatcher;
import org.springframework.util.StringUtils;

/**
 *
 * Creates a list of {@link DestinationTopic.Properties} based on the
 * provided configurations.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author João Lima
 * @author Wang Zhiyang
 * @author Adrian Chlebosz
 * @since 2.7
 *
 */
public class DestinationTopicPropertiesFactory {

	private static final String MAIN_TOPIC_SUFFIX = "";

	private final DestinationTopicSuffixes destinationTopicSuffixes;

	private final List<Long> backOffValues;

	private final int numPartitions;

	private final int maxAttempts;

	private final boolean isSameIntervalReuse;

	private final boolean isFixedDelay;

	private final int retryTopicsAmount;

	private final BiPredicate<Integer, Throwable> shouldRetryOn;

	private final KafkaOperations<?, ?> kafkaOperations;

	private final DltStrategy dltStrategy;

	private final long timeout;

	private final Map<String, Set<Class<? extends Throwable>>> dltRoutingRules;

	@Nullable
	private Boolean autoStartDltHandler;

	/**
	 * Construct an instance with the provided properties.
	 * @param retryTopicSuffix the suffix.
	 * @param dltSuffix the dlt suffix.
	 * @param backOffValues the back off values.
	 * @param exceptionMatcher the exception matcher.
	 * @param numPartitions the number of partitions.
	 * @param kafkaOperations the operations.
	 * @param dltStrategy the dlt strategy.
	 * @param topicSuffixingStrategy the topic suffixing strategy.
	 * @param sameIntervalTopicReuseStrategy the same interval reuse strategy.
	 * @param timeout the timeout.
	 * @since 3.0.12
	 */
	public DestinationTopicPropertiesFactory(String retryTopicSuffix, String dltSuffix, List<Long> backOffValues,
			ExceptionMatcher exceptionMatcher,
			int numPartitions, KafkaOperations<?, ?> kafkaOperations,
			DltStrategy dltStrategy,
			TopicSuffixingStrategy topicSuffixingStrategy,
			SameIntervalTopicReuseStrategy sameIntervalTopicReuseStrategy,
			long timeout) {

		this(retryTopicSuffix, dltSuffix, backOffValues, exceptionMatcher, numPartitions, kafkaOperations,
				dltStrategy, topicSuffixingStrategy, sameIntervalTopicReuseStrategy, timeout, Collections.emptyMap());
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param retryTopicSuffix the suffix.
	 * @param dltSuffix the dlt suffix.
	 * @param backOffValues the back off values.
	 * @param exceptionMatcher the exception matcher.
	 * @param numPartitions the number of partitions.
	 * @param kafkaOperations the operations.
	 * @param dltStrategy the dlt strategy.
	 * @param topicSuffixingStrategy the topic suffixing strategy.
	 * @param sameIntervalTopicReuseStrategy the same interval reuse strategy.
	 * @param timeout the timeout.
	 * @param dltRoutingRules the specification of which DLT should be used for the particular exception type
	 * @since 3.2.0
	 */
	public DestinationTopicPropertiesFactory(@Nullable String retryTopicSuffix, @Nullable String dltSuffix, List<Long> backOffValues,
			ExceptionMatcher exceptionMatcher,
			int numPartitions, KafkaOperations<?, ?> kafkaOperations,
			DltStrategy dltStrategy,
			TopicSuffixingStrategy topicSuffixingStrategy,
			SameIntervalTopicReuseStrategy sameIntervalTopicReuseStrategy,
			long timeout,
			Map<String, Set<Class<? extends Throwable>>> dltRoutingRules) {

		this.dltStrategy = dltStrategy;
		this.kafkaOperations = kafkaOperations;
		this.numPartitions = numPartitions;
		this.timeout = timeout;
		this.destinationTopicSuffixes = new DestinationTopicSuffixes(retryTopicSuffix, dltSuffix);
		this.dltRoutingRules = copyDltRoutingRules(dltRoutingRules);
		this.backOffValues = backOffValues;
		int backOffValuesSize = this.backOffValues.size();
		this.isSameIntervalReuse = SameIntervalTopicReuseStrategy.SINGLE_TOPIC.equals(sameIntervalTopicReuseStrategy);
		this.isFixedDelay = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE.equals(topicSuffixingStrategy)
				|| backOffValuesSize > 1 && backOffValues.stream().distinct().count() == 1;
		// Max Attempts to include the initial try.
		this.maxAttempts = backOffValuesSize + 1;
		this.shouldRetryOn = (attempt, throwable) -> attempt < this.maxAttempts
				&& exceptionMatcher.match(throwable);
		this.retryTopicsAmount = backOffValuesSize - reusableTopicAttempts();
	}

	private Map<String, Set<Class<? extends Throwable>>> copyDltRoutingRules(Map<String, Set<Class<? extends Throwable>>> dltRoutingRules) {
		Map<String, Set<Class<? extends Throwable>>> copyOfDltRoutingRules = new HashMap<>();
		dltRoutingRules.forEach((topicSuffix, exceptions) -> copyOfDltRoutingRules.put(topicSuffix, new HashSet<>(exceptions)));
		return copyOfDltRoutingRules;
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
		List<DestinationTopic.Properties> list = new ArrayList<>(this.retryTopicsAmount + 2);
		list.add(createMainTopicProperties());
		for (int backOffIndex = 0; backOffIndex < this.retryTopicsAmount; backOffIndex++) {
			list.add(createRetryProperties(backOffIndex));
		}
		if (!DltStrategy.NO_DLT.equals(this.dltStrategy)) {
			list.addAll(createCustomDltProperties());
			list.add(createDltProperties());
		}
		return Collections.unmodifiableList(list);
	}

	private DestinationTopic.Properties createMainTopicProperties() {
		return new DestinationTopic.Properties(0, MAIN_TOPIC_SUFFIX, DestinationTopic.Type.MAIN, this.maxAttempts,
				this.numPartitions, this.dltStrategy, this.kafkaOperations, this.shouldRetryOn, this.timeout);
	}

	private DestinationTopic.Properties createDltProperties() {
		return new DestinationTopic.Properties(0, this.destinationTopicSuffixes.getDltSuffix(),
				DestinationTopic.Type.DLT, this.maxAttempts, this.numPartitions, this.dltStrategy,
			this.kafkaOperations, (a, e) -> false, this.timeout, this.autoStartDltHandler, Collections.emptySet());
	}

	private List<DestinationTopic.Properties> createCustomDltProperties() {
		return this.dltRoutingRules.entrySet().stream()
			.map(entry -> new DestinationTopic.Properties(0, entry.getKey() + this.destinationTopicSuffixes.getDltSuffix(),
				DestinationTopic.Type.DLT, this.maxAttempts, this.numPartitions, this.dltStrategy,
				this.kafkaOperations, (a, e) -> false, this.timeout, this.autoStartDltHandler, entry.getValue()))
			.toList();
	}

	private DestinationTopic.Properties createRetryProperties(int backOffIndex) {
		long thisBackOffValue = this.backOffValues.get(backOffIndex);
		return createProperties(thisBackOffValue, getTopicSuffix(backOffIndex, thisBackOffValue));
	}

	private String getTopicSuffix(int backOffIndex, long thisBackOffValue) {
		if (this.isSameIntervalReuse && this.retryTopicsAmount == 1) {
			return this.destinationTopicSuffixes.getRetrySuffix();
		}
		else if (this.isFixedDelay) {
			return joinWithRetrySuffix(backOffIndex);
		}
		else {
			String retrySuffix = joinWithRetrySuffix(thisBackOffValue);
			if (!this.isSameIntervalReuse && hasDuplicates(thisBackOffValue)) {
				return retrySuffix.concat("-" + (backOffIndex - this.backOffValues.indexOf(thisBackOffValue)));
			}
			return retrySuffix;
		}
	}

	private DestinationTopic.Type getDestinationTopicType(Long backOffValue) {
		return this.isSameIntervalReuse && hasDuplicates(backOffValue) ? Type.REUSABLE_RETRY_TOPIC : Type.RETRY;
	}

	private int reusableTopicAttempts() {
		if (this.isSameIntervalReuse && this.backOffValues.size() > 1) {
			// Assuming that duplicates are always at the end of the list.
			return amountOfDuplicates(this.backOffValues.get(this.backOffValues.size() - 1)) - 1;
		}
		return 0;
	}

	private boolean hasDuplicates(Long thisBackOffValue) {
		return amountOfDuplicates(thisBackOffValue) > 1;
	}

	private int amountOfDuplicates(Long thisBackOffValue) {
		return Long.valueOf(this.backOffValues
				.stream()
				.filter(thisBackOffValue::equals)
				.count())
				.intValue();
	}

	private DestinationTopic.Properties createProperties(long delayMs, String suffix) {
		return new DestinationTopic.Properties(delayMs, suffix, getDestinationTopicType(delayMs), this.maxAttempts,
				this.numPartitions, this.dltStrategy, this.kafkaOperations, this.shouldRetryOn, this.timeout);
	}

	private String joinWithRetrySuffix(long parameter) {
		return String.join("-", this.destinationTopicSuffixes.getRetrySuffix(), String.valueOf(parameter));
	}

	public static class DestinationTopicSuffixes {

		private final String retryTopicSuffix;

		private final String dltSuffix;

		public DestinationTopicSuffixes(@Nullable String retryTopicSuffix, @Nullable String dltSuffix) {
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
