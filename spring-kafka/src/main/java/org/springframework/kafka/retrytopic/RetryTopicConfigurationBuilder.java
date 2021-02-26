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

import java.util.ArrayList;
import java.util.List;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.BinaryExceptionClassifierBuilder;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.AllowDenyCollectionManager;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.NoBackOffPolicy;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;
import org.springframework.util.Assert;

/**
 *
 * Builder class to create {@link RetryTopicConfiguration} instances.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class RetryTopicConfigurationBuilder {

	private static final String ALREADY_SELECTED = "You have already selected backoff policy";

	private final List<String> includeTopicNames = new ArrayList<>();

	private final List<String> excludeTopicNames = new ArrayList<>();

	private int maxAttempts = RetryTopicConstants.NOT_SET;

	private BackOffPolicy backOffPolicy;

	private EndpointHandlerMethod dltHandlerMethod;

	private String retryTopicSuffix;

	private String dltSuffix;

	private RetryTopicConfiguration.TopicCreation topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation();

	private ConcurrentKafkaListenerContainerFactory<?, ?> listenerContainerFactory;

	private String listenerContainerFactoryName;

	private BinaryExceptionClassifierBuilder classifierBuilder;

	private FixedDelayStrategy fixedDelayStrategy = FixedDelayStrategy.MULTIPLE_TOPICS;

	private DltStrategy dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR;

	private long timeout = RetryTopicConstants.NOT_SET;

	private TopicSuffixingStrategy topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE;

	/* ---------------- DLT Behavior -------------- */
	public RetryTopicConfigurationBuilder dltHandlerMethod(Class<?> clazz, String methodName) {
		this.dltHandlerMethod = RetryTopicConfigurer.createHandlerMethodWith(clazz, methodName);
		return this;
	}

	public RetryTopicConfigurationBuilder dltHandlerMethod(
			EndpointHandlerMethod endpointHandlerMethod) {

		this.dltHandlerMethod = endpointHandlerMethod;
		return this;
	}

	public RetryTopicConfigurationBuilder doNotRetryOnDltFailure() {
		this.dltStrategy =
				DltStrategy.FAIL_ON_ERROR;
		return this;
	}

	public RetryTopicConfigurationBuilder dltProcessingFailureStrategy(
			DltStrategy dltStrategy) {
		this.dltStrategy = dltStrategy;
		return this;
	}

	public RetryTopicConfigurationBuilder doNotConfigureDlt() {
		this.dltStrategy =
				DltStrategy.NO_DLT;
		return this;
	}

	/* ---------------- Configure Topic GateKeeper -------------- */
	public RetryTopicConfigurationBuilder includeTopics(List<String> topicNames) {
		this.includeTopicNames.addAll(topicNames);
		return this;
	}

	public RetryTopicConfigurationBuilder excludeTopics(List<String> topicNames) {
		this.excludeTopicNames.addAll(topicNames);
		return this;
	}

	public RetryTopicConfigurationBuilder includeTopic(String topicName) {
		this.includeTopicNames.add(topicName);
		return this;
	}

	public RetryTopicConfigurationBuilder excludeTopic(String topicName) {
		this.excludeTopicNames.add(topicName);
		return this;
	}

	/* ---------------- Configure Topic Suffixes -------------- */

	public RetryTopicConfigurationBuilder retryTopicSuffix(String suffix) {
		this.retryTopicSuffix = suffix;
		return this;
	}

	public RetryTopicConfigurationBuilder dltSuffix(String suffix) {
		this.dltSuffix = suffix;
		return this;
	}

	public RetryTopicConfigurationBuilder suffixTopicsWithIndexValues() {
		this.topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE;
		return this;
	}

	public RetryTopicConfigurationBuilder setTopicSuffixingStrategy(TopicSuffixingStrategy topicSuffixingStrategy) {
		this.topicSuffixingStrategy = topicSuffixingStrategy;
		return this;
	}

	/* ---------------- Configure BackOff -------------- */

	public RetryTopicConfigurationBuilder maxAttempts(int maxAttempts) {
		Assert.isTrue(maxAttempts > 0, "Number of attempts should be positive");
		Assert.isTrue(this.maxAttempts == RetryTopicConstants.NOT_SET,
				"You have already set the number of attempts");
		this.maxAttempts = maxAttempts;
		return this;
	}

	public RetryTopicConfigurationBuilder timeoutAfter(long timeout) {
		this.timeout = timeout;
		return this;
	}

	public RetryTopicConfigurationBuilder exponentialBackoff(long initialInterval, double multiplier, long maxInterval) {
		return exponentialBackoff(initialInterval, multiplier, maxInterval, false);
	}

	public RetryTopicConfigurationBuilder exponentialBackoff(long initialInterval, double multiplier, long maxInterval,
			boolean withRandom) {

		Assert.isNull(this.backOffPolicy, ALREADY_SELECTED);
		Assert.isTrue(initialInterval >= 1, "Initial interval should be >= 1");
		Assert.isTrue(multiplier > 1, "Multiplier should be > 1");
		Assert.isTrue(maxInterval > initialInterval, "Max interval should be > than initial interval");
		ExponentialBackOffPolicy policy = withRandom ? new ExponentialRandomBackOffPolicy()
				: new ExponentialBackOffPolicy();
		policy.setInitialInterval(initialInterval);
		policy.setMultiplier(multiplier);
		policy.setMaxInterval(maxInterval);
		this.backOffPolicy = policy;
		return this;
	}

	public RetryTopicConfigurationBuilder fixedBackOff(long interval) {
		Assert.isNull(this.backOffPolicy, ALREADY_SELECTED);
		Assert.isTrue(interval >= 1, "Interval should be >= 1");
		FixedBackOffPolicy policy = new FixedBackOffPolicy();
		policy.setBackOffPeriod(interval);
		this.backOffPolicy = policy;
		return this;
	}

	public RetryTopicConfigurationBuilder uniformRandomBackoff(long minInterval, long maxInterval) {
		Assert.isNull(this.backOffPolicy, ALREADY_SELECTED);
		Assert.isTrue(minInterval >= 1, "Min interval should be >= 1");
		Assert.isTrue(maxInterval >= 1, "Max interval should be >= 1");
		Assert.isTrue(maxInterval > minInterval, "Max interval should be > than min interval");
		UniformRandomBackOffPolicy policy = new UniformRandomBackOffPolicy();
		policy.setMinBackOffPeriod(minInterval);
		policy.setMaxBackOffPeriod(maxInterval);
		this.backOffPolicy = policy;
		return this;
	}

	public RetryTopicConfigurationBuilder noBackoff() {
		Assert.isNull(this.backOffPolicy, ALREADY_SELECTED);
		this.backOffPolicy = new NoBackOffPolicy();
		return this;
	}

	public RetryTopicConfigurationBuilder customBackoff(SleepingBackOffPolicy<?> backOffPolicy) {
		Assert.isNull(this.backOffPolicy, ALREADY_SELECTED);
		Assert.notNull(backOffPolicy, "You should provide non null custom policy");
		this.backOffPolicy = backOffPolicy;
		return this;
	}

	public RetryTopicConfigurationBuilder fixedBackOff(int interval) {
		FixedBackOffPolicy policy = new FixedBackOffPolicy();
		policy.setBackOffPeriod(interval);
		this.backOffPolicy = policy;
		return this;
	}

	public RetryTopicConfigurationBuilder useSingleTopicForFixedDelays() {
		this.fixedDelayStrategy = FixedDelayStrategy.SINGLE_TOPIC;
		return this;
	}

	public RetryTopicConfigurationBuilder useSingleTopicForFixedDelays(FixedDelayStrategy useSameTopicForFixedDelays) {
		this.fixedDelayStrategy = useSameTopicForFixedDelays;
		return this;
	}

	/* ---------------- Configure Topics Auto Creation -------------- */

	public RetryTopicConfigurationBuilder doNotAutoCreateRetryTopics() {
		this.topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation(false);
		return this;
	}

	public RetryTopicConfigurationBuilder autoCreateTopicsWith(int numPartitions, short replicationFactor) {
		this.topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation(true, numPartitions,
				replicationFactor);
		return this;
	}

	public RetryTopicConfigurationBuilder autoCreateTopics(boolean shouldCreate, int numPartitions,
			short replicationFactor) {

		this.topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation(shouldCreate, numPartitions,
				replicationFactor);
		return this;
	}

	/* ---------------- Configure Exception Classifier -------------- */

	public RetryTopicConfigurationBuilder retryOn(Class<? extends Throwable> throwable) {
		classifierBuilder().retryOn(throwable);
		return this;
	}

	public RetryTopicConfigurationBuilder notRetryOn(Class<? extends Throwable> throwable) {
		classifierBuilder().notRetryOn(throwable);
		return this;
	}

	public RetryTopicConfigurationBuilder retryOn(List<Class<? extends Throwable>> throwables) {
		throwables
				.stream()
				.forEach(throwable -> classifierBuilder().retryOn(throwable));
		return this;
	}

	public RetryTopicConfigurationBuilder notRetryOn(List<Class<? extends Throwable>> throwables) {
		throwables
				.stream()
				.forEach(throwable -> classifierBuilder().notRetryOn(throwable));
		return this;
	}

	public RetryTopicConfigurationBuilder traversingCauses() {
		classifierBuilder().traversingCauses();
		return this;
	}

	public RetryTopicConfigurationBuilder traversingCauses(boolean traversing) {
		if (traversing) {
			classifierBuilder().traversingCauses();
		}
		return this;
	}

	private BinaryExceptionClassifierBuilder classifierBuilder() {
		if (this.classifierBuilder == null) {
			this.classifierBuilder = new BinaryExceptionClassifierBuilder();
		}
		return this.classifierBuilder;
	}

	/* ---------------- Configure KafkaListenerContainerFactory -------------- */
	public RetryTopicConfigurationBuilder listenerFactory(ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
		this.listenerContainerFactory = factory;
		return this;
	}

	public RetryTopicConfigurationBuilder listenerFactory(String factoryBeanName) {
		this.listenerContainerFactoryName = factoryBeanName;
		return this;
	}

	// The templates are configured per ListenerContainerFactory. Only the first configured ones will be used.
	public RetryTopicConfiguration create(KafkaOperations<?, ?> sendToTopicKafkaTemplate) {
		ListenerContainerFactoryResolver.Configuration containerFactory =
				new ListenerContainerFactoryResolver.Configuration(this.listenerContainerFactory,
						this.listenerContainerFactoryName);
		AllowDenyCollectionManager<String> allowListManager =
				new AllowDenyCollectionManager<>(this.includeTopicNames, this.excludeTopicNames);
		List<DestinationTopic.Properties> destinationTopicProperties =
				new DestinationTopicPropertiesFactory(this.retryTopicSuffix, this.dltSuffix, this.maxAttempts,
						this.backOffPolicy, buildClassifier(), this.topicCreationConfiguration.getNumPartitions(),
						sendToTopicKafkaTemplate, this.fixedDelayStrategy, this.dltStrategy,
						this.topicSuffixingStrategy, this.timeout)
						.createProperties();
		return new RetryTopicConfiguration(destinationTopicProperties,
				this.dltHandlerMethod, this.topicCreationConfiguration, allowListManager, containerFactory);
	}

	private BinaryExceptionClassifier buildClassifier() {
		return this.classifierBuilder != null
				? this.classifierBuilder.build()
				: new BinaryExceptionClassifierBuilder().retryOn(Throwable.class).build();
	}

	/**
	 * Create a new instance of the builder.
	 * @return the new instance.
	 */
	public static RetryTopicConfigurationBuilder newInstance() {
		return new RetryTopicConfigurationBuilder();
	}

}
