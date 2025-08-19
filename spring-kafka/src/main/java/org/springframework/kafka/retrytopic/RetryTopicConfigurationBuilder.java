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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.AllowDenyCollectionManager;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.kafka.support.ExceptionMatcher;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 *
 * Builder class to create {@link RetryTopicConfiguration} instances.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @author Wang Zhiyang
 * @author Stephane Nicoll
 *
 * @since 2.7
 *
 */
public class RetryTopicConfigurationBuilder {

	private static final String ALREADY_CONFIGURED = "A custom backOff has already been configured";

	private final List<String> includeTopicNames = new ArrayList<>();

	private final List<String> excludeTopicNames = new ArrayList<>();

	private int maxAttempts = RetryTopicConstants.NOT_SET;

	@Nullable
	private BackOff backOff;

	@Nullable
	private EndpointHandlerMethod dltHandlerMethod;

	@Nullable
	private String retryTopicSuffix;

	@Nullable
	private String dltSuffix;

	private RetryTopicConfiguration.TopicCreation topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation();

	@Nullable
	private ConcurrentKafkaListenerContainerFactory<?, ?> listenerContainerFactory;

	@Nullable
	private String listenerContainerFactoryName;

	@Nullable
	private ExceptionEntriesConfigurer exceptionEntriesConfigurer;

	@Nullable
	private Boolean traversingCauses;

	private final Map<String, Set<Class<? extends Throwable>>> dltRoutingRules = new HashMap<>();

	private DltStrategy dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR;

	private long timeout = RetryTopicConstants.NOT_SET;

	private TopicSuffixingStrategy topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE;

	private SameIntervalTopicReuseStrategy sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS;

	@Nullable
	private Boolean autoStartDltHandler;

	private @Nullable Integer concurrency;

	/* ---------------- DLT Behavior -------------- */
	/**
	 * Configure a DLT handler method.
	 * @param beanName the bean name.
	 * @param methodName the method name.
	 * @return the builder.
	 * @since 2.8
	 */
	public RetryTopicConfigurationBuilder dltHandlerMethod(String beanName, String methodName) {
		this.dltHandlerMethod = RetryTopicConfigurer.createHandlerMethodWith(beanName, methodName);
		return this;
	}

	/**
	 * Configure the concurrency for the retry and DLT containers.
	 * @param concurrency the concurrency.
	 * @return the builder.
	 * @since 3.0
	 */
	public RetryTopicConfigurationBuilder concurrency(Integer concurrency) {
		this.concurrency = concurrency;
		return this;
	}

	/**
	 * Configure a DLT handler method.
	 * @param endpointHandlerMethod the handler method.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder dltHandlerMethod(EndpointHandlerMethod endpointHandlerMethod) {
		this.dltHandlerMethod = endpointHandlerMethod;
		return this;
	}

	/**
	 * Configure the {@link DltStrategy} to {@link DltStrategy#FAIL_ON_ERROR}.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder doNotRetryOnDltFailure() {
		this.dltStrategy = DltStrategy.FAIL_ON_ERROR;
		return this;
	}

	/**
	 * Configure the {@link DltStrategy}.
	 * @param dltStrategy the strategy.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder dltProcessingFailureStrategy(DltStrategy dltStrategy) {
		this.dltStrategy = dltStrategy;
		return this;
	}

	/**
	 * Configure the {@link DltStrategy} to {@link DltStrategy#NO_DLT}.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder doNotConfigureDlt() {
		this.dltStrategy = DltStrategy.NO_DLT;
		return this;
	}

	/**
	 * Set to false to not start the DLT handler (configured or default); overrides
	 * the container factory's autoStartup property.
	 * @param autoStart false to not auto start.
	 * @return this builder.
	 * @since 2.8
	 */
	public RetryTopicConfigurationBuilder autoStartDltHandler(@Nullable Boolean autoStart) {
		this.autoStartDltHandler = autoStart;
		return this;
	}

	/* ---------------- Configure Topic GateKeeper -------------- */
	/**
	 * Configure the topic names for which to use the target configuration.
	 * @param topicNames the names.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder includeTopics(List<String> topicNames) {
		this.includeTopicNames.addAll(topicNames);
		return this;
	}

	/**
	 * Configure the topic names for which the target configuration will NOT be used.
	 * @param topicNames the names.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder excludeTopics(List<String> topicNames) {
		this.excludeTopicNames.addAll(topicNames);
		return this;
	}

	/**
	 * Configure a topic name for which to use the target configuration.
	 * @param topicName the name.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder includeTopic(String topicName) {
		this.includeTopicNames.add(topicName);
		return this;
	}

	/**
	 * Configure a topic name for which the target configuration will NOT be used.
	 * @param topicName the name.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder excludeTopic(String topicName) {
		this.excludeTopicNames.add(topicName);
		return this;
	}

	/* ---------------- Configure Topic Suffixes -------------- */

	/**
	 * Configure the suffix to add to the retry topics.
	 * @param suffix the suffix.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder retryTopicSuffix(@Nullable String suffix) {
		this.retryTopicSuffix = suffix;
		return this;
	}

	/**
	 * Configure the suffix to add to the DLT topic.
	 * @param suffix the suffix.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder dltSuffix(@Nullable String suffix) {
		this.dltSuffix = suffix;
		return this;
	}

	/**
	 * Configure the retry topic names to be suffixed with ordinal index values.
	 * @return the builder.
	 * @see TopicSuffixingStrategy#SUFFIX_WITH_INDEX_VALUE
	 */
	public RetryTopicConfigurationBuilder suffixTopicsWithIndexValues() {
		this.topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE;
		return this;
	}

	/**
	 * Configure the retry topic name {@link TopicSuffixingStrategy}.
	 * @param topicSuffixingStrategy the strategy.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder setTopicSuffixingStrategy(TopicSuffixingStrategy topicSuffixingStrategy) {
		this.topicSuffixingStrategy = topicSuffixingStrategy;
		return this;
	}

	/**
	 * Configure the {@link SameIntervalTopicReuseStrategy}.
	 *
	 * <p>Note: for fixed backoffs, when this is configured as
	 * {@link SameIntervalTopicReuseStrategy#SINGLE_TOPIC}, it has precedence over
	 * the configuration done through
	 * {@link #useSingleTopicForSameIntervals()}.
	 * @param sameIntervalTopicReuseStrategy the strategy.
	 * @return the builder.
	 * @since 3.0.4
	 */
	public RetryTopicConfigurationBuilder sameIntervalTopicReuseStrategy(SameIntervalTopicReuseStrategy sameIntervalTopicReuseStrategy) {
		this.sameIntervalTopicReuseStrategy = sameIntervalTopicReuseStrategy;
		return this;
	}

	/**
	 * Configure the use of a single retry topic for the attempts that have the same
	 * back off interval (as long as these attempts are in the end of the chain).
	 *
	 * Used for the last retries of exponential backoff (when a {@code maxDelay} is
	 * provided), and to use a single retry topic for fixed backoff.
	 *
	 * @return the builder.
	 * @since 3.0.4
	 * @see SameIntervalTopicReuseStrategy
	 */
	public RetryTopicConfigurationBuilder useSingleTopicForSameIntervals() {
		this.sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC;
		return this;
	}

	/* ---------------- Configure BackOff -------------- */

	/**
	 * Configure the maximum delivery attempts (including the first).
	 * @param maxAttempts the attempts.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder maxAttempts(int maxAttempts) {
		Assert.isTrue(maxAttempts > 0, "Number of attempts should be positive");
		Assert.isTrue(this.maxAttempts == RetryTopicConstants.NOT_SET,
				"You have already set the number of attempts");
		this.maxAttempts = maxAttempts;
		return this;
	}

	/**
	 * Configure a global timeout, in milliseconds, after which a record will go straight
	 * to the DLT the next time a listener throws an exception. Default no timeout.
	 * @param timeout the timeout.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder timeoutAfter(long timeout) {
		this.timeout = timeout;
		return this;
	}

	/**
	 * Configure an {@link ExponentialBackOff}.
	 * @param initialInterval the initial delay interval.
	 * @param multiplier the multiplier.
	 * @param maxInterval the maximum delay interval.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder exponentialBackoff(long initialInterval, double multiplier, long maxInterval) {
		return exponentialBackoff(initialInterval, multiplier, maxInterval, 0);
	}

	/**
	 * Configure an {@link ExponentialBackOff} with a {@linkplain ExponentialBackOff#setJitter(long) jitter value}.
	 * @param initialInterval the initial delay interval.
	 * @param multiplier the multiplier.
	 * @param maxInterval the maximum delay interval.
	 * @param jitter the jitter value.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder exponentialBackoff(long initialInterval, double multiplier, long maxInterval,
			long jitter) {

		Assert.isNull(this.backOff, ALREADY_CONFIGURED);
		Assert.isTrue(initialInterval >= 1, "Initial interval should be >= 1");
		Assert.isTrue(multiplier > 1, "Multiplier should be > 1");
		Assert.isTrue(maxInterval > initialInterval, "Max interval should be > than initial interval");
		ExponentialBackOff exponentialBackOff = new ExponentialBackOff(initialInterval, multiplier);
		exponentialBackOff.setMaxInterval(maxInterval);
		exponentialBackOff.setJitter(jitter);
		this.backOff = exponentialBackOff;
		return this;
	}

	/**
	 * Configure a {@link FixedBackOff}.
	 * @param interval the interval.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder fixedBackOff(long interval) {
		Assert.isNull(this.backOff, ALREADY_CONFIGURED);
		Assert.isTrue(interval >= 1, "Interval should be >= 1");
		this.backOff = new FixedBackOff(interval);
		return this;
	}

	/**
	 * Configure a {@link BackOff} that applies random delay between the specified
	 * minimum interval and maximum interval.
	 * @param minInterval the minimum interval.
	 * @param maxInterval the maximum interval.
	 * @return the builder.
	 * @deprecated since 4.0 in favor of {@link #exponentialBackoff(long, double, long)}
	 */
	@Deprecated(since = "4.0", forRemoval = true)
	public RetryTopicConfigurationBuilder uniformRandomBackoff(long minInterval, long maxInterval) {
		Assert.isNull(this.backOff, ALREADY_CONFIGURED);
		Assert.isTrue(minInterval >= 1, "Min interval should be >= 1");
		Assert.isTrue(maxInterval >= 1, "Max interval should be >= 1");
		Assert.isTrue(maxInterval > minInterval, "Max interval should be > than min interval");
		long jitter = (maxInterval - minInterval) / 2;
		ExponentialBackOff backOff = new ExponentialBackOff();
		backOff.setInitialInterval(minInterval + jitter);
		backOff.setMaxInterval(maxInterval);
		backOff.setJitter(jitter);
		this.backOff = backOff;
		return this;
	}

	/**
	 * Configure a {@link BackOff} that does not apply any delay.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder noBackoff() {
		Assert.isNull(this.backOff, ALREADY_CONFIGURED);
		this.backOff = new FixedBackOff(0);
		return this;
	}

	/**
	 * Configure a custom {@link BackOff}.
	 * @param backOff the backOff
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder customBackoff(BackOff backOff) {
		Assert.isNull(this.backOff, ALREADY_CONFIGURED);
		Assert.notNull(backOff, "You should provide non null custom BackOff");
		this.backOff = backOff;
		return this;
	}

	/**
	 * Configure a {@link FixedBackOff}.
	 * @param interval the interval.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder fixedBackOff(int interval) {
		Assert.isNull(this.backOff, ALREADY_CONFIGURED);
		this.backOff = new FixedBackOff(interval);
		return this;
	}

	/* ---------------- Configure Topics Auto Creation -------------- */

	/**
	 * Configure the topic creation behavior to NOT auto-create topics.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder doNotAutoCreateRetryTopics() {
		this.topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation(false);
		return this;
	}

	/**
	 * Configure the topic creation behavior to auto-create topics with the provided
	 * properties.
	 * @param numPartitions the number of partitions.
	 * @param replicationFactor the replication factor (-1 to use the broker default. If the
	 * broker is earlier than version 2.4, an explicit value is required).
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder autoCreateTopicsWith(@Nullable Integer numPartitions,
			@Nullable Short replicationFactor) {

		this.topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation(true, numPartitions,
				replicationFactor);
		return this;
	}

	/**
	 * Configure the topic creation behavior to optionally create topics with the provided
	 * properties.
	 * @param shouldCreate true to auto create.
	 * @param numPartitions the number of partitions.
	 * @param replicationFactor the replication factor (-1 to use the broker default. If the
	 * broker is earlier than version 2.4, an explicit value is required).
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder autoCreateTopics(@Nullable Boolean shouldCreate,
			@Nullable Integer numPartitions, @Nullable Short replicationFactor) {

		this.topicCreationConfiguration = new RetryTopicConfiguration.TopicCreation(shouldCreate, numPartitions,
				replicationFactor);
		return this;
	}

	/* ---------------- Configure Exception Classifier -------------- */

	/**
	 * Configure the behavior to retry on the provided {@link Throwable}.
	 * @param throwable the throwable.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder retryOn(Class<? extends Throwable> throwable) {
		exceptionEntriesConfigurer(true).entries.add(throwable);
		return this;
	}

	/**
	 * Configure the behavior to NOT retry on the provided {@link Throwable}.
	 * @param throwable the throwable.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder notRetryOn(Class<? extends Throwable> throwable) {
		exceptionEntriesConfigurer(false).entries.add(throwable);
		return this;
	}

	/**
	 * Configure the behavior to retry on the provided {@link Throwable}s.
	 * @param throwables the throwables.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder retryOn(List<Class<? extends Throwable>> throwables) {
		if (!CollectionUtils.isEmpty(throwables)) {
			exceptionEntriesConfigurer(true).entries.addAll(throwables);
		}
		return this;
	}

	/**
	 * Configure the behavior to NOT retry on the provided {@link Throwable}s.
	 * @param throwables the throwables.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder notRetryOn(List<Class<? extends Throwable>> throwables) {
		if (!CollectionUtils.isEmpty(throwables)) {
			exceptionEntriesConfigurer(false).entries.addAll(throwables);
		}
		return this;
	}

	/**
	 * Configure the classifier to traverse the cause chain.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder traversingCauses() {
		return traversingCauses(true);
	}

	/**
	 * Configure the classifier to traverse, or not, the cause chain.
	 * @param traversing true to traverse.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder traversingCauses(boolean traversing) {
		this.traversingCauses = traversing;
		return this;
	}

	private ExceptionEntriesConfigurer exceptionEntriesConfigurer(boolean matchIfFound) {
		if (this.exceptionEntriesConfigurer == null) {
			this.exceptionEntriesConfigurer = new ExceptionEntriesConfigurer(matchIfFound);
		}
		if (this.exceptionEntriesConfigurer.matchIfFound != matchIfFound) {
			throw new IllegalStateException("RetryOn and NotRetryOn cannot be combined");
		}
		return this.exceptionEntriesConfigurer;
	}

	/**
	 * Configure to set DLT routing rules causing the message to be redirected to the custom
	 * DLT when the configured exception has been thrown during message processing.
	 * The cause of the originally thrown exception will be traversed in order to find the
	 * match with the configured exceptions.
	 * @param dltRoutingRules specification of custom DLT name extensions and exceptions which should be matched for them
	 * @return the builder
	 * @since 3.2.0
	 */
	public RetryTopicConfigurationBuilder dltRoutingRules(Map<String, Set<Class<? extends Throwable>>> dltRoutingRules) {
		this.dltRoutingRules.putAll(dltRoutingRules);
		return this;
	}

	/* ---------------- Configure KafkaListenerContainerFactory -------------- */
	/**
	 * Configure the container factory to use.
	 * @param factory the factory.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder listenerFactory(ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
		this.listenerContainerFactory = factory;
		return this;
	}

	/**
	 * Configure the container factory to use via its bean name.
	 * @param factoryBeanName the factory bean name.
	 * @return the builder.
	 */
	public RetryTopicConfigurationBuilder listenerFactory(@Nullable String factoryBeanName) {
		this.listenerContainerFactoryName = factoryBeanName;
		return this;
	}

	/**
	 * Create the {@link RetryTopicConfiguration} with the provided template.
	 * @param sendToTopicKafkaTemplate the template.
	 * @return the configuration.
	 */
	// The templates are configured per ListenerContainerFactory. Only the first configured ones will be used.
	public RetryTopicConfiguration create(KafkaOperations<?, ?> sendToTopicKafkaTemplate) {

		ListenerContainerFactoryResolver.Configuration factoryResolverConfig =
				new ListenerContainerFactoryResolver.Configuration(this.listenerContainerFactory,
						this.listenerContainerFactoryName);

		AllowDenyCollectionManager<String> allowListManager =
				new AllowDenyCollectionManager<>(this.includeTopicNames, this.excludeTopicNames);

		List<Long> backOffValues = new BackOffValuesGenerator(this.maxAttempts, this.backOff).generateValues();

		List<DestinationTopic.Properties> destinationTopicProperties =
				new DestinationTopicPropertiesFactory(this.retryTopicSuffix, this.dltSuffix, backOffValues,
						buildExceptionMatcher(), this.topicCreationConfiguration.getNumPartitions(),
						sendToTopicKafkaTemplate, this.dltStrategy,
					this.topicSuffixingStrategy, this.sameIntervalTopicReuseStrategy, this.timeout, this.dltRoutingRules)
								.autoStartDltHandler(this.autoStartDltHandler)
								.createProperties();
		return new RetryTopicConfiguration(destinationTopicProperties,
				this.dltHandlerMethod, this.topicCreationConfiguration, allowListManager,
				factoryResolverConfig, this.concurrency);
	}

	private ExceptionMatcher buildExceptionMatcher() {
		if (this.exceptionEntriesConfigurer == null) {
			return ExceptionMatcher.forAllowList().add(Throwable.class).build();
		}
		ExceptionMatcher.Builder builder = (this.exceptionEntriesConfigurer.matchIfFound)
				? ExceptionMatcher.forAllowList() : ExceptionMatcher.forDenyList();
		builder.addAll(this.exceptionEntriesConfigurer.entries);
		if (this.traversingCauses != null) {
			builder.traverseCauses(this.traversingCauses);
		}
		return builder.build();
	}

	/**
	 * Create a new instance of the builder.
	 * @return the new instance.
	 */
	public static RetryTopicConfigurationBuilder newInstance() {
		return new RetryTopicConfigurationBuilder();
	}

	private static final class ExceptionEntriesConfigurer {

		private final boolean matchIfFound;

		private final Set<Class<? extends Throwable>> entries = new LinkedHashSet<>();

		private ExceptionEntriesConfigurer(boolean matchIfFound) {
			this.matchIfFound = matchIfFound;
		}

	}

}
