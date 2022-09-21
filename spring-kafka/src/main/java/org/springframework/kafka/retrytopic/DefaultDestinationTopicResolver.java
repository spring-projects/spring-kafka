/*
 * Copyright 2018-2022 the original author or authors.
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

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.listener.ExceptionClassifier;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.lang.Nullable;


/**
 *
 * Default implementation of the {@link DestinationTopicResolver} interface.
 * The container is closed when a {@link ContextRefreshedEvent} is received
 * and no more destinations can be added after that.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Yvette Quinby
 * @since 2.7
 *
 */
public class DefaultDestinationTopicResolver extends ExceptionClassifier
		implements DestinationTopicResolver, ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware {

	private static final String NO_OPS_SUFFIX = "-noOps";

	private static final List<Class<? extends Throwable>> FRAMEWORK_EXCEPTIONS =
			Arrays.asList(ListenerExecutionFailedException.class, TimestampedException.class);

	private final Map<String, DestinationTopicHolder> sourceDestinationsHolderMap;

	private final Clock clock;

	private ApplicationContext applicationContext;

	private boolean contextRefreshed;

	@Deprecated(since = "2.9", forRemoval = true) // in 3.1
	public DefaultDestinationTopicResolver(Clock clock, ApplicationContext applicationContext) {
		this(clock);
		this.applicationContext = applicationContext;
	}

	/**
	 * Constructs an instance with the given clock.
	 * @param clock the clock to be used for time-based operations
	 * such as verifying timeouts.
	 * @since 2.9
	 */
	public DefaultDestinationTopicResolver(Clock clock) {
		this.clock = clock;
		this.sourceDestinationsHolderMap = new HashMap<>();
		this.contextRefreshed = false;
	}

	/**
	 * Constructs an instance with a default clock.
	 * @since 2.9
	 */
	public DefaultDestinationTopicResolver() {
		this(Clock.systemUTC());
	}

	@Override
	public DestinationTopic resolveDestinationTopic(String topic, Integer attempt, Exception e,
													long originalTimestamp) {
		DestinationTopicHolder destinationTopicHolder = getDestinationHolderFor(topic);
		return destinationTopicHolder.getSourceDestination().isDltTopic()
				? handleDltProcessingFailure(destinationTopicHolder, e)
				: destinationTopicHolder.getSourceDestination().shouldRetryOn(attempt, maybeUnwrapException(e))
						&& isNotFatalException(e)
						&& !isPastTimout(originalTimestamp, destinationTopicHolder)
					? resolveRetryDestination(destinationTopicHolder)
					: getDltOrNoOpsDestination(topic);
	}

	private Boolean isNotFatalException(Exception e) {
		return getClassifier().classify(e);
	}

	private Throwable maybeUnwrapException(Throwable e) {
		return FRAMEWORK_EXCEPTIONS
				.stream()
				.filter(frameworkException -> frameworkException.isAssignableFrom(e.getClass()))
				.map(frameworkException -> maybeUnwrapException(e.getCause()))
				.findFirst()
				.orElse(e);
	}

	private boolean isPastTimout(long originalTimestamp, DestinationTopicHolder destinationTopicHolder) {
		long timeout = destinationTopicHolder.getNextDestination().getDestinationTimeout();
		return timeout != RetryTopicConstants.NOT_SET &&
				Instant.now(this.clock).toEpochMilli() > originalTimestamp + timeout;
	}

	private DestinationTopic handleDltProcessingFailure(DestinationTopicHolder destinationTopicHolder, Exception e) {
		return destinationTopicHolder.getSourceDestination().isAlwaysRetryOnDltFailure()
				&& isNotFatalException(e)
					? destinationTopicHolder.getSourceDestination()
					: destinationTopicHolder.getNextDestination();
	}

	private DestinationTopic resolveRetryDestination(DestinationTopicHolder destinationTopicHolder) {
		return destinationTopicHolder.getSourceDestination().isSingleTopicRetry()
				? destinationTopicHolder.getSourceDestination()
				: destinationTopicHolder.getNextDestination();
	}

	@Override
	public DestinationTopic getDestinationTopicByName(String topic) {
		return Objects.requireNonNull(this.sourceDestinationsHolderMap.get(topic),
				() -> "No DestinationTopic found for " + topic).getSourceDestination();
	}

	@Nullable
	@Override
	public DestinationTopic getDltFor(String topicName) {
		DestinationTopic destination = getDltOrNoOpsDestination(topicName);
		return destination.isNoOpsTopic()
				? null
				: destination;
	}

	private DestinationTopic getDltOrNoOpsDestination(String topic) {
		DestinationTopic destination = getNextDestinationTopicFor(topic);
		return destination.isDltTopic() || destination.isNoOpsTopic()
				? destination
				: getDltOrNoOpsDestination(destination.getDestinationName());
	}

	@Override
	public DestinationTopic getNextDestinationTopicFor(String topic) {
		return getDestinationHolderFor(topic).getNextDestination();
	}

	private DestinationTopicHolder getDestinationHolderFor(String topic) {
		return this.contextRefreshed
				? doGetDestinationFor(topic)
				: getDestinationTopicSynchronized(topic);
	}

	private DestinationTopicHolder getDestinationTopicSynchronized(String topic) {
		synchronized (this.sourceDestinationsHolderMap) {
			return doGetDestinationFor(topic);
		}
	}

	private DestinationTopicHolder doGetDestinationFor(String topic) {
		return Objects.requireNonNull(this.sourceDestinationsHolderMap.get(topic),
				() -> "No destination found for topic: " + topic);
	}

	@Override
	public void addDestinationTopics(List<DestinationTopic> destinationsToAdd) {
		if (this.contextRefreshed) {
			throw new IllegalStateException("Cannot add new destinations, "
					+ DefaultDestinationTopicResolver.class.getSimpleName() + " is already refreshed.");
		}
		synchronized (this.sourceDestinationsHolderMap) {
			this.sourceDestinationsHolderMap.putAll(correlatePairSourceAndDestinationValues(destinationsToAdd));
		}
	}

	private Map<String, DestinationTopicHolder> correlatePairSourceAndDestinationValues(
			List<DestinationTopic> destinationList) {
		return IntStream
				.range(0, destinationList.size())
				.boxed()
				.collect(Collectors.toMap(index -> destinationList.get(index).getDestinationName(),
						index -> new DestinationTopicHolder(destinationList.get(index), getNextDestinationTopic(destinationList, index))));
	}

	private DestinationTopic getNextDestinationTopic(List<DestinationTopic> destinationList, int index) {
		return index != destinationList.size() - 1
				? destinationList.get(index + 1)
				: new DestinationTopic(destinationList.get(index).getDestinationName() + NO_OPS_SUFFIX,
				destinationList.get(index), NO_OPS_SUFFIX, DestinationTopic.Type.NO_OPS);
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (Objects.equals(event.getApplicationContext(), this.applicationContext)) {
			this.contextRefreshed = true;
		}
	}

	/**
	 * Return true if the application context is refreshed.
	 * @return true if refreshed.
	 * @since 2.7.8
	 */
	public boolean isContextRefreshed() {
		return this.contextRefreshed;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public static class DestinationTopicHolder {

		private final DestinationTopic sourceDestination;

		private final DestinationTopic nextDestination;

		DestinationTopicHolder(DestinationTopic sourceDestination, DestinationTopic nextDestination) {
			this.sourceDestination = sourceDestination;
			this.nextDestination = nextDestination;
		}

		protected DestinationTopic getNextDestination() {
			return this.nextDestination;
		}

		protected DestinationTopic getSourceDestination() {
			return this.sourceDestination;
		}
	}
}
