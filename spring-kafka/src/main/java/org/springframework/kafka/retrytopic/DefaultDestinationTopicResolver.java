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

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.listener.ExceptionClassifier;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.retrytopic.DestinationTopic.Type;
import org.springframework.util.Assert;

/**
 *
 * Default implementation of the {@link DestinationTopicResolver} interface.
 * The container is closed when a {@link ContextRefreshedEvent} is received
 * and no more destinations can be added after that.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Yvette Quinby
 * @author Adrian Chlebosz
 * @author Omer Celik
 * @author Hyunggeol Lee
 * @since 2.7
 *
 */
public class DefaultDestinationTopicResolver extends ExceptionClassifier
		implements DestinationTopicResolver, ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware {

	private static final String NO_OPS_SUFFIX = "-noOps";

	private static final List<Class<? extends Throwable>> FRAMEWORK_EXCEPTIONS =
			Arrays.asList(ListenerExecutionFailedException.class, TimestampedException.class);

	private final Map<String, Map<String, DestinationTopicHolder>> sourceDestinationsHolderMap;

	private final Lock sourceDestinationsHolderLock = new ReentrantLock();

	private final Clock clock;

	@SuppressWarnings("NullAway.Init")
	private ApplicationContext applicationContext;

	private boolean contextRefreshed;

	/**
	 * Constructs an instance with the given clock.
	 * @param clock the clock to be used for time-based operations
	 * such as verifying timeouts.
	 * @since 2.9
	 */
	public DefaultDestinationTopicResolver(Clock clock) {
		this.clock = clock;
		this.sourceDestinationsHolderMap = new ConcurrentHashMap<>();
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
	public DestinationTopic resolveDestinationTopic(String mainListenerId, String topic, Integer attempt, Exception e,
													long originalTimestamp) {
		DestinationTopicHolder destinationTopicHolder = getDestinationHolderFor(mainListenerId, topic);
		return destinationTopicHolder.getSourceDestination().isDltTopic()
				? handleDltProcessingFailure(destinationTopicHolder, e)
				: destinationTopicHolder.getSourceDestination().shouldRetryOn(attempt, maybeUnwrapException(e))
						&& isNotFatalException(e)
						&& !isPastTimeout(originalTimestamp, destinationTopicHolder)
					? resolveRetryDestination(mainListenerId, destinationTopicHolder, e)
					: getDltOrNoOpsDestination(mainListenerId, topic, e);
	}

	private Boolean isNotFatalException(Exception e) {
		return getExceptionMatcher().match(e);
	}

	private Throwable maybeUnwrapException(@Nullable Throwable e) {
		Assert.state(e != null, "Exception cannot be null");
		return FRAMEWORK_EXCEPTIONS
				.stream()
				.filter(frameworkException -> frameworkException.isAssignableFrom(e.getClass()))
				.map(frameworkException -> maybeUnwrapException(e.getCause()))
				.findFirst()
				.orElse(e);
	}

	private boolean isPastTimeout(long originalTimestamp, DestinationTopicHolder destinationTopicHolder) {
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

	@SuppressWarnings("deprecation")
	private DestinationTopic resolveRetryDestination(String mainListenerId, DestinationTopicHolder destinationTopicHolder, Exception e) {
		if (destinationTopicHolder.getSourceDestination().isReusableRetryTopic()) {
			return destinationTopicHolder.getSourceDestination();
		}

		if (isAlreadyDltDestination(destinationTopicHolder)) {
			return getDltOrNoOpsDestination(mainListenerId, destinationTopicHolder.getSourceDestination().getDestinationName(), e);
		}

		return destinationTopicHolder.getNextDestination();
	}

	private static boolean isAlreadyDltDestination(DestinationTopicHolder destinationTopicHolder) {
		return destinationTopicHolder.getNextDestination().isDltTopic();
	}

	@Override
	public DestinationTopic getDestinationTopicByName(String mainListenerId, String topic) {
		Map<String, DestinationTopicHolder> map = this.sourceDestinationsHolderMap.get(mainListenerId);
		Assert.notNull(map, () -> "No destination resolution information for listener " + mainListenerId);
		return Objects.requireNonNull(map.get(topic),
				() -> "No DestinationTopic found for " + mainListenerId + ":" + topic).getSourceDestination();
	}

	@Nullable
	@Override
	public DestinationTopic getDltFor(String mainListenerId, String topicName, @Nullable Exception e) {
		DestinationTopic destination = getDltOrNoOpsDestination(mainListenerId, topicName, e);
		return destination.isNoOpsTopic()
				? null
				: destination;
	}

	private DestinationTopic getDltOrNoOpsDestination(String mainListenerId, @Nullable String topic, @Nullable Exception e) {
		DestinationTopic destination = getNextDestinationTopicFor(mainListenerId, topic);
		return isMatchingDltTopic(destination, e) || destination.isNoOpsTopic() ?
			destination :
			getDltOrNoOpsDestination(mainListenerId, destination.getDestinationName(), e);
	}

	private static boolean isMatchingDltTopic(DestinationTopic destination, @Nullable Exception e) {
		if (!destination.isDltTopic()) {
			return false;
		}

		boolean isDltIntendedForCurrentExc = destination.usedForExceptions().stream()
			.anyMatch(excType -> isDirectExcOrCause(e, excType));
		boolean isGenericPurposeDlt = destination.usedForExceptions().isEmpty();
		return isDltIntendedForCurrentExc || isGenericPurposeDlt;
	}

	private static boolean isDirectExcOrCause(@Nullable Exception e, Class<? extends Throwable> excType) {
		if (e == null) {
			return false;
		}

		Throwable toMatch = e;

		boolean isMatched = excType.isInstance(toMatch);
		while (!isMatched) {
			toMatch = toMatch.getCause();
			if (toMatch == null) {
				return false;
			}
			isMatched = excType.isInstance(toMatch);
		}

		return isMatched;
	}

	@Override
	public DestinationTopic getNextDestinationTopicFor(String mainListenerId, @Nullable String topic) {
		return getDestinationHolderFor(mainListenerId, topic).getNextDestination();
	}

	private DestinationTopicHolder getDestinationHolderFor(String mainListenerId, @Nullable String topic) {
		return this.contextRefreshed
				? doGetDestinationFor(mainListenerId, topic)
				: getDestinationTopicSynchronized(mainListenerId, topic);
	}

	private DestinationTopicHolder getDestinationTopicSynchronized(String mainListenerId, @Nullable String topic) {
		try {
			this.sourceDestinationsHolderLock.lock();
			return doGetDestinationFor(mainListenerId, topic);
		}
		finally {
			this.sourceDestinationsHolderLock.unlock();
		}
	}

	private DestinationTopicHolder doGetDestinationFor(String mainListenerId, @Nullable String topic) {
		Map<String, DestinationTopicHolder> map = this.sourceDestinationsHolderMap.get(mainListenerId);
		Assert.notNull(map, () -> "No destination resolution information for listener " + mainListenerId);
		return Objects.requireNonNull(map.get(topic),
				() -> "No destination found for topic: " + topic);
	}

	@Override
	public void addDestinationTopics(String mainListenerId, List<DestinationTopic> destinationsToAdd) {
		if (this.contextRefreshed) {
			throw new IllegalStateException("Cannot add new destinations, "
					+ DefaultDestinationTopicResolver.class.getSimpleName() + " is already refreshed.");
		}
		validateDestinations(destinationsToAdd);
		try {
			this.sourceDestinationsHolderLock.lock();
			Map<String, DestinationTopicHolder> map = this.sourceDestinationsHolderMap.computeIfAbsent(mainListenerId,
					id -> new HashMap<>());
			map.putAll(correlatePairSourceAndDestinationValues(destinationsToAdd));
		}
		finally {
			this.sourceDestinationsHolderLock.unlock();
		}
	}

	private void validateDestinations(List<DestinationTopic> destinationsToAdd) {
		for (int i = 0; i < destinationsToAdd.size(); i++) {
			DestinationTopic destination = destinationsToAdd.get(i);
			if (destination.isReusableRetryTopic()) {
				// Allow multiple DLTs after REUSABLE_RETRY_TOPIC
				boolean isLastOrFollowedOnlyByDlts = (i == destinationsToAdd.size() - 1) ||
						destinationsToAdd.subList(i + 1, destinationsToAdd.size())
								.stream()
								.allMatch(DestinationTopic::isDltTopic);

				Assert.isTrue(isLastOrFollowedOnlyByDlts,
						() -> String.format("In the destination topic chain, the type %s can only be " +
										"specified as the last retry topic (followed only by DLT topics).",
								Type.REUSABLE_RETRY_TOPIC));
			}
		}
	}

	private Map<String, DestinationTopicHolder> correlatePairSourceAndDestinationValues(
			List<DestinationTopic> destinationList) {
		return IntStream
				.range(0, destinationList.size())
				.boxed()
				.collect(Collectors.toMap(index -> destinationList.get(index).getDestinationName(),
						index -> new DestinationTopicHolder(destinationList.get(index),
								getNextDestinationTopic(destinationList, index))));
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
