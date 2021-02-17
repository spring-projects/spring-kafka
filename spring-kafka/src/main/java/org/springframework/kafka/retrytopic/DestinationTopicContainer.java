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

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.listener.ListenerExecutionFailedException;


/**
 *
 * Contains the destination topics and correlates them with their source via the
 * Map&lt;String, {@link org.springframework.kafka.retrytopic.DestinationTopicResolver.DestinationsHolder}&gt; map.
 *
 * Implements the {@link DestinationTopicResolver} interface.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class DestinationTopicContainer implements DestinationTopicResolver, ApplicationListener<ContextRefreshedEvent> {

	private final Map<String, DestinationsHolder> destinationsHolderMap;

	private boolean containerClosed;

	private final Clock clock;

	public DestinationTopicContainer(Clock clock) {
		this.clock = clock;
		this.destinationsHolderMap = new ConcurrentHashMap<>();
		this.containerClosed = false;
	}

	@Override
	public DestinationTopic resolveNextDestination(String topic, Integer attempt, Exception e,
												long originalTimestamp) {
		DestinationsHolder destinationsHolder = getDestinationHolderFor(topic);
		return destinationsHolder.getSourceDestination().isDltTopic()
				? handleDltProcessingFailure(destinationsHolder)
				: destinationsHolder.getSourceDestination().shouldRetryOn(attempt, maybeUnwrapException(e))
						&& !isPastTimout(originalTimestamp, destinationsHolder)
					? resolveRetryDestination(destinationsHolder)
					: resolveDltOrNoOpsDestination(topic);
	}

	private Throwable maybeUnwrapException(Exception e) {
		return ListenerExecutionFailedException.class.isAssignableFrom(e.getClass()) && e.getCause() != null
				? e.getCause()
				: e;
	}

	private boolean isPastTimout(long originalTimestamp, DestinationsHolder destinationsHolder) {
		long timeout = destinationsHolder.getNextDestination().getDestinationTimeout();
		return timeout != RetryTopicConstants.NOT_SET &&
				Instant.now(this.clock).toEpochMilli() > originalTimestamp + timeout;
	}

	private DestinationTopic handleDltProcessingFailure(DestinationsHolder destinationsHolder) {
		return destinationsHolder.getSourceDestination().isAlwaysRetryOnDltFailure()
				? destinationsHolder.getSourceDestination()
				: destinationsHolder.getNextDestination();
	}

	private DestinationTopic resolveRetryDestination(DestinationsHolder destinationsHolder) {
		return destinationsHolder.getSourceDestination().isSingleTopicRetry()
				? destinationsHolder.getSourceDestination()
				: destinationsHolder.getNextDestination();
	}

	@Override
	public long resolveDestinationNextExecutionTimestamp(String topic, Integer attempt, Exception e,
														long originalTimestamp) {
		return Instant.now(this.clock).plusMillis(resolveNextDestination(topic, attempt, e, originalTimestamp)
				.getDestinationDelay()).toEpochMilli();
	}

	@Override
	public DestinationTopic getCurrentTopic(String topic) {
		return getDestinationHolderFor(topic).getSourceDestination();
	}

	private DestinationTopic resolveDltOrNoOpsDestination(String topic) {
		DestinationTopic destination = getDestinationFor(topic);
		return destination.isDltTopic() || destination.isNoOpsTopic()
				? destination
				: resolveDltOrNoOpsDestination(destination.getDestinationName());
	}

	private DestinationTopic getDestinationFor(String topic) {
		return getDestinationHolderFor(topic).getNextDestination();
	}

	private DestinationsHolder getDestinationHolderFor(String topic) {
		return this.containerClosed
				? doGetDestinationFor(topic)
				: getDestinationTopicSynchronized(topic);
	}

	private DestinationsHolder getDestinationTopicSynchronized(String topic) {
		synchronized (this.destinationsHolderMap) {
			return doGetDestinationFor(topic);
		}
	}

	private DestinationsHolder doGetDestinationFor(String topic) {
		return Objects.requireNonNull(this.destinationsHolderMap.get(topic),
				() -> "No destination found for topic: " + topic);
	}

	@Override
	public void addDestinations(Map<String, DestinationTopicResolver.DestinationsHolder> sourceDestinationMapToAdd) {
		if (this.containerClosed) {
			throw new IllegalStateException("Cannot add new destinations, "
					+ DestinationTopicContainer.class.getSimpleName() + " is already closed.");
		}
		synchronized (this.destinationsHolderMap) {
			this.destinationsHolderMap.putAll(sourceDestinationMapToAdd);
		}
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		this.containerClosed = true;
	}
}
