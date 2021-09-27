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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.TimestampedException;

/**
 * @author Tomaz Fernandes
 * @author Yvette Quinby
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class DefaultDestinationTopicResolverTests extends DestinationTopicTests {

	private Map<String, DefaultDestinationTopicResolver.DestinationTopicHolder> destinationTopicMap;

	@Mock
	private ApplicationContext applicationContext;

	@Mock
	private ApplicationContext otherApplicationContext;

	private final Clock clock = TestClockUtils.CLOCK;

	private DestinationTopicResolver defaultDestinationTopicContainer;

	private final long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	private final long failureTimestamp = Instant.now(this.clock).plusMillis(500).toEpochMilli();

	private final byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();

	@BeforeEach
	public void setup() {

		defaultDestinationTopicContainer = new DefaultDestinationTopicResolver(clock, applicationContext);
		defaultDestinationTopicContainer.addDestinationTopics(allFirstDestinationsTopics);
		defaultDestinationTopicContainer.addDestinationTopics(allSecondDestinationTopics);
		defaultDestinationTopicContainer.addDestinationTopics(allThirdDestinationTopics);

	}

	@Test
	void shouldResolveRetryDestination() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(mainDestinationTopic.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(firstRetryDestinationTopic);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(firstRetryDestinationTopic.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(secondRetryDestinationTopic);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(secondRetryDestinationTopic.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(dltDestinationTopic.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(noOpsDestinationTopic);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(mainDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(firstRetryDestinationTopic2);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(firstRetryDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(secondRetryDestinationTopic2);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(secondRetryDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic2);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(dltDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic2);
	}

	@Test
	void shouldResolveDltDestinationForNonRetryableException() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(mainDestinationTopic.getDestinationName(),
						1, new RuntimeException(), originalTimestamp)).isEqualTo(dltDestinationTopic);
	}

	@Test
	void shouldResolveRetryDestinationForWrappedListenerExecutionFailedException() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(mainDestinationTopic.getDestinationName(),
						1, new ListenerExecutionFailedException("Test exception!",
								new IllegalArgumentException()), originalTimestamp)).isEqualTo(firstRetryDestinationTopic);
	}

	@Test
	void shouldResolveRetryDestinationForWrappedTimestampedException() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(mainDestinationTopic.getDestinationName(),
						1, new TimestampedException(new IllegalArgumentException()), originalTimestamp))
				.isEqualTo(firstRetryDestinationTopic);
	}

	@Test
	void shouldResolveNoOpsDestinationForDoNotRetryDltPolicy() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(dltDestinationTopic.getDestinationName(),
						1, new IllegalArgumentException(), originalTimestamp)).isEqualTo(noOpsDestinationTopic);
	}

	@Test
	void shouldResolveDltDestinationForAlwaysRetryDltPolicy() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(dltDestinationTopic2.getDestinationName(),
						1, new IllegalArgumentException(), originalTimestamp)).isEqualTo(dltDestinationTopic2);
	}

	@Test
	void shouldResolveDltDestinationForExpiredTimeout() {
		long timestampInThePastToForceTimeout = this.originalTimestamp - 10000;
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic(mainDestinationTopic2.getDestinationName(),
						1, new IllegalArgumentException(), timestampInThePastToForceTimeout)).isEqualTo(dltDestinationTopic2);
	}

	@Test
	void shouldThrowIfNoDestinationFound() {
		assertThatNullPointerException().isThrownBy(() -> defaultDestinationTopicContainer.resolveDestinationTopic("Non-existing-topic", 0,
						new IllegalArgumentException(), originalTimestamp));
	}

	@Test
	void shouldResolveNoOpsIfDltAndNotRetryable() {
		assertThat(defaultDestinationTopicContainer
						.resolveDestinationTopic(mainDestinationTopic3.getDestinationName(), 0,
						new RuntimeException(), originalTimestamp)).isEqualTo(noOpsDestinationTopic3);
	}

	private long getExpectedNextExecutionTime(DestinationTopic destinationTopic) {
		return failureTimestamp + destinationTopic.getDestinationDelay();
	}

	@Test
	void shouldThrowIfAddsDestinationsAfterClosed() {
		((DefaultDestinationTopicResolver) defaultDestinationTopicContainer)
				.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
		assertThatIllegalStateException().isThrownBy(() ->
				defaultDestinationTopicContainer.addDestinationTopics(Collections.emptyList()));
	}

	@Test
	void shouldCloseContainerOnContextRefresh() {
		((DefaultDestinationTopicResolver) defaultDestinationTopicContainer)
				.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
		assertThat(((DefaultDestinationTopicResolver) defaultDestinationTopicContainer).isContextRefreshed()).isTrue();
	}

	@Test
	void shouldNotMarkContainerRefeshedOnOtherContextRefresh() {
		((DefaultDestinationTopicResolver) defaultDestinationTopicContainer)
				.onApplicationEvent(new ContextRefreshedEvent(otherApplicationContext));
		assertThat(((DefaultDestinationTopicResolver) defaultDestinationTopicContainer).isContextRefreshed()).isFalse();
	}

}
