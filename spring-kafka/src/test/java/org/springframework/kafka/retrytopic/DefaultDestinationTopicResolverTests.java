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
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.retrytopic.DestinationTopic.Type;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * @author Tomaz Fernandes
 * @author Yvette Quinby
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @author Hyunggeol Lee
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class DefaultDestinationTopicResolverTests extends DestinationTopicTests {

	@Mock
	private ApplicationContext applicationContext;

	@Mock
	private ApplicationContext otherApplicationContext;

	private final Clock clock = TestClockUtils.CLOCK;

	private DefaultDestinationTopicResolver defaultDestinationTopicContainer;

	private final long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	@BeforeEach
	public void setup() {

		defaultDestinationTopicContainer = new DefaultDestinationTopicResolver(clock);
		defaultDestinationTopicContainer.setApplicationContext(applicationContext);
		defaultDestinationTopicContainer.addDestinationTopics("id", allFirstDestinationsTopics);
		defaultDestinationTopicContainer.addDestinationTopics("id", allSecondDestinationTopics);
		defaultDestinationTopicContainer.addDestinationTopics("id", allThirdDestinationTopics);
		defaultDestinationTopicContainer.addDestinationTopics("id", allFourthDestinationTopics);
		defaultDestinationTopicContainer.addDestinationTopics("id", allFifthDestinationTopics);

	}

	@Test
	void shouldResolveRetryDestination() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic.getDestinationName(), 1,
					new IllegalStateException(), this.originalTimestamp)).isEqualTo(firstRetryDestinationTopic);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", firstRetryDestinationTopic.getDestinationName(), 1,
					new IllegalStateException(), this.originalTimestamp)).isEqualTo(secondRetryDestinationTopic);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", secondRetryDestinationTopic.getDestinationName(), 1,
					new IllegalStateException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", dltDestinationTopic.getDestinationName(), 1,
					new IllegalStateException(), this.originalTimestamp)).isEqualTo(noOpsDestinationTopic);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(firstRetryDestinationTopic2);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", firstRetryDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(secondRetryDestinationTopic2);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", secondRetryDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic2);
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", dltDestinationTopic2.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic2);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic4.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(singleFixedRetryDestinationTopic4);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", singleFixedRetryDestinationTopic4.getDestinationName(), maxAttempts - 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(singleFixedRetryDestinationTopic4);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", singleFixedRetryDestinationTopic4.getDestinationName(), maxAttempts,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic4);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic5.getDestinationName(), 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(reusableRetryDestinationTopic5);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", reusableRetryDestinationTopic5.getDestinationName(), maxAttempts - 1,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(reusableRetryDestinationTopic5);

		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", reusableRetryDestinationTopic5.getDestinationName(), maxAttempts,
						new IllegalArgumentException(), this.originalTimestamp)).isEqualTo(dltDestinationTopic5);
	}

	@Test
	void shouldResolveDltDestinationForNonRetryableException() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic.getDestinationName(),
						1, new IllegalArgumentException(), originalTimestamp)).isEqualTo(dltDestinationTopic);
	}

	@Test
	void shouldResolveDltDestinationForFatalDefaultException() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic.getDestinationName(),
						1, new ConversionException("Test exception", new RuntimeException()), originalTimestamp))
				.isEqualTo(dltDestinationTopic);
	}

	@Test
	void shouldResolveDeserializationDltDestinationForDeserializationException() {
		DeserializationException exc = new DeserializationException("", new byte[] {}, false, new IllegalStateException());
		TimestampedException timestampedExc = new TimestampedException(exc);

		assertThat(defaultDestinationTopicContainer
			.resolveDestinationTopic("id", secondRetryDestinationTopic.getDestinationName(),
				1, timestampedExc, originalTimestamp)).isEqualTo(deserializationExcDltDestinationTopic);
		assertThat(defaultDestinationTopicContainer
			.resolveDestinationTopic("id", deserializationExcDltDestinationTopic.getDestinationName(),
				1, timestampedExc, originalTimestamp)).isEqualTo(dltDestinationTopic);
	}

	@Test
	void shouldResolveNoOpsForFatalDefaultExceptionInDlt() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", dltDestinationTopic.getDestinationName(),
						1, new ConversionException("Test exception", new RuntimeException()), originalTimestamp))
				.isEqualTo(noOpsDestinationTopic);
	}

	@Test
	void shouldResolveRetryDestinationForWrappedListenerExecutionFailedException() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic.getDestinationName(),
						1, new ListenerExecutionFailedException("Test exception!",
								new RuntimeException()), originalTimestamp)).isEqualTo(firstRetryDestinationTopic);
	}

	@Test
	void shouldResolveRetryDestinationForWrappedTimestampedException() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic.getDestinationName(),
						1, new TimestampedException(new RuntimeException(), Instant.now(this.clock)),
						originalTimestamp))
								.isEqualTo(firstRetryDestinationTopic);
	}

	@Test
	void shouldResolveNoOpsDestinationForDoNotRetryDltPolicy() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", dltDestinationTopic.getDestinationName(),
						1, new IllegalArgumentException(), originalTimestamp)).isEqualTo(noOpsDestinationTopic);
	}

	@Test
	void shouldResolveDltDestinationForAlwaysRetryDltPolicy() {
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", dltDestinationTopic2.getDestinationName(),
						1, new IllegalArgumentException(), originalTimestamp)).isEqualTo(dltDestinationTopic2);
	}

	@Test
	void shouldResolveDltDestinationForExpiredTimeout() {
		long timestampInThePastToForceTimeout = this.originalTimestamp - 10000;
		assertThat(defaultDestinationTopicContainer
				.resolveDestinationTopic("id", mainDestinationTopic2.getDestinationName(),
						1, new IllegalArgumentException(), timestampInThePastToForceTimeout))
								.isEqualTo(dltDestinationTopic2);
	}

	@Test
	void shouldGetDestinationTopic() {
		assertThat(defaultDestinationTopicContainer
				.getDestinationTopicByName("id", mainDestinationTopic.getDestinationName()))
						.isEqualTo(mainDestinationTopic);
	}

	@Test
	void shouldGetNextDestinationTopic() {
		assertThat(defaultDestinationTopicContainer
				.getNextDestinationTopicFor("id", mainDestinationTopic.getDestinationName()))
				.isEqualTo(firstRetryDestinationTopic);
	}

	@Test
	void shouldGetGeneralPurposeDltWhenExceptionIsNotKnown() {
		assertThat(defaultDestinationTopicContainer
				.getDltFor("id", mainDestinationTopic.getDestinationName(), null))
				.isEqualTo(dltDestinationTopic);
	}

	@Test
	void shouldGetGeneralPurposeDltWhenThereIsNoCustomDltRegisteredForExceptionType() {
		assertThat(defaultDestinationTopicContainer
				.getDltFor("id", mainDestinationTopic.getDestinationName(), new RuntimeException()))
				.isEqualTo(dltDestinationTopic);
	}

	@Test
	void shouldGetCustomDltWhenThereIsCustomDltRegisteredForExceptionType() {
		assertThat(defaultDestinationTopicContainer
				.getDltFor("id", mainDestinationTopic.getDestinationName(), new DeserializationException(null, null, false, null)))
				.isEqualTo(deserializationExcDltDestinationTopic);
	}

	@Test
	void shouldThrowIfNoDestinationFound() {
		assertThatNullPointerException().isThrownBy(
				() -> defaultDestinationTopicContainer.resolveDestinationTopic("id", "Non-existing-topic", 0,
						new IllegalArgumentException(), originalTimestamp));
	}

	@Test
	void shouldThrowIfMultipleReusableRetryTopicsAdded() {
		DefaultDestinationTopicResolver destinationResolver = new DefaultDestinationTopicResolver(clock);
		destinationResolver.setApplicationContext(applicationContext);
		destinationResolver.addDestinationTopics("id", allFirstDestinationsTopics);

		List<DestinationTopic> destinationTopics = Arrays
				.asList(mainDestinationTopic5, reusableRetryDestinationTopic5, reusableRetryDestinationTopic5, dltDestinationTopic5);

		assertThatIllegalArgumentException().isThrownBy(
				() -> destinationResolver.addDestinationTopics("id", destinationTopics))
			.withMessageMatching(String.format(".*%s.*last retry topic.*", Type.REUSABLE_RETRY_TOPIC));
	}

	@Test
	void shouldResolveNoOpsIfDltAndNotRetryable() {
		assertThat(defaultDestinationTopicContainer
						.resolveDestinationTopic("id", mainDestinationTopic3.getDestinationName(), 0,
						new RuntimeException(), originalTimestamp)).isEqualTo(noOpsDestinationTopic3);
	}

	@Test
	void shouldThrowIfAddsDestinationsAfterClosed() {
		defaultDestinationTopicContainer
				.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
		assertThatIllegalStateException().isThrownBy(() ->
				defaultDestinationTopicContainer.addDestinationTopics("id", Collections.emptyList()));
	}

	@Test
	void shouldCloseContainerOnContextRefresh() {
		defaultDestinationTopicContainer
				.onApplicationEvent(new ContextRefreshedEvent(applicationContext));
		assertThat(defaultDestinationTopicContainer.isContextRefreshed()).isTrue();
	}

	@Test
	void shouldNotMarkContainerRefreshedOnOtherContextRefresh() {
		defaultDestinationTopicContainer
				.onApplicationEvent(new ContextRefreshedEvent(otherApplicationContext));
		assertThat(defaultDestinationTopicContainer.isContextRefreshed()).isFalse();
	}

	@Test
	void shouldAllowReusableRetryTopicWithSingleDlt() {
		assertThatNoException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", allFifthDestinationTopics));
	}

	@Test
	void shouldAllowReusableRetryTopicWithMultipleDlts() {
		assertThatNoException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", allSixthDestinationTopics));
	}

	@Test
	void shouldAllowReusableRetryTopicAsLastTopic() {
		List<DestinationTopic> topics = Arrays.asList(
				mainDestinationTopic5,
				reusableRetryDestinationTopic5
		);

		assertThatNoException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", topics));
	}

	@Test
	void shouldRejectReusableRetryTopicFollowedByRegularRetry() {
		List<DestinationTopic> topics = Arrays.asList(
				mainDestinationTopic6,
				reusableRetryDestinationTopic6,
				invalidRetryDestinationTopic6,
				dltDestinationTopic6
		);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", topics))
				.withMessageContaining("REUSABLE_RETRY_TOPIC")
				.withMessageContaining("followed only by DLT topics");
	}

	@Test
	void shouldRejectReusableRetryTopicFollowedByNoOps() {
		List<DestinationTopic> topics = Arrays.asList(
				mainDestinationTopic6,
				reusableRetryDestinationTopic6,
				noOpsDestinationTopic6,
				dltDestinationTopic6
		);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", topics))
				.withMessageContaining("REUSABLE_RETRY_TOPIC")
				.withMessageContaining("followed only by DLT topics");
	}

	@Test
	void shouldAllowReusableRetryTopicWithTwoDlts() {
		List<DestinationTopic> topics = Arrays.asList(
				mainDestinationTopic6,
				reusableRetryDestinationTopic6,
				customDltDestinationTopic6,
				dltDestinationTopic6
		);

		assertThatNoException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", topics));
	}

	@Test
	void shouldAllowReusableRetryTopicWithDifferentDltCombinations() {
		List<DestinationTopic> topics = Arrays.asList(
				mainDestinationTopic6,
				reusableRetryDestinationTopic6,
				validationDltDestinationTopic6,
				dltDestinationTopic6
		);

		assertThatNoException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", topics));
	}

	@Test
	void shouldRejectReusableRetryTopicFollowedByMainTopic() {
		List<DestinationTopic> topics = Arrays.asList(
				mainDestinationTopic6,
				reusableRetryDestinationTopic6,
				mainDestinationTopic5
		);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> defaultDestinationTopicContainer
						.addDestinationTopics("id", topics))
				.withMessageContaining("REUSABLE_RETRY_TOPIC");
	}
}
