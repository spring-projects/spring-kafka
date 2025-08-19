/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.ExceptionMatcher;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * @author Antonio Tomac
 * @since 3.0.9
 *
 */
class ErrorHandlingUtilsTest {

	private final Exception thrownException = new RuntimeException("initial cause");

	private final Consumer<?, ?> consumer = mock(Consumer.class);

	private final MessageListenerContainer container = mock(MessageListenerContainer.class);

	private final Runnable listener = mock(Runnable.class);

	private final BackOff backOff = new FixedBackOff(1000, 3);

	private final CommonErrorHandler seeker = mock(CommonErrorHandler.class);

	@SuppressWarnings("unchecked")
	private final BiConsumer<ConsumerRecords<?, ?>, Exception> recoverer = mock(BiConsumer.class);

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(ErrorHandlingUtilsTest.class));

	private final List<RetryListener> retryListeners = new ArrayList<>();

	private final ExceptionMatcher exceptionMatcher = ExceptionMatcher.defaultMatcher();

	private final ConsumerRecords<?, ?> consumerRecords = recordsOf(
			new ConsumerRecord<>("foo", 0, 0L, "a", "a"),
			new ConsumerRecord<>("foo", 1, 0L, "b", "b")
	);

	@SafeVarargs
	@SuppressWarnings("varargs")
	private <K, V> ConsumerRecords<K, V> recordsOf(ConsumerRecord<K, V>... records) {
		return new ConsumerRecords<>(
				Arrays.stream(records).collect(Collectors.groupingBy(
						(cr) -> new TopicPartition(cr.topic(), cr.partition())
				)), Map.of());
	}

	@BeforeEach
	public void resetMocks() {
		reset(consumer, container, listener, seeker, recoverer);
		willReturn(true).given(container).isRunning();
		willReturn(false).given(container).isPauseRequested();
	}

	private void doRetries() {
		ErrorHandlingUtils.retryBatch(
				thrownException, consumerRecords, consumer, container, listener, backOff,
				seeker, recoverer, logger, KafkaException.Level.INFO, retryListeners,
				exceptionMatcher, true
		);
	}

	private long execDurationOf(Runnable runnable) {
		long start = System.currentTimeMillis();
		runnable.run();
		long end = System.currentTimeMillis();
		return end - start;
	}

	@Test
	void testStopRetriesWhenNotRunning() {
		willReturn(false).given(container).isRunning();
		assertThatThrownBy(this::doRetries)
				.isInstanceOf(KafkaException.class)
				.message().isEqualTo("Container stopped during retries");
		verifyNoInteractions(seeker, listener, recoverer);
	}

	@Test
	void testOneSuccessfulRetry() {
		long duration = execDurationOf(this::doRetries);
		assertThat(duration).as("duration of one round of sleep").isGreaterThanOrEqualTo(1000L);
		verifyNoInteractions(seeker, recoverer);
		verify(listener, times(1)).run();
		verifyNoInteractions(seeker, recoverer);
	}

	@Test
	void stopRetriesWhenContainerIsPaused() {
		willReturn(true).given(container).isPauseRequested();
		long duration = execDurationOf(() ->
				assertThatThrownBy(this::doRetries)
						.isInstanceOf(KafkaException.class)
						.message().isEqualTo("Container paused requested during retries")
		);
		assertThat(duration)
				.as("duration should not be full retry interval")
				.isLessThan(1000L);
		verify(seeker).handleBatch(thrownException, consumerRecords, consumer, container, ErrorHandlingUtils.NO_OP);
		verifyNoInteractions(listener, recoverer);
	}

	@Test
	void stopRetriesWhenPartitionIsPaused() {
		willReturn(true).given(container).isPartitionPauseRequested(new TopicPartition("foo", 1));
		long duration = execDurationOf(() ->
				assertThatThrownBy(this::doRetries)
						.isInstanceOf(KafkaException.class)
						.message().isEqualTo("Container paused requested during retries")
		);
		assertThat(duration)
				.as("duration should not be full retry interval")
				.isLessThan(1000L);
		verify(seeker).handleBatch(thrownException, consumerRecords, consumer, container, ErrorHandlingUtils.NO_OP);
		verifyNoInteractions(listener, recoverer);
	}
}
