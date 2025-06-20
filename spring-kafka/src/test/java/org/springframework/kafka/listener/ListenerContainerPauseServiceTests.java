/*
 * Copyright 2022-present the original author or authors.
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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.scheduling.TaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit test for {@link ListenerContainerPauseService}.
 *
 * @author Jan Marincek
 * @author Gary Russell
 * @since 2.9
 */
@ExtendWith(MockitoExtension.class)
class ListenerContainerPauseServiceTests {
	@Mock
	private ListenerContainerRegistry listenerContainerRegistry;

	@Mock
	private TaskScheduler scheduler;

	@InjectMocks
	private ListenerContainerPauseService kafkaPausableListenersService;

	@Test
	@SuppressWarnings("unchecked")
	void testPausingAndResumingListener() throws InterruptedException {
		long timeToBePausedInSec = 2;
		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(
				KafkaMessageListenerContainer.class);

		AtomicBoolean paused = new AtomicBoolean();
		willAnswer(inv -> {
			return paused.get();
		}).given(messageListenerContainer).isPauseRequested();
		willAnswer(inv -> {
			paused.set(true);
			return null;
		}).given(messageListenerContainer).pause();
		willAnswer(inv -> {
			paused.set(false);
			return null;
		}).given(messageListenerContainer).resume();

		given(this.listenerContainerRegistry.getListenerContainer("test-listener"))
				.willReturn(messageListenerContainer);
		willAnswer(inv -> {
			Runnable r = inv.getArgument(0);
			r.run();
			return null;
		}).given(this.scheduler).schedule(any(), any(Instant.class));

		long t1 = System.currentTimeMillis();

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(timeToBePausedInSec));

		then(messageListenerContainer).should(times(1)).pause();

		then(messageListenerContainer).should(times(1)).resume();

		ArgumentCaptor<Instant> captor = ArgumentCaptor.forClass(Instant.class);
		verify(this.scheduler).schedule(any(), captor.capture());
		assertThat(captor.getValue().toEpochMilli()).isGreaterThanOrEqualTo(t1 + timeToBePausedInSec * 1000);
	}

	@Test
	@SuppressWarnings("unchecked")
	void testPausingNonExistingListener() {
		long timeToBePausedInSec = 2;

		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(
				KafkaMessageListenerContainer.class);
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(null);

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(timeToBePausedInSec));

		then(messageListenerContainer).should(never()).pause();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testResumingNotPausedListener() throws InterruptedException {
		long timeToBePausedInSec = 2;
		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(
				KafkaMessageListenerContainer.class);

		given(messageListenerContainer.isPauseRequested()).willReturn(false);
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(messageListenerContainer);
		willAnswer(inv -> {
			Runnable r = inv.getArgument(0);
			r.run();
			return null;
		}).given(this.scheduler).schedule(any(), any(Instant.class));

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(timeToBePausedInSec));

		then(messageListenerContainer).should(times(1)).pause();

		then(messageListenerContainer).should(never()).resume();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testAlreadyPausedListener() {
		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(
				KafkaMessageListenerContainer.class);

		given(messageListenerContainer.isPauseRequested()).willReturn(true);
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(messageListenerContainer);

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(30));

		then(messageListenerContainer).should(never()).pause();
	}
}
