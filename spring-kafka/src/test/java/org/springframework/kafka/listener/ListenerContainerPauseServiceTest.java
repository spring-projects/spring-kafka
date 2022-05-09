/*
 * Copyright 2014-2022 the original author or authors.
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

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit test for {@link ListenerContainerPauseService}.
 *
 * @author Jan Marincek
 * @since 2.9
 */
@ExtendWith(MockitoExtension.class)
class ListenerContainerPauseServiceTest {
	@Mock
	private ListenerContainerRegistry listenerContainerRegistry;

	@InjectMocks
	private ListenerContainerPauseService kafkaPausableListenersService;

	@Test
	@SuppressWarnings("unchecked")
	void testPausingAndResumingListener() throws InterruptedException {
		long timeToBePausedInSec = 2;
		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(KafkaMessageListenerContainer.class);

		given(messageListenerContainer.isPauseRequested()).willReturn(false);
		given(messageListenerContainer.isContainerPaused()).willReturn(true);
		given(messageListenerContainer.getListenerId()).willReturn("test-listener");
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(messageListenerContainer);

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(timeToBePausedInSec));

		then(messageListenerContainer).should(times(1)).pause();

		TimeUnit.SECONDS.sleep(timeToBePausedInSec + 1);

		then(messageListenerContainer).should(times(1)).resume();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testPausingListener() {
		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(KafkaMessageListenerContainer.class);

		given(messageListenerContainer.getListenerId()).willReturn("test-listener");
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(messageListenerContainer);

		kafkaPausableListenersService.pause("test-listener");

		then(messageListenerContainer).should(times(1)).pause();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testPausingNonExistingListener() {
		long timeToBePausedInSec = 2;

		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(KafkaMessageListenerContainer.class);
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(null);

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(timeToBePausedInSec));

		then(messageListenerContainer).should(never()).pause();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testResmingNotPausedListener() throws InterruptedException {
		long timeToBePausedInSec = 2;
		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(KafkaMessageListenerContainer.class);

		given(messageListenerContainer.isPauseRequested()).willReturn(false);
		given(messageListenerContainer.isContainerPaused()).willReturn(false);
		given(messageListenerContainer.getListenerId()).willReturn("test-listener");
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(messageListenerContainer);

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(timeToBePausedInSec));

		then(messageListenerContainer).should(times(1)).pause();

		TimeUnit.SECONDS.sleep(timeToBePausedInSec + 1);

		then(messageListenerContainer).should(never()).resume();
	}

	@Test
	@SuppressWarnings("unchecked")
	void testAlreadyPausedListener() {
		KafkaMessageListenerContainer<Object, Object> messageListenerContainer = mock(KafkaMessageListenerContainer.class);

		given(messageListenerContainer.isPauseRequested()).willReturn(true);
		given(listenerContainerRegistry.getListenerContainer("test-listener")).willReturn(messageListenerContainer);

		kafkaPausableListenersService.pause("test-listener", Duration.ofSeconds(30));

		then(messageListenerContainer).should(never()).pause();
	}
}
