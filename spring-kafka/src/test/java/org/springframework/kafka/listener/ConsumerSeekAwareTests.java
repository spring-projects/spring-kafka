/*
 * Copyright 2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.6
 *
 */
public class ConsumerSeekAwareTests {

	@SuppressWarnings("unchecked")
	@Test
	void beginningEndAndBulkSeekToTimestamp() throws Exception {
		class CSA extends AbstractConsumerSeekAware {
		}
		AbstractConsumerSeekAware csa = new CSA();
		var exec1 = Executors.newSingleThreadExecutor();
		var exec2 = Executors.newSingleThreadExecutor();
		var cb1 = mock(ConsumerSeekCallback.class);
		var cb2  = mock(ConsumerSeekCallback.class);
		var first = new AtomicBoolean(true);
		var map1 = new LinkedHashMap<>(Map.of(new TopicPartition("foo", 0), 0L, new TopicPartition("foo", 1), 0L));
		var map2 = new LinkedHashMap<>(Map.of(new TopicPartition("foo", 2), 0L, new TopicPartition("foo", 3), 0L));
		var register = new Callable<Void>() {

			@Override
			public Void call() {
				if (first.getAndSet(false)) {
					csa.registerSeekCallback(cb1);
					csa.onPartitionsAssigned(map1, null);
				}
				else {
					csa.registerSeekCallback(cb2);
					csa.onPartitionsAssigned(map2, null);
				}
				return null;
			}

		};
		exec1.submit(register).get();
		exec2.submit(register).get();
		csa.seekToBeginning();
		verify(cb1).seekToBeginning(new LinkedList<>(map1.keySet()));
		verify(cb2).seekToBeginning(new LinkedList<>(map2.keySet()));
		csa.seekToEnd();
		verify(cb1).seekToEnd(new LinkedList<>(map1.keySet()));
		verify(cb2).seekToEnd(new LinkedList<>(map2.keySet()));
		csa.seekToTimestamp(42L);
		verify(cb1).seekToTimestamp(new LinkedList<>(map1.keySet()), 42L);
		verify(cb2).seekToTimestamp(new LinkedList<>(map2.keySet()), 42L);
		var revoke1 = new Callable<Void>() {

			@Override
			public Void call() {
				if (!first.getAndSet(true)) {
					csa.onPartitionsRevoked(Collections.singletonList(map1.keySet().iterator().next()));
				}
				else {
					csa.onPartitionsRevoked(Collections.singletonList(map2.keySet().iterator().next()));
				}
				return null;
			}

		};
		exec1.submit(revoke1).get();
		exec2.submit(revoke1).get();
		map1.remove(map1.keySet().iterator().next());
		map2.remove(map2.keySet().iterator().next());
		csa.seekToTimestamp(43L);
		verify(cb1).seekToTimestamp(new LinkedList<>(map1.keySet()), 43L);
		verify(cb2).seekToTimestamp(new LinkedList<>(map2.keySet()), 43L);
		var revoke2 = new Callable<Void>() {

			@Override
			public Void call() {
				if (first.getAndSet(false)) {
					csa.onPartitionsRevoked(Collections.singletonList(map1.keySet().iterator().next()));
				}
				else {
					csa.onPartitionsRevoked(Collections.singletonList(map2.keySet().iterator().next()));
				}
				return null;
			}

		};
		exec1.submit(revoke2).get();
		exec2.submit(revoke2).get();
		assertThat(KafkaTestUtils.getPropertyValue(csa, "callbacks", Map.class)).isEmpty();
		assertThat(KafkaTestUtils.getPropertyValue(csa, "callbacksToTopic", Map.class)).isEmpty();
		var checkTL = new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				csa.unregisterSeekCallback();
				assertThat(KafkaTestUtils.getPropertyValue(csa, "callbackForThread", ThreadLocal.class).get()).isNull();
				return null;
			}

		};
		exec1.submit(checkTL).get();
		exec2.submit(checkTL).get();
		exec1.shutdown();
		exec2.shutdown();
	}

}
