/*
 * Copyright 2019 the original author or authors.
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.config.ContainerProperties;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.2.4
 *
 */
public class ConcurrentMessageListenerContainerMockTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCorrectContainerForConsumerError() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		Consumer consumer = mock(Consumer.class);
		AtomicBoolean first = new AtomicBoolean(true);
		willAnswer(invocation -> {
			if (first.getAndSet(false)) {
				throw new RuntimeException("planned");
			}
			Thread.sleep(100);
			return new ConsumerRecords<>(Collections.emptyMap());
		}).given(consumer)
				.poll(anyLong());

		given(consumerFactory.createConsumer("grp", "", "-0")).willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) record -> { });
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<MessageListenerContainer> errorContainer = new AtomicReference<>();
		containerProperties.setErrorHandler((ContainerAwareErrorHandler) (thrownException, records, consumer1, ec) -> {
			errorContainer.set(ec);
			latch.countDown();
		});
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer<>(consumerFactory,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(errorContainer.get()).isSameAs(container);
		container.stop();
	}

}
