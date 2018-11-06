/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.event.ContainerStoppedEvent;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

/**
 * @author Gary Russell
 * @since 2.1.11
 *
 */
public class TestOOMError {

	@SuppressWarnings("unchecked")
	@Test
	public void testOOMCMLC() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), eq("-0"))).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			return consumerRecords;
		});
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.RECORD);
		MessageListener<Integer, String> messageListener = cr -> {
			@SuppressWarnings("unused")
			byte[] bytes = new byte[Integer.MAX_VALUE];
		};
		containerProps.setMessageListener(messageListener);
		containerProps.setClientId("clientId");
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ContainerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isRunning()).isFalse();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testOOMKMLC() throws Exception {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull())).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
		ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			return consumerRecords;
		});
		TopicPartitionInitialOffset[] topicPartition = new TopicPartitionInitialOffset[] {
				new TopicPartitionInitialOffset("foo", 0) };
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.RECORD);
		MessageListener<Integer, String> messageListener = cr -> {
			@SuppressWarnings("unused")
			byte[] bytes = new byte[Integer.MAX_VALUE];
		};
		containerProps.setMessageListener(messageListener);
		containerProps.setClientId("clientId");
		containerProps.setShutdownTimeout(100);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ContainerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isRunning()).isFalse();
	}

}
