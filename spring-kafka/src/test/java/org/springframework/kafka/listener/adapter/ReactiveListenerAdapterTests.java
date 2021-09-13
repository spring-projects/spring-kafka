/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionOffset;

import reactor.core.publisher.Mono;

/**
 * @author Gary Russell
 * @since 2.8
 *
 */
public class ReactiveListenerAdapterTests {

	@Test
	void basic() throws InterruptedException {
		List<ConsumerRecord<String, String>> records = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch latch = new CountDownLatch(2);
		ReactiveListenerAdapter<String, String> adapter = new ReactiveListenerAdapter<>() {

			@Override
			public Mono<ConsumerRecord<String, String>> assemble(Mono<ConsumerRecord<String, String>> mono) {
				return mono
						.doOnNext(rec -> {
							records.add(rec);
							latch.countDown();
						});
			}

		};
		adapter.setParallel(10);
		adapter.start();
		Acknowledgment ack = mock(Acknowledgment.class);
		ConsumerRecord<String, String> rec = new ConsumerRecord<String, String>("foo", 0, 0, 0,
				TimestampType.NO_TIMESTAMP_TYPE, 0, 3, 5, "key", "value");
		adapter.onMessage(rec, ack);
		adapter.onMessage(rec, ack);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(records).hasSize(2);
		verify(ack, times(2)).acknowledge();
	}

	@Test
	void withContainer() throws InterruptedException {
		ConsumerFactory<Integer, String> cf = mock(ConsumerFactory.class);
		Consumer<Integer, String> consumer = mock(Consumer.class);
		given(cf.createConsumer(eq("grp"), eq("clientId"), isNull(), any())).willReturn(consumer);
		final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
		records.put(new TopicPartition("foo", 0), Arrays.asList(
				new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
				new ConsumerRecord<>("foo", 0, 1L, 1, "bar"),
				new ConsumerRecord<>("foo", 0, 2L, 1, "baz"),
				new ConsumerRecord<>("foo", 0, 3L, 1, "qux")));
		ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
		AtomicBoolean first = new AtomicBoolean(true);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			Thread.sleep(50);
			return first.getAndSet(false) ? consumerRecords : ConsumerRecords.empty();
		});
		TopicPartitionOffset topicPartition = new TopicPartitionOffset("foo", 0);
		ContainerProperties containerProps = new ContainerProperties(topicPartition);
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.MANUAL);
		containerProps.setAsyncAcks(true);
		final CountDownLatch latch = new CountDownLatch(4);
		final List<Acknowledgment> acks = new ArrayList<>();

		final CountDownLatch commitLatch = new CountDownLatch(1);

		willAnswer(i -> {
					commitLatch.countDown();
					return null;
				}
		).given(consumer).commitSync(anyMap(), any());

		List<ConsumerRecord<String, String>> received = Collections.synchronizedList(new ArrayList<>());

		ReactiveListenerAdapter<String, String> adapter = new ReactiveListenerAdapter<>() {

			@Override
			public Mono<ConsumerRecord<String, String>> assemble(Mono<ConsumerRecord<String, String>> mono) {
				return mono
						.doOnNext(rec -> {
							received.add(rec);
							latch.countDown();
						});
			}

		};
		adapter.start();

		containerProps.setMessageListener(adapter);
		containerProps.setClientId("clientId");
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(received).hasSize(4);
		verify(consumer, times(1)).commitSync(any(), any());
		verify(consumer).commitSync(Map.of(new TopicPartition("foo", 0), new OffsetAndMetadata(4L)),
				Duration.ofMinutes(1));
		container.stop();
	}

}
