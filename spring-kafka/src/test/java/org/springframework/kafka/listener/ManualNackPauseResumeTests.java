/*
 * Copyright 2017-2021 the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class ManualNackPauseResumeTests {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;
	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@SuppressWarnings({ "unchecked" })
	@Test
	public void dontResumeAlreadyPaused() throws Exception {
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.replayTime).isBetween(50L, 30_000L);
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pausedForNack).hasSize(1);
		assertThat(this.config.resumedForNack).hasSize(1);
		assertThat(this.config.pausedForNack).contains(new TopicPartition("foo", 1));
		assertThat(this.config.resumedForNack).contains(new TopicPartition("foo", 1));
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final List<String> contents = new ArrayList<>();

		final CountDownLatch pollLatch = new CountDownLatch(3);

		final CountDownLatch deliveryLatch = new CountDownLatch(7);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		final CountDownLatch commitLatch = new CountDownLatch(2);

		final Set<TopicPartition> pausedForNack = new HashSet<>();

		final Set<TopicPartition> resumedForNack = new HashSet<>();

		volatile int count;

		volatile long replayTime;

		@KafkaListener(id = "foo", topics = "foo", groupId = "grp")
		public void foo(String in, Acknowledgment ack) {
			this.contents.add(in);
			if (in.equals("qux")) {
				this.replayTime = System.currentTimeMillis() - this.replayTime;
			}
			this.deliveryLatch.countDown();
			if (++this.count == 4) { // part 1, offset 1, first time
				ack.nack(50);
			}
			else {
				ack.acknowledge();
			}
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer();
			given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public Consumer consumer() {
			final Consumer consumer = mock(Consumer.class);
			final TopicPartition topicPartition0 = new TopicPartition("foo", 0);
			final TopicPartition topicPartition1 = new TopicPartition("foo", 1);
			final TopicPartition topicPartition2 = new TopicPartition("foo", 2);
			willAnswer(i -> {
				((ConsumerRebalanceListener) i.getArgument(1)).onPartitionsAssigned(
						Collections.singletonList(topicPartition1));
				return null;
			}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
			Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
			records1.put(topicPartition0, Arrays.asList(
					new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "bar",
							new RecordHeaders(), Optional.empty())));
			records1.put(topicPartition1, Arrays.asList(
					new ConsumerRecord("foo", 1, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "baz",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord("foo", 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "qux",
							new RecordHeaders(), Optional.empty())));
			records1.put(topicPartition2, Arrays.asList(
					new ConsumerRecord("foo", 2, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "fiz",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord("foo", 2, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "buz",
							new RecordHeaders(), Optional.empty())));
			Map<TopicPartition, List<ConsumerRecord>> records2 = new LinkedHashMap<>(records1);
			records2.remove(topicPartition0);
			records2.put(topicPartition1, Arrays.asList(
					new ConsumerRecord("foo", 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "qux",
							new RecordHeaders(), Optional.empty())));
			final AtomicInteger which = new AtomicInteger();
			final AtomicBoolean paused = new AtomicBoolean();
			willAnswer(i -> {
				if (paused.get()) {
					Thread.sleep(10);
					return ConsumerRecords.empty();
				}
				this.pollLatch.countDown();
				switch (which.getAndIncrement()) {
					case 0:
						return new ConsumerRecords(records1);
					case 1:
						return new ConsumerRecords(records2);
					default:
						try {
							Thread.sleep(1000);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						return new ConsumerRecords(Collections.emptyMap());
				}
			}).given(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
			willAnswer(i -> {
				return Set.of(topicPartition0, topicPartition2);
			}).given(consumer).paused();
			willAnswer(i -> {
				paused.set(true);
				this.pausedForNack.addAll(i.getArgument(0));
				return null;
			}).given(consumer).pause(any());
			willAnswer(i -> {
				paused.set(false);
				this.resumedForNack.addAll(i.getArgument(0));
				return null;
			}).given(consumer).resume(any());
			willAnswer(i -> {
				this.commitLatch.countDown();
				return null;
			}).given(consumer).commitSync(anyMap(), any());
			willAnswer(i -> {
				this.closeLatch.countDown();
				return null;
			}).given(consumer).close();
			return consumer;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.getContainerProperties().setAckMode(AckMode.MANUAL);
			factory.getContainerProperties().setMissingTopicsFatal(false);
			return factory;
		}

	}

}
