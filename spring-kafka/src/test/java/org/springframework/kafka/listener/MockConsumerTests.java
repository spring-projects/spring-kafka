/*
 * Copyright 2023-present the original author or authors.
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
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.mock.MockConsumerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 3.0.7
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class MockConsumerTests {

	@Autowired
	private Config config;

	@Test
	public void testWithMockConsumer() throws Exception {
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.count).isEqualTo(6);
		assertThat(this.config.contents).containsExactlyInAnyOrder("foo", "bar", "baz", "qux", "fiz", "buz");
	}

	@Configuration(proxyBeanMethods = false)
	@EnableKafka
	public static class Config {

		final List<String> contents = new ArrayList<>();

		final CountDownLatch deliveryLatch = new CountDownLatch(6);

		volatile int count;

		@KafkaListener(topicPartitions =
				@org.springframework.kafka.annotation.TopicPartition(topic = "foo", partitions = "0,1,2"),
				groupId = "grp")
		public void foo(String in) {
			this.contents.add(in);
			this.count++;
			this.deliveryLatch.countDown();
		}

		@Bean
		public ConsumerFactory<String, String> consumerFactory() {
			MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
			TopicPartition topicPartition0 = new TopicPartition("foo", 0);
			TopicPartition topicPartition1 = new TopicPartition("foo", 1);
			TopicPartition topicPartition2 = new TopicPartition("foo", 2);
			List<TopicPartition> topicPartitions = Arrays
					.asList(topicPartition0, topicPartition1, topicPartition2);
			Map<TopicPartition, Long> beginningOffsets = topicPartitions.stream().collect(Collectors
					.toMap(Function.identity(), tp -> 0L));
			consumer.updateBeginningOffsets(beginningOffsets);
			consumer.schedulePollTask(() -> {
				consumer.addRecord(
						new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
								new RecordHeaders(), Optional.empty()));
				consumer.addRecord(
						new ConsumerRecord<>("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "bar",
								new RecordHeaders(), Optional.empty()));
				consumer.addRecord(
						new ConsumerRecord<>("foo", 1, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "baz",
								new RecordHeaders(), Optional.empty()));
				consumer.addRecord(
						new ConsumerRecord<>("foo", 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "qux",
								new RecordHeaders(), Optional.empty()));
				consumer.addRecord(
						new ConsumerRecord<>("foo", 2, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "fiz",
								new RecordHeaders(), Optional.empty()));
				consumer.addRecord(
						new ConsumerRecord<>("foo", 2, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "buz",
								new RecordHeaders(), Optional.empty()));
			});
			return new MockConsumerFactory<>(() -> consumer);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(ConsumerFactory consumerFactory) {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory);
			factory.getContainerProperties().setIdleBetweenPolls(100);
			return factory;
		}

	}

}
