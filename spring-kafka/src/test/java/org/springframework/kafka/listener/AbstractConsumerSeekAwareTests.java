/*
 * Copyright 2024 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractConsumerSeekAwareTests.Config.MultiGroupListener;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Borahm Lee
 * @since 3.3
 */
@DirtiesContext
@SpringJUnitConfig
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EmbeddedKafka(topics = {AbstractConsumerSeekAwareTests.TOPIC}, partitions = 3)
class AbstractConsumerSeekAwareTests {

	static final String TOPIC = "Seek";

	@Autowired
	Config config;

	@Autowired
	KafkaTemplate<String, String> template;

	@Autowired
	MultiGroupListener multiGroupListener;

	@Test
	@Order(1)
	public void checkSizeOfCallbacks() {
		Map<ConsumerSeekCallback, List<TopicPartition>> callbacksAndTopics = multiGroupListener.getCallbacksAndTopics();
		Set<ConsumerSeekCallback> registeredCallbacks = callbacksAndTopics.keySet();
		Map<TopicPartition, List<ConsumerSeekCallback>> topicsAndCallbacks = multiGroupListener.getSeekCallbacks();
		Set<ConsumerSeekCallback> foundCallbacks = topicsAndCallbacks.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
		assertThat(registeredCallbacks).containsExactlyInAnyOrderElementsOf(foundCallbacks);
	}

	@Test
	@Order(2)
	void seekForAllGroups() throws Exception {
		template.send(TOPIC, "test-data");
		template.send(TOPIC, "test-data");
		assertThat(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();

		MultiGroupListener.latch1 = new CountDownLatch(2);
		MultiGroupListener.latch2 = new CountDownLatch(2);

		multiGroupListener.seekToBeginning();
		assertThat(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	@Order(2)
	void seekForSpecificGroup() throws Exception {
		template.send(TOPIC, "test-data");
		template.send(TOPIC, "test-data");
		assertThat(MultiGroupListener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();

		MultiGroupListener.latch1 = new CountDownLatch(2);
		MultiGroupListener.latch2 = new CountDownLatch(2);

		multiGroupListener.seekToBeginningForGroup("group2");
		assertThat(MultiGroupListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(MultiGroupListener.latch1.await(100, TimeUnit.MICROSECONDS)).isFalse();
		assertThat(MultiGroupListener.latch1.getCount()).isEqualTo(2);
	}

	@EnableKafka
	@Configuration
	static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			return factory;
		}

		@Bean
		ConsumerFactory<String, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(KafkaTestUtils.consumerProps("test-group", "false", this.broker));
		}

		@Bean
		ProducerFactory<String, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(this.broker));
		}

		@Bean
		KafkaTemplate<String, String> template(ProducerFactory<String, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Component
		static class MultiGroupListener extends AbstractConsumerSeekAware {

			static CountDownLatch latch1 = new CountDownLatch(2);

			static CountDownLatch latch2 = new CountDownLatch(2);

			@KafkaListener(id = "group1", groupId = "group1", topics = TOPIC, concurrency = "2")
			void listenForGroup1(String in) {
				latch1.countDown();
			}

			@KafkaListener(id = "group2", groupId = "group2", topics = TOPIC, concurrency = "2")
			void listenForGroup2(String in) {
				latch2.countDown();
			}

			void seekToBeginningForGroup(String groupIdForSeek) {
				getCallbacksAndTopics().forEach((cb, topics) -> {
					if (groupIdForSeek.equals(cb.getGroupId())) {
						topics.forEach(tp -> cb.seekToBeginning(tp.topic(), tp.partition()));
					}
				});
			}
		}
	}

}
