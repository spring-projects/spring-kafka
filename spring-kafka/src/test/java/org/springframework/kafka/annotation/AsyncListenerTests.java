/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.kafka.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import reactor.core.publisher.Mono;

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = {
		AsyncListenerTests.FUTURE_TOPIC_1, AsyncListenerTests.FUTURE_TOPIC_BATCH_1,
		AsyncListenerTests.MONO_TOPIC_1, AsyncListenerTests.MONO_TOPIC_BATCH_1,
		AsyncListenerTests.SEND_TOPIC_1}, partitions = 1)
public class AsyncListenerTests {

	static final String FUTURE_TOPIC_1 = "future-topic-1";

	static final String FUTURE_TOPIC_BATCH_1 = "future-topic-batch-1";

	static final String MONO_TOPIC_1 = "mono-topic-1";

	static final String MONO_TOPIC_BATCH_1 = "mono-topic-batch-1";

	static final String SEND_TOPIC_1 = "send-topic-1";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private Config config;

	@Test
	public void testAsyncListener() throws Exception {

		kafkaTemplate.send(FUTURE_TOPIC_1, "foo-1");
		ConsumerRecord<String, String> cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 0);
		assertThat(cr1.value()).isEqualTo("FOO-1");
		kafkaTemplate.send(FUTURE_TOPIC_1, "bar-1");
		cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 1);
		assertThat(cr1.value()).isEqualTo("bar-1_eh");

		kafkaTemplate.send(FUTURE_TOPIC_BATCH_1, "foo-2");
		cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 2);
		assertThat(cr1.value()).isEqualTo("1");
		kafkaTemplate.send(FUTURE_TOPIC_BATCH_1, "bar-2");
		cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 3);
		assertThat(cr1.value()).isEqualTo("bar-2_beh");

		kafkaTemplate.send(MONO_TOPIC_1, "foo-3");
		cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 4);
		assertThat(cr1.value()).isEqualTo("FOO-3");
		kafkaTemplate.send(MONO_TOPIC_1, "bar-3");
		assertThat(config.latch1.await(10, TimeUnit.SECONDS)).isEqualTo(true);
		cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 5);
		assertThat(cr1.value()).isEqualTo("bar-3_eh");


		kafkaTemplate.send(MONO_TOPIC_BATCH_1, "foo-4");
		cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 6);
		assertThat(cr1.value()).isEqualTo("1");
		kafkaTemplate.send(MONO_TOPIC_BATCH_1, "bar-4");
		assertThat(config.latch2.await(10, TimeUnit.SECONDS)).isEqualTo(true);
		cr1 = kafkaTemplate.receive(SEND_TOPIC_1, 0, 7);
		assertThat(cr1.value()).isEqualTo("bar-4_beh");
	}

	public static class Listener {

		private final AtomicBoolean future1 = new AtomicBoolean(true);

		private final AtomicBoolean futureBatch1 = new AtomicBoolean(true);

		private final AtomicBoolean mono1 = new AtomicBoolean(true);

		private final AtomicBoolean monoBatch1 = new AtomicBoolean(true);

		@KafkaListener(id = "future1", topics = FUTURE_TOPIC_1, errorHandler = "errorHandler")
		@SendTo(SEND_TOPIC_1)
		public CompletableFuture<String> listen1(String foo) {
			CompletableFuture<String> future = new CompletableFuture<>();
			if (future1.getAndSet(false)) {
				future.complete(foo.toUpperCase());
			}
			else {
				future.completeExceptionally(new RuntimeException("Future.exception()"));
			}
			return future;
		}

		@KafkaListener(id = "futureBatch1", topics = FUTURE_TOPIC_BATCH_1, errorHandler = "errorBatchHandler")
		@SendTo(SEND_TOPIC_1)
		public CompletableFuture<String> listen2(List<String> foo) {
			CompletableFuture<String> future = new CompletableFuture<>();
			if (futureBatch1.getAndSet(false)) {
				future.complete(String.valueOf(foo.size()));
			}
			else {
				future.completeExceptionally(new RuntimeException("Future.exception(batch)"));
			}
			return future;
		}

		@KafkaListener(id = "mono1", topics = MONO_TOPIC_1, errorHandler = "errorHandler")
		@SendTo(SEND_TOPIC_1)
		public Mono<String> listen3(String bar) {
			if (mono1.getAndSet(false)) {
				return Mono.just(bar.toUpperCase());
			}
			else {
				return Mono.error(new RuntimeException("Mono.error()"));
			}
		}

		@KafkaListener(id = "monoBatch1", topics = MONO_TOPIC_BATCH_1, errorHandler = "errorBatchHandler")
		@SendTo(SEND_TOPIC_1)
		public Mono<String> listen4(List<String> bar) {
			if (monoBatch1.getAndSet(false)) {
				return Mono.just(String.valueOf(bar.size()));
			}
			else {
				return Mono.error(new RuntimeException("Mono.error(batch)"));
			}
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final CountDownLatch latch1 = new CountDownLatch(2);

		private final CountDownLatch latch2 = new CountDownLatch(2);

		@Autowired
		private KafkaTemplate<String, String> kafkaTemplate;

		@Bean
		public Listener listener() {
			return new Listener();
		}

		@Bean
		public KafkaTemplate<String, String> template(EmbeddedKafkaBroker embeddedKafka) {
			KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory(embeddedKafka));
			template.setConsumerFactory(consumerFactory(embeddedKafka));
			return template;
		}

		@Bean
		public ProducerFactory<String, String> producerFactory(EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaProducerFactory<>(producerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> producerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			return KafkaTestUtils.producerProps(embeddedKafka);
		}

		@Bean
		public KafkaListenerErrorHandler errorHandler() {
			return (message, exception) -> {
				latch1.countDown();
				return message.getPayload() + "_eh";
			};
		}

		@Bean
		public KafkaListenerErrorHandler errorBatchHandler() {
			return (message, exception) -> {
				latch2.countDown();
				return message.getPayload() + "_beh";
			};
		}

		@Bean
		public DefaultKafkaConsumerFactory<String, String> consumerFactory(
				EmbeddedKafkaBroker embeddedKafka) {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs(embeddedKafka));
		}

		@Bean
		public Map<String, Object> consumerConfigs(EmbeddedKafkaBroker embeddedKafka) {
			return KafkaTestUtils.consumerProps("test", "false", embeddedKafka);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaBatchListenerContainerFactory(
				EmbeddedKafkaBroker embeddedKafka) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory(embeddedKafka));
			factory.setBatchListener(true);
			factory.setReplyTemplate(kafkaTemplate);
			return factory;
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				EmbeddedKafkaBroker embeddedKafka) {
			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory(embeddedKafka));
			factory.setReplyTemplate(kafkaTemplate);
			return factory;
		}

	}

}
