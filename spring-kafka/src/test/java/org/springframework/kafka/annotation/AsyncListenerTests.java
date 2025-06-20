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

package org.springframework.kafka.annotation;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = {
		AsyncListenerTests.FUTURE_TOPIC_1, AsyncListenerTests.FUTURE_TOPIC_BATCH_1,
		AsyncListenerTests.MONO_TOPIC_1, AsyncListenerTests.MONO_TOPIC_BATCH_1,
		AsyncListenerTests.AUTO_DETECT_ASYNC_FUTURE, AsyncListenerTests.AUTO_DETECT_ASYNC_BATCH_FUTURE,
		AsyncListenerTests.AUTO_DETECT_ASYNC_MONO, AsyncListenerTests.AUTO_DETECT_ASYNC_BATCH_MONO,
		AsyncListenerTests.AUTO_DETECT_ASYNC_KAFKA_HANDLER, AsyncListenerTests.SEND_TOPIC_1}, partitions = 1)
public class AsyncListenerTests {

	static final String FUTURE_TOPIC_1 = "future-topic-1";

	static final String FUTURE_TOPIC_BATCH_1 = "future-topic-batch-1";

	static final String MONO_TOPIC_1 = "mono-topic-1";

	static final String MONO_TOPIC_BATCH_1 = "mono-topic-batch-1";

	static final String SEND_TOPIC_1 = "send-topic-1";

	static final String AUTO_DETECT_ASYNC_FUTURE = "auto-detect-async-future";

	static final String AUTO_DETECT_ASYNC_BATCH_FUTURE = "auto-detect-async-batch-future";

	static final String AUTO_DETECT_ASYNC_MONO = "auto-detect-async-mono";

	static final String AUTO_DETECT_ASYNC_BATCH_MONO = "auto-detect-async-batch-mono";

	static final String AUTO_DETECT_ASYNC_KAFKA_HANDLER = "auto-detect-async-kafka-handler";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private Config config;

	@Autowired
	private Listener listener;

	@Autowired
	MultiMethodListener multiMethodListener;

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
		assertThat(cr1.value()).isEqualTo("[bar-2]_beh");

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
		assertThat(cr1.value()).isEqualTo("[bar-4]_beh");
	}

	@Test
	public void testAsyncAcks() throws Exception {

		kafkaTemplate.send(AUTO_DETECT_ASYNC_FUTURE, "baz-future");
		assertThat(this.listener.autoDetectFuture.await(10, TimeUnit.SECONDS)).isTrue();

		kafkaTemplate.send(AUTO_DETECT_ASYNC_BATCH_FUTURE, "baz-batch-future");
		assertThat(this.listener.autoDetectBatchFuture.await(10, TimeUnit.SECONDS)).isTrue();

		kafkaTemplate.send(AUTO_DETECT_ASYNC_MONO, "baz-mono");
		assertThat(this.listener.autoDetectMono.await(10, TimeUnit.SECONDS)).isTrue();

		kafkaTemplate.send(AUTO_DETECT_ASYNC_BATCH_MONO, "baz-batch-mono");
		assertThat(this.listener.autoDetectBatchMono.await(10, TimeUnit.SECONDS)).isTrue();

		kafkaTemplate.send(AUTO_DETECT_ASYNC_KAFKA_HANDLER, "foo-multi-async");
		assertThat(this.multiMethodListener.handler1.await(10, TimeUnit.SECONDS)).isTrue();
	}

	public static class Listener {

		private final AtomicBoolean future1 = new AtomicBoolean(true);

		private final AtomicBoolean futureBatch1 = new AtomicBoolean(true);

		private final AtomicBoolean mono1 = new AtomicBoolean(true);

		private final AtomicBoolean monoBatch1 = new AtomicBoolean(true);

		public final CountDownLatch autoDetectFuture = new CountDownLatch(1);

		public final CountDownLatch autoDetectMono = new CountDownLatch(1);

		public final CountDownLatch autoDetectBatchFuture = new CountDownLatch(1);

		public final CountDownLatch autoDetectBatchMono = new CountDownLatch(1);

		@KafkaListener(id = "future1", topics = FUTURE_TOPIC_1, errorHandler = "errorHandler",
				containerFactory = "kafkaListenerContainerFactory")
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

		@KafkaListener(id = "futureBatch1", topics = FUTURE_TOPIC_BATCH_1, errorHandler = "errorBatchHandler",
				containerFactory = "kafkaBatchListenerContainerFactory")
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

		@KafkaListener(id = "mono1", topics = MONO_TOPIC_1, errorHandler = "errorHandler",
				containerFactory = "kafkaListenerContainerFactory")
		@SendTo(SEND_TOPIC_1)
		public Mono<String> listen3(String bar) {
			if (mono1.getAndSet(false)) {
				return Mono.just(bar.toUpperCase());
			}
			else {
				return Mono.error(new RuntimeException("Mono.error()"));
			}
		}

		@KafkaListener(id = "monoBatch1", topics = MONO_TOPIC_BATCH_1, errorHandler = "errorBatchHandler",
				containerFactory = "kafkaBatchListenerContainerFactory")
		@SendTo(SEND_TOPIC_1)
		public Mono<String> listen4(List<String> bar) {
			if (monoBatch1.getAndSet(false)) {
				return Mono.just(String.valueOf(bar.size()));
			}
			else {
				return Mono.error(new RuntimeException("Mono.error(batch)"));
			}
		}

		@KafkaListener(id = "autoDetectFuture", topics = AUTO_DETECT_ASYNC_FUTURE,
				containerFactory = "kafkaListenerContainerFactory")
		public CompletableFuture<String> listen5(String baz, Acknowledgment acknowledgment) {
			CompletableFuture<String> future = new CompletableFuture<>();
			future.complete(baz.toUpperCase());
			if (acknowledgment.isOutOfOrderCommit()) {
				autoDetectFuture.countDown();
			}
			return future;
		}

		@KafkaListener(id = "autoDetectBatchFuture", topics = AUTO_DETECT_ASYNC_FUTURE,
				containerFactory = "kafkaBatchListenerContainerFactory")
		public CompletableFuture<String> listen6(List<String> baz, Acknowledgment acknowledgment) {
			CompletableFuture<String> future = new CompletableFuture<>();
			future.complete(String.valueOf(baz.size()));
			if (acknowledgment.isOutOfOrderCommit()) {
				autoDetectBatchFuture.countDown();
			}
			return future;
		}

		@KafkaListener(id = "autoDetectMono", topics = AUTO_DETECT_ASYNC_MONO,
				containerFactory = "kafkaListenerContainerFactory")
		public Mono<String> listen7(String qux, Acknowledgment acknowledgment) {
			if (acknowledgment.isOutOfOrderCommit()) {
				autoDetectMono.countDown();
			}
			return Mono.just(qux.toUpperCase());
		}

		@KafkaListener(id = "autoDetectBatchMono", topics = AUTO_DETECT_ASYNC_BATCH_MONO,
				containerFactory = "kafkaBatchListenerContainerFactory")
		public Mono<String> listen8(List<String> qux, Acknowledgment acknowledgment) {
			if (acknowledgment.isOutOfOrderCommit()) {
				autoDetectBatchMono.countDown();
			}
			return Mono.just(String.valueOf(qux.size()));
		}

	}

	@KafkaListener(id = "autoDetectKafkaHandler", topics = AUTO_DETECT_ASYNC_KAFKA_HANDLER,
			containerFactory = "kafkaListenerContainerFactory")
	public static class MultiMethodListener {

		public final CountDownLatch handler1 = new CountDownLatch(1);

		@KafkaHandler
		public Mono<String> handler1(String foo, Acknowledgment acknowledgment) {
			if (acknowledgment.isOutOfOrderCommit()) {
				handler1.countDown();
			}
			return Mono.just(foo);
		}

		@KafkaHandler
		public CompletableFuture<Integer> handler2(Integer bar) {
			CompletableFuture<Integer> future = new CompletableFuture<>();
			future.complete(bar);
			return future;
		}

		@KafkaHandler(isDefault = true)
		public void handler2(Short baz, Acknowledgment acknowledgment) {
			acknowledgment.acknowledge();
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
		public MultiMethodListener multiMethodListener() {
			return new MultiMethodListener();
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
			return KafkaTestUtils.consumerProps(embeddedKafka, "test", false);
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
