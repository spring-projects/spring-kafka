/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests for a listener error handler that returns {@code null} on an async listener.
 *
 * @author WooSeung Kim
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "anehFuture", "anehLater", "anehMono", "anehThrow", "anehBatch", "anehSendTo",
		"anehReply", "anehMixedAsync", "anehMixedBlocking", "anehManual", "anehBlockingManual", "anehInFlight" })
class AsyncNullErrorHandlerAckTests {

	@Autowired
	private KafkaTemplate<String, Object> template;

	@Autowired
	private EmbeddedKafkaBroker broker;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Autowired
	private Listener listener;

	@Autowired
	private ConsumerFactory<String, Object> consumerFactory;

	@Test
	void futureFailureHandledWithNullCommitsAndTheNextRecordArrives() throws Exception {
		deliverTwice("anehFuture", this.listener.future);
	}

	@Test
	void laterFutureFailureHandledWithNullCommitsAndTheNextRecordArrives() throws Exception {
		deliverTwice("anehLater", this.listener.later);
	}

	@Test
	void monoFailureHandledWithNullCommitsAndTheNextRecordArrives() throws Exception {
		deliverTwice("anehMono", this.listener.mono);
	}

	@Test
	void failureBeforeTheFutureHandledWithNullCommits() throws Exception {
		deliverTwice("anehThrow", this.listener.thrown);
	}

	@Test
	void batchFailureHandledWithNullCommits() throws Exception {
		deliverTwice("anehBatch", this.listener.batch);
	}

	@Test
	void sendToFailureHandledWithNullCommitsWithoutAReply() throws Exception {
		deliverTwice("anehSendTo", this.listener.sendTo);
		try (Consumer<String, Object> consumer = this.consumerFactory.createConsumer("anehReplyCheck", null)) {
			assertThat(KafkaTestUtils.getEndOffsets(consumer, "anehReply", 0)).containsValue(0L);
		}
	}

	@Test
	void asyncHandlerFailureOnMixedListenerCommits() throws Exception {
		send("anehMixedAsync", "text");
		awaitCommitted("anehMixedAsync", 1L);
	}

	@Test
	void blockingHandlerFailureOnMixedListenerCommits() throws Exception {
		send("anehMixedBlocking", 7);
		awaitCommitted("anehMixedBlocking", 1L);
	}

	@Test
	void manualAckErrorHandlerReturningNullCommitsOnlyWhenItAcknowledges() throws Exception {
		send("anehManual", "one");
		await().atMost(Duration.ofSeconds(20)).until(() -> this.listener.manualAck.get() != null);
		awaitNotCommitted("anehManual");
		this.listener.manualAck.get().acknowledge();
		awaitCommitted("anehManual", 1L);
	}

	@Test
	void blockingManualListenerIsNotAcknowledged() throws Exception {
		send("anehBlockingManual", "one");
		await().atMost(Duration.ofSeconds(20)).until(() -> this.listener.blockingManual.get() == 1);
		awaitNotCommitted("anehBlockingManual");
	}

	@Test
	void earlierRecordStillInFlightHoldsTheCommit() throws Exception {
		MessageListenerContainer container = this.registry.getListenerContainer("anehInFlight");
		assertThat(container).isNotNull();
		send("anehInFlight", "first");
		send("anehInFlight", "second");
		container.start();
		await().atMost(Duration.ofSeconds(20)).until(() -> this.listener.inFlight.get() == 2);
		awaitNotCommitted("anehInFlight");
		this.listener.held.complete("done");
		awaitCommitted("anehInFlight", 2L);
	}

	private void deliverTwice(String topic, AtomicInteger calls) throws Exception {
		send(topic, "one");
		awaitCommitted(topic, 1L);
		send(topic, "two");
		awaitCommitted(topic, 2L);
		assertThat(calls.get()).isEqualTo(2);
	}

	private void send(String topic, Object value) throws Exception {
		this.template.send(topic, value).get(10, TimeUnit.SECONDS);
	}

	private void awaitCommitted(String topic, long offset) {
		await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(committed(topic)).isEqualTo(offset));
		assertThat(this.listener.recovered).hasValue(0);
	}

	private void awaitNotCommitted(String topic) {
		await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> committed(topic) == null);
	}

	private Long committed(String topic) throws Exception {
		var current = KafkaTestUtils.getCurrentOffset(this.broker.getBrokersAsString(), topic, topic, 0);
		return current != null ? current.offset() : null;
	}

	static CompletableFuture<String> failed() {
		return CompletableFuture.failedFuture(new IllegalStateException("test"));
	}

	public static class Listener {

		final AtomicInteger future = new AtomicInteger();

		final AtomicInteger later = new AtomicInteger();

		final AtomicInteger mono = new AtomicInteger();

		final AtomicInteger thrown = new AtomicInteger();

		final AtomicInteger batch = new AtomicInteger();

		final AtomicInteger sendTo = new AtomicInteger();

		final AtomicInteger blockingManual = new AtomicInteger();

		final AtomicInteger inFlight = new AtomicInteger();

		final AtomicInteger recovered = new AtomicInteger();

		final AtomicReference<Acknowledgment> manualAck = new AtomicReference<>();

		final CompletableFuture<String> held = new CompletableFuture<>();

		@KafkaListener(id = "anehFuture", topics = "anehFuture", errorHandler = "nullErrorHandler")
		CompletableFuture<String> future(String in) {
			this.future.incrementAndGet();
			return failed();
		}

		@KafkaListener(id = "anehLater", topics = "anehLater", errorHandler = "nullErrorHandler")
		CompletableFuture<String> later(String in) {
			this.later.incrementAndGet();
			return CompletableFuture.supplyAsync(() -> {
				throw new IllegalStateException("test");
			}, CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS));
		}

		@KafkaListener(id = "anehMono", topics = "anehMono", errorHandler = "nullErrorHandler")
		Mono<String> mono(String in) {
			this.mono.incrementAndGet();
			return Mono.error(new IllegalStateException("test"));
		}

		@KafkaListener(id = "anehThrow", topics = "anehThrow", errorHandler = "nullErrorHandler")
		CompletableFuture<String> thrown(String in) {
			this.thrown.incrementAndGet();
			throw new IllegalStateException("test");
		}

		@KafkaListener(id = "anehBatch", topics = "anehBatch", errorHandler = "nullErrorHandler",
				batch = "true")
		CompletableFuture<String> batch(List<String> in) {
			this.batch.incrementAndGet();
			return failed();
		}

		@KafkaListener(id = "anehSendTo", topics = "anehSendTo", errorHandler = "nullErrorHandler")
		@SendTo("anehReply")
		CompletableFuture<String> sendTo(String in) {
			this.sendTo.incrementAndGet();
			return failed();
		}

		@KafkaListener(id = "anehManual", topics = "anehManual", errorHandler = "manualNullErrorHandler")
		CompletableFuture<String> manual(String in) {
			return failed();
		}

		@KafkaListener(id = "anehBlockingManual", topics = "anehBlockingManual",
				containerFactory = "manualFactory", errorHandler = "nullErrorHandler")
		String blockingManual(String in) {
			this.blockingManual.incrementAndGet();
			throw new IllegalStateException("test");
		}

		@KafkaListener(id = "anehInFlight", topics = "anehInFlight", errorHandler = "nullErrorHandler",
				autoStartup = "false")
		CompletableFuture<String> inFlight(String in) {
			this.inFlight.incrementAndGet();
			return "first".equals(in) ? this.held : failed();
		}

	}

	@KafkaListener(id = "anehMixedAsync", topics = "anehMixedAsync", errorHandler = "nullErrorHandler")
	public static class MixedAsyncListener {

		@KafkaHandler
		CompletableFuture<String> text(String in) {
			return failed();
		}

		@KafkaHandler
		String number(Integer in) {
			return "number";
		}

	}

	@KafkaListener(id = "anehMixedBlocking", topics = "anehMixedBlocking", errorHandler = "nullErrorHandler")
	public static class MixedBlockingListener {

		@KafkaHandler
		CompletableFuture<String> text(String in) {
			return CompletableFuture.completedFuture(in);
		}

		@KafkaHandler
		String number(Integer in) {
			throw new IllegalStateException("test");
		}

	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Bean
		Listener listener() {
			return new Listener();
		}

		@Bean
		MixedAsyncListener mixedAsyncListener() {
			return new MixedAsyncListener();
		}

		@Bean
		MixedBlockingListener mixedBlockingListener() {
			return new MixedBlockingListener();
		}

		@Bean
		KafkaListenerErrorHandler nullErrorHandler() {
			return (message, exception) -> null;
		}

		@Bean
		ManualAckListenerErrorHandler manualNullErrorHandler(Listener listener) {
			return (message, exception, consumer, ack) -> {
				listener.manualAck.set(ack);
				return null;
			};
		}

		@Bean
		ConsumerFactory<String, Object> consumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "unused", false);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
			props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		ProducerFactory<String, Object> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = KafkaTestUtils.producerProps(broker);
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
			return new DefaultKafkaProducerFactory<>(props);
		}

		@Bean
		KafkaTemplate<String, Object> template(ProducerFactory<String, Object> producerFactory) {
			return new KafkaTemplate<>(producerFactory);
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
				ConsumerFactory<String, Object> consumerFactory, KafkaTemplate<String, Object> template,
				Listener listener) {

			ConcurrentKafkaListenerContainerFactory<String, Object> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setReplyTemplate(template);
			factory.setCommonErrorHandler(new DefaultErrorHandler((record, ex) -> listener.recovered.incrementAndGet(),
					new FixedBackOff(0L, 0L)));
			return factory;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, Object> manualFactory(
				ConsumerFactory<String, Object> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, Object> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.getContainerProperties().setAckMode(AckMode.MANUAL);
			factory.getContainerProperties().setAsyncAcks(true);
			return factory;
		}

	}

}
