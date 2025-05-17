/*
 * Copyright 2016-2025 the original author or authors.
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

package org.springframework.kafka.entity.reactive;

import java.lang.annotation.Annotation;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.BaseSubscriber;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.entity.ComplexKeyRecord;
import org.springframework.kafka.entity.PlaceVersion;
import org.springframework.kafka.entity.reactive.SimpleKafkaEntityPublisherTest.KafkaProducerConfig;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonKeySerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.LogLevels;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Popovics Boglarka
 */
@SpringJUnitConfig(KafkaProducerConfig.class)
@DirtiesContext
@EmbeddedKafka(topics = { SimpleKafkaEntityPublisherTest.TOPIC_PLACEVERSION }, partitions = 1, brokerProperties = {
		"offsets.topic.replication.factor=1", "offset.storage.replication.factor=1",
		"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class SimpleKafkaEntityPublisherTest {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private KafkaTemplate<ComplexKeyRecord, PlaceVersion> kafkaPlaceVersionProducer;

	public static final String TOPIC_PLACEVERSION = "org.springframework.kafka.entity.PlaceVersion";

	void publishPlaceVersionMessages(CountDownLatch sentCounter, int i, List<PlaceVersion> placeVersionList)
			throws Exception {

		logger.warn("publishPlaceVersionMessages " + i + " " + placeVersionList.size());

		for (PlaceVersion pV : placeVersionList) {

			ProducerRecord<ComplexKeyRecord, PlaceVersion> p2Record = new ProducerRecord<>(TOPIC_PLACEVERSION,
					pV.getComplexKeyRecord(), pV);
			sendPlaceVersion(sentCounter, p2Record);
		}
	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendEvent_PlaceVersion() throws Exception {

		List<PlaceVersion> eventList = new ArrayList<>();

		List<PlaceVersion> sending = List.of(
				new PlaceVersion(new ComplexKeyRecord(UUID.randomUUID(), LocalDateTime.now()), "absolute now"),
				new PlaceVersion(new ComplexKeyRecord(UUID.fromString("ee3968cc-3d74-44c0-8645-31e3546c963f"),
						LocalDateTime.of(2025, 04, 21, 12, 24)), "fix date and uuid"));

		CountDownLatch placeVersionReceivedCounter = new CountDownLatch(sending.size());
		Publisher<PlaceVersion> placeVersionPublisher = new SimpleKafkaEntityPublisher<PlaceVersion, ComplexKeyRecord>(
				List.of(embeddedKafka.getBrokersAsString()), new KafkaEntityPublisher() {

					@Override
					public Class<? extends Annotation> annotationType() {
						return KafkaEntityPublisher.class;
					}

				}, PlaceVersion.class, "test_sendEvent_PlaceVersion");
		Assertions.assertNotNull(placeVersionPublisher);
		@NonNull
		BaseSubscriber<PlaceVersion> connect = new BaseSubscriber<>() {
			@Override
			protected void hookOnNext(PlaceVersion r) {
				logger.info("received: " + r);
				eventList.add(r);

				placeVersionReceivedCounter.countDown();
			}
		};
		placeVersionPublisher.subscribe(connect);

		CountDownLatch placeVersionSentCounter = new CountDownLatch(sending.size());

		publishPlaceVersionMessages(placeVersionSentCounter, 0, sending);

		logger.warn("waiting maximum 90_000");
		placeVersionSentCounter.await(90, TimeUnit.SECONDS);
		placeVersionReceivedCounter.await(90, TimeUnit.SECONDS);

		logger.warn("asserting");
		Assertions.assertEquals(sending, eventList);

	}

	private void sendPlaceVersion(CountDownLatch sentCounter, ProducerRecord<ComplexKeyRecord, PlaceVersion> record)
			throws InterruptedException, ExecutionException {
		CompletableFuture<SendResult<ComplexKeyRecord, PlaceVersion>> sendingIntoTheFuture = kafkaPlaceVersionProducer
				.send(record);

		sendingIntoTheFuture.get();
		while (!sendingIntoTheFuture.isDone()) {
			logger.info("waiting");
		}
		sentCounter.countDown();
	}

	@Configuration
	public static class KafkaProducerConfig {

		@Autowired
		private EmbeddedKafkaBroker embeddedKafka;

		@Bean
		public ProducerFactory<ComplexKeyRecord, PlaceVersion> placeVersionProducerFactory() {
			Map<String, Object> config = KafkaTestUtils.producerProps(embeddedKafka);

			return new DefaultKafkaProducerFactory<>(config, new JsonKeySerializer<ComplexKeyRecord>(),
					new JsonSerializer<PlaceVersion>());
		}

		@Bean
		public KafkaTemplate<ComplexKeyRecord, PlaceVersion> kafkaPlaceVerionProducer() {
			return new KafkaTemplate<>(placeVersionProducerFactory());
		}
	}

}
