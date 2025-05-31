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
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.EnableKafkaEntity;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.entity.ComplexKeyRecord;
import org.springframework.kafka.entity.Place;
import org.springframework.kafka.entity.PlaceVersion;
import org.springframework.kafka.entity.reactive.EnableKafkaEntityPublisherTest.KafkaProducerConfig;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonKeySerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.LogLevels;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Service;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Popovics Boglarka
 */
@SpringJUnitConfig(KafkaProducerConfig.class)
@DirtiesContext
@EmbeddedKafka(topics = { EnableKafkaEntityPublisherTest.TOPIC_PLACE,
		EnableKafkaEntityPublisherTest.TOPIC_PLACEVERSION }, partitions = 1, brokerProperties = {
				"offsets.topic.replication.factor=1", "offset.storage.replication.factor=1",
				"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class EnableKafkaEntityPublisherTest {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	@Autowired
	private ExampleKafkaEntityPublisherService placePublisherService;

	@Autowired
	private ExampleKafkaEntityPlaceVersionPublisherService placeVersionPublisherService;

	@Autowired
	private KafkaTemplate<String, Place> kafkaProducer;

	@Autowired
	private KafkaTemplate<ComplexKeyRecord, PlaceVersion> kafkaPlaceVersionProducer;

	public static final String TOPIC_PLACE = "org.springframework.kafka.entity.Place";

	public static final String TOPIC_PLACEVERSION = "org.springframework.kafka.entity.PlaceVersion";

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendEvent_PlaceVersion() throws Exception {

		List<PlaceVersion> eventList = new ArrayList<>();

		List<PlaceVersion> sending = List.of(
				new PlaceVersion(new ComplexKeyRecord(UUID.randomUUID(), LocalDateTime.now()), "absolute now"),
				new PlaceVersion(new ComplexKeyRecord(UUID.fromString("ee3968cc-3d74-44c0-8645-31e3546c963f"),
						LocalDateTime.of(2025, 04, 21, 12, 24)), "fix date and uuid"));

		CountDownLatch placeVersionReceivedCounter = new CountDownLatch(sending.size());
		Publisher<PlaceVersion> placePublisher = placeVersionPublisherService.getPlaceVersionPublisher();
		Assertions.assertNotNull(placePublisher);
		@NonNull
		BaseSubscriber<PlaceVersion> connect = new BaseSubscriber<>() {
			@Override
			protected void hookOnNext(PlaceVersion r) {
				logger.info("received: " + r);
				eventList.add(r);

				placeVersionReceivedCounter.countDown();
			}
		};
		placePublisher.subscribe(connect);

		CountDownLatch placeVersionSentCounter = new CountDownLatch(sending.size());
		publishPlaceVersionMessages(placeVersionSentCounter, 0, sending);

		logger.warn("waiting maximum 60_000");
		placeVersionSentCounter.await(60, TimeUnit.SECONDS);
		placeVersionReceivedCounter.await(60, TimeUnit.SECONDS);

		logger.warn("asserting");
		Assertions.assertEquals(sending, eventList);

	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendEvent_Place() throws Exception {

		List<Place> eventList = new ArrayList<>();
		List<Place> eventListOther = new ArrayList<>();
		List<Place> eventListBefore = new ArrayList<>();
		List<Place> eventListThird = new ArrayList<>();

		int sendingFirstCount = 2;
		int sendingSecondCount = 3;

		CountDownLatch beforeCounter = new CountDownLatch(sendingFirstCount + sendingSecondCount);
		Publisher<Place> placePublisherBefore = placePublisherService.getPlacePublisherBefore();
		Assertions.assertNotNull(placePublisherBefore);
		@NonNull
		BaseSubscriber<Place> connectBefore = new BaseSubscriber<>() {
			@Override
			protected void hookOnNext(Place r) {
				logger.info("received-Before: " + r);
				eventListBefore.add(r);

				beforeCounter.countDown();
			}
		};
		placePublisherBefore.subscribe(connectBefore);

		CountDownLatch receivedCounter = new CountDownLatch(sendingFirstCount);
		Flux<Place> placePublisher = placePublisherService.getPlacePublisher();

		Disposable connect1 = placePublisher.subscribe(r -> {
			logger.info("received: " + r);
			eventList.add(r);
			receivedCounter.countDown();
		});

		logger.warn("waiting 30_0000");
		Thread.sleep(30_000);

		CountDownLatch sentCounter = new CountDownLatch(sendingFirstCount);
		publishMessages(sentCounter, 100, sendingFirstCount);
		logger.warn("sentCounter.await() maximum 40_000");
		sentCounter.await(40, TimeUnit.SECONDS);

		logger.warn("waiting maximum 40_000");
		receivedCounter.await(40, TimeUnit.SECONDS);
		connect1.dispose();

		Publisher<Place> placePublisherOther = placePublisherService.getPlacePublisherOther();
		@NonNull
		BaseSubscriber<Place> connect2 = new BaseSubscriber<>() {
			@Override
			protected void hookOnNext(Place r) {
				logger.info("received-other: " + r);
				eventListOther.add(r);
			}
		};
		placePublisherOther.subscribe(connect2);

		logger.warn("waiting 30_000");
		Thread.sleep(30_000);
		connect2.dispose();

		CountDownLatch thirdCounter = new CountDownLatch(sendingSecondCount);
		Publisher<Place> placePublisherThird = placePublisherService.getPlacePublisherThird();
		@NonNull
		BaseSubscriber<Place> connectThird = new BaseSubscriber<>() {
			@Override
			protected void hookOnNext(Place r) {
				logger.info("received-Third: " + r);
				eventListThird.add(r);
				thirdCounter.countDown();
			}
		};
		placePublisherThird.subscribe(connectThird);

		CountDownLatch sentCounterSecond = new CountDownLatch(sendingSecondCount);
		// send events
		publishMessages(sentCounterSecond, 200, sendingSecondCount);
		logger.warn("sentCounterSecond.await() max 30_000");
		sentCounterSecond.await(30, TimeUnit.SECONDS);

		logger.warn("waiting maximum 90_000");
		thirdCounter.await(90, TimeUnit.SECONDS);

		logger.warn("waiting maximum 120_000");
		beforeCounter.await(120, TimeUnit.SECONDS);

		logger.info("eventList      : " + eventList);
		logger.info("eventListOther : " + eventListOther);
		logger.info("eventListBefore: " + eventListBefore);
		logger.info("eventListThird : " + eventListThird);
		Assertions.assertEquals(sendingFirstCount, eventList.size(), "eventList");
		Assertions.assertEquals(0, eventListOther.size(), "eventListOther");
		Assertions.assertEquals(sendingFirstCount + sendingSecondCount, eventListBefore.size(), "eventListBefore");
		Assertions.assertEquals(sendingSecondCount, eventListThird.size(), "eventListThird");

		connectThird.dispose();
		connectBefore.dispose();
	}

	void publishMessages(CountDownLatch sentCounter, int i, int count) throws Exception {

		logger.warn("publishMessages " + i + " " + count);

		for (int placeInt = 0; placeInt < count; placeInt++) {

			Place p2 = new Place("p" + (i++) + "id");
			ProducerRecord<String, Place> p2Record = new ProducerRecord<>(TOPIC_PLACE, p2.id(), p2);
			sendPlace(sentCounter, p2Record);
		}
	}

	void publishPlaceVersionMessages(CountDownLatch sentCounter, int i, List<PlaceVersion> placeVersionList)
			throws Exception {

		logger.warn("publishPlaceVersionMessages " + i + " " + placeVersionList.size());

		for (PlaceVersion pV : placeVersionList) {

			ProducerRecord<ComplexKeyRecord, PlaceVersion> p2Record = new ProducerRecord<>(TOPIC_PLACEVERSION,
					pV.getComplexKeyRecord(), pV);
			sendPlaceVersion(sentCounter, p2Record);
		}
	}

	private void sendPlace(CountDownLatch sentCounter, ProducerRecord<String, Place> record)
			throws InterruptedException, ExecutionException {
		CompletableFuture<SendResult<String, Place>> sendingIntoTheFuture = kafkaProducer.send(record);

		sendingIntoTheFuture.get();
		while (!sendingIntoTheFuture.isDone()) {
			logger.info("waiting");
		}
		sentCounter.countDown();
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

	@Service
	public static class ExampleKafkaEntityPublisherService {

		@KafkaEntityPublisher
		private Flux<Place> placePublisher;

		@KafkaEntityPublisher
		private Publisher<Place> placePublisherOther;

		@KafkaEntityPublisher
		private Publisher<Place> placePublisherThird;

		@KafkaEntityPublisher
		private Publisher<Place> placePublisherBefore;

		public Flux<Place> getPlacePublisher() {
			return placePublisher.log();
		}

		public Publisher<Place> getPlacePublisherOther() {
			return placePublisherOther;
		}

		public Publisher<Place> getPlacePublisherThird() {
			return placePublisherThird;
		}

		public Publisher<Place> getPlacePublisherBefore() {
			return placePublisherBefore;
		}

	}

	@Service
	public static class ExampleKafkaEntityPlaceVersionPublisherService {

		@KafkaEntityPublisher
		private Publisher<PlaceVersion> placeVersionPublisher;

		public Publisher<PlaceVersion> getPlaceVersionPublisher() {
			return placeVersionPublisher;
		}

	}

	@Configuration
	@ComponentScan(basePackageClasses = { ExampleKafkaEntityPublisherService.class,
			ExampleKafkaEntityPlaceVersionPublisherService.class })
	@EnableKafkaEntity
	public static class KafkaProducerConfig {

		@Autowired
		private EmbeddedKafkaBroker embeddedKafka;

		@Bean
		public ProducerFactory<String, Place> producerFactory() {

			Map<String, Object> config = KafkaTestUtils.producerProps(embeddedKafka);

			return new DefaultKafkaProducerFactory<>(config, new JsonKeySerializer<String>(),
					new JsonSerializer<Place>());
		}

		@Bean
		public KafkaTemplate<String, Place> kafkaProducer() {
			return new KafkaTemplate<>(producerFactory());
		}

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
