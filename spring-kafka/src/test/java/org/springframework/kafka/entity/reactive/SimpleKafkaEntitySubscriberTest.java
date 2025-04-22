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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.entity.City;
import org.springframework.kafka.entity.CityGroup;
import org.springframework.kafka.entity.ComplexKey;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.LogLevels;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Popovics Boglarka
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { SimpleKafkaEntitySubscriberTest.TOPIC_CITY,
		SimpleKafkaEntitySubscriberTest.TOPIC_CITYGROUP }, partitions = 1, brokerProperties = {
				"offsets.topic.replication.factor=1", "offset.storage.replication.factor=1",
				"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class SimpleKafkaEntitySubscriberTest {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	public static final String TOPIC_CITY = "org.springframework.kafka.entity.City";

	public static final String TOPIC_CITYGROUP = "org.springframework.kafka.entity.CityGroup";

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendCity() throws Exception {

		List<City> eventList = List.of(new City("Budapest"), new City("Wien"));

		CountDownLatch consumerInitialized = new CountDownLatch(1);
		CountDownLatch consumerFinished = new CountDownLatch(1);

		AtomicInteger sum = new AtomicInteger();
		new Thread(new TestConsumerRunnable<City, String>("test_sendCity()", eventList.size(), TOPIC_CITY,
				embeddedKafka, consumerInitialized, consumerFinished, sum, City.class, String.class)).start();

		logger.warn("waiting maximum 200_000");
		consumerInitialized.await(200, TimeUnit.SECONDS);

		Subscriber<City> productSubscriber = new SimpleKafkaEntitySubscriber<City, String>(
				List.of(embeddedKafka.getBrokersAsString()), new KafkaEntitySubscriber() {

					@Override
					public Class<? extends Annotation> annotationType() {
						return KafkaEntitySubscriber.class;
					}

					@Override
					public boolean transactional() {
						return false;
					}
				}, City.class, "test_sendCity");

		Flux.fromIterable(eventList).subscribe(productSubscriber);

		logger.warn("waiting maximum 200_000");
		consumerFinished.await(200, TimeUnit.SECONDS);
		Assertions.assertEquals(eventList.size(), sum.get());

	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendCity_no_transaction() throws Exception {

		List<City> eventList = List.of(new City("Budapest"), new City("Hamburg"));

		CountDownLatch consumerInitialized = new CountDownLatch(1);
		CountDownLatch consumerFinished = new CountDownLatch(1);

		AtomicInteger sum = new AtomicInteger();
		new Thread(new TestConsumerRunnable<City, String>("test_sendCity()", eventList.size(), TOPIC_CITY,
				embeddedKafka, consumerInitialized, consumerFinished, sum, City.class, String.class)).start();

		logger.warn("waiting maximum 200_000");
		consumerInitialized.await(200, TimeUnit.SECONDS);

		Subscriber<City> productSubscriber = new SimpleKafkaEntitySubscriber<City, String>(
				List.of(embeddedKafka.getBrokersAsString()), new KafkaEntitySubscriber() {

					@Override
					public Class<? extends Annotation> annotationType() {
						return KafkaEntitySubscriber.class;
					}

					@Override
					public boolean transactional() {
						return false;
					}
				}, City.class, "test_sendCity_no_transaction");

		Flux.fromIterable(eventList).subscribe(productSubscriber);

		logger.warn("waiting maximum 200_000");
		consumerFinished.await(200, TimeUnit.SECONDS);
		Assertions.assertEquals(eventList.size(), sum.get());
	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendCityGroup_with_ComplexKey() throws Exception {

		List<CityGroup> eventList = List.of(new CityGroup(new ComplexKey(111, "group1"), "Hungary"),
				new CityGroup(new ComplexKey(6, "aus"), "Austria"), new CityGroup(new ComplexKey(40, "40"), "Germany"));

		CountDownLatch consumerInitialized = new CountDownLatch(1);
		CountDownLatch consumerFinished = new CountDownLatch(1);

		AtomicInteger sum = new AtomicInteger();
		new Thread(new TestConsumerRunnable<CityGroup, ComplexKey>("test_sendCityGroup_with_ComplexKey()",
				eventList.size(), TOPIC_CITYGROUP, embeddedKafka, consumerInitialized, consumerFinished, sum,
				CityGroup.class, ComplexKey.class)).start();

		logger.warn("waiting maximum 200_000");
		consumerInitialized.await(200, TimeUnit.SECONDS);

		Subscriber<CityGroup> productSubscriber = new SimpleKafkaEntitySubscriber<CityGroup, ComplexKey>(
				List.of(embeddedKafka.getBrokersAsString()), new KafkaEntitySubscriber() {

					@Override
					public Class<? extends Annotation> annotationType() {
						return KafkaEntitySubscriber.class;
					}

					@Override
					public boolean transactional() {
						return false;
					}
				}, CityGroup.class, "test_sendCityGroup_with_ComplexKey");

		Flux.fromIterable(eventList).subscribe(productSubscriber);

		logger.warn("waiting maximum 200_000");
		consumerFinished.await(200, TimeUnit.SECONDS);
		Assertions.assertEquals(eventList.size(), sum.get());

	}

}
