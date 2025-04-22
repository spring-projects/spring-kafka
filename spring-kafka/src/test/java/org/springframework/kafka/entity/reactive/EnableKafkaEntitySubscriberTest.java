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
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.EnableKafkaEntity;
import org.springframework.kafka.entity.City;
import org.springframework.kafka.entity.CityGroup;
import org.springframework.kafka.entity.ComplexKey;
import org.springframework.kafka.entity.reactive.EnableKafkaEntitySubscriberTest.SpringKafkaEntitySubscriberTestConfiguration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.LogLevels;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.stereotype.Service;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Popovics Boglarka
 */
@SpringJUnitConfig(SpringKafkaEntitySubscriberTestConfiguration.class)
@DirtiesContext
@EmbeddedKafka(topics = {EnableKafkaEntitySubscriberTest.TOPIC_CITY, EnableKafkaEntitySubscriberTest.TOPIC_CITYGROUP}, partitions = 1, brokerProperties = {
		"offsets.topic.replication.factor=1",
		"offset.storage.replication.factor=1", "transaction.state.log.replication.factor=1",
		"transaction.state.log.min.isr=1" })
public class EnableKafkaEntitySubscriberTest {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	@Autowired
	private ExampleKafkaEntitySubscriberService subscriber;

	@Autowired
	private ExampleKafkaEntitySubscriberNoTransactionService subscriberNoTr;

	@Autowired
	private ExampleKafkaEntitySubscriberCityGroupService subscriberCityGroup;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	public static final String TOPIC_CITY = "org.springframework.kafka.entity.City";

	public static final String TOPIC_CITYGROUP = "org.springframework.kafka.entity.CityGroup";

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendCity() throws Exception {

		List<City> eventList = subscriber.getInput();
		Subscriber<City> productSubscriber = subscriber.getCitySubscriber();

		Assertions.assertNotNull(productSubscriber);

		CountDownLatch consumerInitialized = new CountDownLatch(1);
		CountDownLatch consumerFinished = new CountDownLatch(1);

		AtomicInteger sum = new AtomicInteger();
		new Thread(new TestConsumerRunnable<City, String>("test_sendCity()", eventList.size(), TOPIC_CITY, embeddedKafka,
				consumerInitialized, consumerFinished, sum, City.class, String.class)).start();

		logger.warn("waiting maximum 200_000");
		consumerInitialized.await(200, TimeUnit.SECONDS);

		Flux.fromIterable(eventList).log().subscribe(productSubscriber);

		logger.warn("waiting maximum 200_000");
		consumerFinished.await(200, TimeUnit.SECONDS);
		Assertions.assertEquals(eventList.size(), sum.get());

	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendCity_no_transaction() throws Exception {

		List<City> eventList = subscriberNoTr.getInput();

		Subscriber<City> productSubscriber = subscriberNoTr.getCitySubscriber();

		Assertions.assertNotNull(productSubscriber);

		CountDownLatch consumerInitialized = new CountDownLatch(1);
		CountDownLatch consumerFinished = new CountDownLatch(1);

		AtomicInteger sum = new AtomicInteger();
		new Thread(new TestConsumerRunnable<City, String>("test_sendCity_no_transaction()", eventList.size(), TOPIC_CITY, embeddedKafka,
				consumerInitialized, consumerFinished, sum, City.class, String.class)).start();

		logger.warn("waiting maximum 200_000");
		consumerInitialized.await(200, TimeUnit.SECONDS);

		Flux.fromIterable(eventList).log().subscribe(productSubscriber);

		logger.warn("waiting maximum 200_000");
		consumerFinished.await(200, TimeUnit.SECONDS);
		Assertions.assertEquals(eventList.size(), sum.get());
	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendCityGroup_with_ComplexKey() throws Exception {

		List<CityGroup> eventList = subscriberCityGroup.getInput();
		Subscriber<CityGroup> productSubscriber = subscriberCityGroup.getCityGroupSubscriber();

		Assertions.assertNotNull(productSubscriber);

		CountDownLatch consumerInitialized = new CountDownLatch(1);
		CountDownLatch consumerFinished = new CountDownLatch(1);

		AtomicInteger sum = new AtomicInteger();
		new Thread(new TestConsumerRunnable<CityGroup, ComplexKey>("test_sendCityGroup_with_ComplexKey()", eventList.size(), TOPIC_CITYGROUP, embeddedKafka,
				consumerInitialized, consumerFinished, sum, CityGroup.class, ComplexKey.class)).start();

		logger.warn("waiting maximum 200_000");
		consumerInitialized.await(200, TimeUnit.SECONDS);

		Flux.fromIterable(eventList).log().subscribe(productSubscriber);

		logger.warn("waiting maximum 200_000");
		consumerFinished.await(200, TimeUnit.SECONDS);
		Assertions.assertEquals(eventList.size(), sum.get());
	}

	@Service
	public static class ExampleKafkaEntitySubscriberNoTransactionService {

		@KafkaEntitySubscriber(transactional = false)
		private Subscriber<City> citySubscriber;

		private List<City> input = List.of(new City("Debrecen"), new City("Linz"), new City("Szeged"));

		public Subscriber<City> getCitySubscriber() {
			return citySubscriber;
		}

		public List<City> getInput() {
			return input;
		}

	}

	@Service
	public static class ExampleKafkaEntitySubscriberService {

		@KafkaEntitySubscriber
		private Subscriber<City> citySubscriber;

		private List<City> input = List.of(new City("Budapest"), new City("Wien"));

		public Subscriber<City> getCitySubscriber() {
			return citySubscriber;
		}

		public List<City> getInput() {
			return input;
		}

	}

	@Service
	public static class ExampleKafkaEntitySubscriberCityGroupService {
		@KafkaEntitySubscriber
		private Subscriber<CityGroup> cityGroupSubscriber;

		private List<CityGroup> input = List.of(new CityGroup(new ComplexKey(111, "group1"), "Hungary"), new CityGroup(new ComplexKey(6, "aus"), "Austria"), new CityGroup(new ComplexKey(40, "40"), "Germany"));

		public Subscriber<CityGroup> getCityGroupSubscriber() {
			return cityGroupSubscriber;
		}

		public List<CityGroup> getInput() {
			return input;
		}

	}

	@Configuration
	@ComponentScan(basePackageClasses = { ExampleKafkaEntitySubscriberNoTransactionService.class,
			ExampleKafkaEntitySubscriberService.class, ExampleKafkaEntitySubscriberCityGroupService.class })
	@EnableKafkaEntity
	public static class SpringKafkaEntitySubscriberTestConfiguration {

	}

}
