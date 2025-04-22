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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Processor;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.EnableKafkaEntity;
import org.springframework.kafka.entity.KafkaEntityException;
import org.springframework.kafka.entity.Product;
import org.springframework.kafka.entity.Student;
import org.springframework.kafka.entity.User;
import org.springframework.kafka.entity.reactive.EnableKafkaEntityProcessorTest.SpringKafkaEntityProcessorTestConfiguration;
import org.springframework.kafka.test.condition.LogLevels;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.stereotype.Service;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Popovics Boglarka
 */
@SpringJUnitConfig(SpringKafkaEntityProcessorTestConfiguration.class)
@DirtiesContext
@EmbeddedKafka(topics = {

		EnableKafkaEntityProcessorTest.TOPIC_CITY, EnableKafkaEntityProcessorTest.TOPIC_PLACE,
		EnableKafkaEntityProcessorTest.TOPIC_PRODUCT, EnableKafkaEntityProcessorTest.TOPIC_USER,
		EnableKafkaEntityProcessorTest.TOPIC_STUDENT }, partitions = 1, brokerProperties = {
				"offsets.topic.replication.factor=1", "offset.storage.replication.factor=1",
				"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class EnableKafkaEntityProcessorTest {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	@Autowired
	private OtherKafkaEntityProcessorService processorService;

	@Autowired
	private KafkaEntityDefaultConfiguration kafkaEntityDefaultConfiguration;

	public static final String TOPIC_PRODUCT = "PRODUCT";

	public static final String TOPIC_USER = "org.springframework.kafka.entity.User";

	public static final String TOPIC_STUDENT = "org.springframework.kafka.entity.Student";

	public static final String TOPIC_CITY = "org.springframework.kafka.entity.City";

	public static final String TOPIC_PLACE = "org.springframework.kafka.entity.Place";

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendEvent() throws KafkaEntityException, InterruptedException, ExecutionException {

		Product event = new Product();
		event.setId("123456");
		RecordMetadata sendEventMetadata = processorService.sendEvent(event);
		Assertions.assertNotNull(sendEventMetadata);
		logger.info("sendEventMetadata: " + sendEventMetadata.offset());

	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendUser() throws KafkaEntityException, InterruptedException, ExecutionException {

		User event = new User("abcdef");
		Assertions.assertThrows(NullPointerException.class, () -> processorService.sendUser(event));

		KafkaEntityException ex = Assertions.assertThrows(KafkaEntityException.class,
				() -> kafkaEntityDefaultConfiguration.throwFirstError());
		Assertions.assertEquals(
				"org.springframework.kafka.entity.reactive.EnableKafkaEntityProcessorTest$OtherKafkaEntityProcessorService#userProcessor: org.springframework.kafka.entity.User @KafkaEntityKey is mandatory in @KafkaEntity",
				ex.getMessage());
	}

	@LogLevels(categories = { "org.springframework.kafka.entity", "reactor.core.publisher" }, level = "TRACE")
	@Test
	public void test_sendStudent() throws KafkaEntityException, InterruptedException, ExecutionException {

		Student event = new Student("hrs123", 23);
		RecordMetadata sendStudentMetadata = processorService.sendStudent(event);
		Assertions.assertNotNull(sendStudentMetadata);
		logger.info("sendStudentMetadata: " + sendStudentMetadata.offset());

	}

	@Service
	public static class OtherKafkaEntityProcessorService {

		private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

		@KafkaEntityProcessor
		private Processor<Product, RecordMetadata> productProcessor;

		@KafkaEntityProcessor
		private Processor<User, RecordMetadata> userProcessor;

		@KafkaEntityProcessor(transactional = false)
		private KafkaProcessor<Student> studentProcessor;

		public RecordMetadata sendEvent(Product p) throws InterruptedException {
			CountDownLatch countDownLatch = new CountDownLatch(1);
			List<RecordMetadata> ret = new ArrayList<>();
			productProcessor.subscribe(new BaseSubscriber<>() {
				@Override
				protected void hookOnNext(RecordMetadata r) {
					logger.trace("received: " + r);
					ret.add(r);
					countDownLatch.countDown();
				}
			});
			Mono.just(p).subscribe(productProcessor);
			logger.warn("waiting 30_000");
			countDownLatch.await(30, TimeUnit.SECONDS);
			return ret.get(0);
		}

		public RecordMetadata sendUser(User u) throws InterruptedException {
			CountDownLatch countDownLatch = new CountDownLatch(1);
			List<RecordMetadata> ret = new ArrayList<>();
			userProcessor.subscribe(new BaseSubscriber<>() {
				@Override
				protected void hookOnNext(RecordMetadata r) {
					logger.trace("received: " + r);
					ret.add(r);
					countDownLatch.countDown();
				}
			});
			Mono.just(u).subscribe(userProcessor);
			logger.warn("waiting 30_000");
			countDownLatch.await(30, TimeUnit.SECONDS);
			return ret.get(0);
		}

		public RecordMetadata sendStudent(Student s) throws InterruptedException {
			CountDownLatch countDownLatch = new CountDownLatch(1);
			List<RecordMetadata> ret = new ArrayList<>();
			studentProcessor.subscribe(new BaseSubscriber<>() {
				@Override
				protected void hookOnNext(RecordMetadata r) {
					logger.trace("received: " + r);
					ret.add(r);
					countDownLatch.countDown();
				}
			});
			Flux.fromIterable(Arrays.asList(s)).subscribe(studentProcessor);
			logger.warn("waiting 30_000");
			countDownLatch.await(30, TimeUnit.SECONDS);
			return ret.get(0);
		}

	}

	@Configuration
	@ComponentScan(basePackageClasses = { SpringKafkaEntityProcessorTestConfiguration.class,
			OtherKafkaEntityProcessorService.class })
	@EnableKafkaEntity
	public static class SpringKafkaEntityProcessorTestConfiguration {

	}
}
