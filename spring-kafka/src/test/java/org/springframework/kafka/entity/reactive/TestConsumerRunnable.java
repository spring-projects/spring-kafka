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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonKeyDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @param <T> class of the entity, representing messages
 * @param <K> the key from the entity
 *
 * @author Popovics Boglarka
 */
public class TestConsumerRunnable<T, K> implements Runnable {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private String testName;

	private int expectedSum;

	private String topic;

	private EmbeddedKafkaBroker embeddedKafka;

	private CountDownLatch consumerInitialized;

	private CountDownLatch consumerFinished;

	private AtomicInteger sum;

	private Class<T> valueClazz;

	private Class<K> keyClazz;

	public TestConsumerRunnable(String testName, int expectedSum, String topic, EmbeddedKafkaBroker embeddedKafka,
			CountDownLatch consumerInitialized, CountDownLatch consumerFinished, AtomicInteger sum, Class<T> valueClazz,
			Class<K> keyClazz) {
		super();
		this.testName = testName;
		this.expectedSum = expectedSum;
		this.topic = topic;
		this.embeddedKafka = embeddedKafka;
		this.consumerInitialized = consumerInitialized;
		this.consumerFinished = consumerFinished;
		this.sum = sum;
		this.valueClazz = valueClazz;
		this.keyClazz = keyClazz;
	}

	@Override
	public void run() {

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.getClass().getName(), "false",
				embeddedKafka);
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>(valueClazz);
		JsonKeyDeserializer<K> keyDeserializer = new JsonKeyDeserializer<>(keyClazz);

		valueDeserializer.addTrustedPackages(valueClazz.getPackageName());
		keyDeserializer.addTrustedPackages(keyClazz.getPackageName());

		try (KafkaConsumer<K, T> kafkaConsumer = new KafkaConsumer<K, T>(consumerProps, keyDeserializer,
				valueDeserializer);) {
			kafkaConsumer.subscribe(List.of(topic));

			kafkaConsumer.poll(Duration.ofSeconds(10L));

			kafkaConsumer.seekToEnd(Collections.emptyList());
			kafkaConsumer.commitSync();
			// wait until kafkaConsumer is ready and offset setted
			LocalDateTime then = LocalDateTime.now();
			while (kafkaConsumer.committed(kafkaConsumer.assignment()).isEmpty()) {
				if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 100) {
					throw new RuntimeException("KafkaConsumer is not ready in 100sec.");
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("started " + testName + "...");
			}

			do {
				if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 180) {
					throw new RuntimeException("KafkaConsumer polling is timed out.");
				}
				ConsumerRecords<K, T> poll = kafkaConsumer.poll(Duration.ofSeconds(10));
				if (logger.isTraceEnabled()) {
					poll.forEach(cr -> logger.trace("polled: " + cr.value()));
				}
				sum.addAndGet(poll.count());
				if (logger.isTraceEnabled()) {
					logger.trace("polling... sum=" + sum);
				}
				consumerInitialized.countDown();
			} while (sum.get() < expectedSum);

			kafkaConsumer.unsubscribe();
		}
		finally {
			consumerFinished.countDown();
		}
	}
}
